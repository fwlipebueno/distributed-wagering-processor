import type { Message } from "@aws-sdk/client-sqs";
import type { MikroORM } from "@mikro-orm/postgresql";
import { parseMessage, payloadHash } from "../application/contracts.js";
import type { WagerService } from "../application/wager-service.js";
import { RequestError } from "../domain/errors.js";
import { OutboxMessage } from "../domain/messages.js";
import type { Config } from "./config.js";
import { databaseErrorCode, sql, transientError } from "./database.js";
import { OutboxRow } from "./entities.js";
import { log, type Metrics } from "./observability.js";
import type { SqsGateway } from "./sqs.js";

export interface WorkerHooks {
  afterCommit?: (messageId: string, transactionId: string) => Promise<void>;
  afterPublish?: (eventId: string) => Promise<void>;
}

export class Workers {
  private readonly abort = new AbortController();
  private readonly active = new Map<string, () => Promise<void>>();
  private running?: Promise<void>;
  private nextMetricsAt = 0;

  constructor(
    private readonly orm: MikroORM,
    private readonly wagers: WagerService,
    private readonly sqs: SqsGateway,
    private readonly metrics: Metrics,
    private readonly config: Config,
    private readonly hooks: WorkerHooks = {},
  ) {}

  start(): void {
    this.running = Promise.all([
      this.loop("consumer", async () => {
        const response = await this.sqs.receive();
        if (this.abort.signal.aborted) {
          await Promise.allSettled(
            (response.Messages ?? [])
              .filter((message) => message.ReceiptHandle)
              .map((message) =>
                this.sqs.visibility(message.ReceiptHandle as string, 0),
              ),
          );
          return;
        }
        await Promise.all(
          (response.Messages ?? []).map((message) => this.handle(message)),
        );
      }),
      this.loop("outbox", async () => {
        const published = await this.publishOne();
        if (!published) await this.pause(200);
        if (Date.now() >= this.nextMetricsAt) {
          this.nextMetricsAt = Date.now() + 1000;
          await this.updateLag();
        }
      }),
      this.loop("reference", async () => {
        await this.wagers.retryReferences();
        await this.pause(200);
      }),
    ]).then(() => undefined);
  }

  private async pause(ms: number): Promise<void> {
    if (this.abort.signal.aborted) return;
    await new Promise<void>((resolve) => {
      const finish = () => {
        clearTimeout(timer);
        this.abort.signal.removeEventListener("abort", finish);
        resolve();
      };
      const timer = setTimeout(finish, ms);
      this.abort.signal.addEventListener("abort", finish, { once: true });
    });
  }

  private async loop(name: string, work: () => Promise<void>): Promise<void> {
    while (!this.abort.signal.aborted) {
      try {
        await work();
      } catch (error) {
        if (this.abort.signal.aborted) break;
        log(
          "error",
          `${name}_unavailable`,
          {},
          databaseErrorCode(error) ?? "INFRASTRUCTURE_ERROR",
        );
        this.metrics.retries.inc({ source: name });
        await this.pause(1000);
      }
    }
  }

  async handle(message: Message): Promise<void> {
    const receipt = message.ReceiptHandle;
    if (!receipt) return;
    let stopped = false;
    let renewal = Promise.resolve();
    const heartbeat = setInterval(
      () => {
        if (stopped) return;
        renewal = renewal.then(async () => {
          if (stopped) return;
          await this.sqs
            .visibility(receipt, this.config.SQS_VISIBILITY_SECONDS)
            .catch(() => {
              log(
                "warn",
                "visibility_extension_failed",
                { messageId: message.MessageId },
                "SQS_UNAVAILABLE",
              );
            });
        });
      },
      Math.max(500, (this.config.SQS_VISIBILITY_SECONDS * 1000) / 3),
    );
    const stopHeartbeat = async () => {
      stopped = true;
      clearInterval(heartbeat);
      await renewal;
    };
    this.active.set(receipt, stopHeartbeat);
    let context = {
      correlationId: message.MessageId ?? "unknown",
      messageId: message.MessageId,
      walletId: undefined as string | undefined,
      providerId: undefined as string | undefined,
    };
    try {
      const parsed = parseMessage(message.Body ?? "");
      context = {
        correlationId: parsed.messageId,
        messageId: parsed.messageId,
        walletId: parsed.request.walletId,
        providerId: parsed.request.providerId,
      };
      const result = await this.wagers.execute(
        parsed.request,
        parsed.key,
        { correlationId: parsed.messageId, causationId: parsed.messageId },
        { messageId: parsed.messageId, payloadHash: parsed.inboxHash },
      );
      await this.hooks.afterCommit?.(parsed.messageId, result.transactionId);
      await this.sqs.ack(receipt);
      log("info", "message_acknowledged", {
        ...context,
        transactionId: result.transactionId,
      });
    } catch (error) {
      const attempts = Number(
        message.Attributes?.ApproximateReceiveCount ?? "1",
      );
      const permanent = error instanceof RequestError || !transientError(error);
      if (permanent || attempts >= this.config.SQS_MAX_ATTEMPTS) {
        try {
          await this.sqs.deadLetter(
            message.Body ?? "",
            payloadHash({ sqsId: message.MessageId, body: message.Body }),
          );
          await this.sqs.ack(receipt);
          this.metrics.dlq.inc();
          log(
            "warn",
            "message_dead_lettered",
            context,
            error instanceof RequestError
              ? error.code
              : permanent
                ? "PERMANENT_INFRASTRUCTURE_ERROR"
                : "RETRY_EXHAUSTED",
          );
        } catch {
          log(
            "error",
            "dead_letter_forward_failed",
            context,
            "SQS_UNAVAILABLE",
          );
        }
      } else {
        this.metrics.retries.inc({ source: "sqs" });
        const seconds = Math.min(60, 2 ** Math.min(attempts, 6));
        await this.sqs.visibility(receipt, seconds).catch(() => undefined);
        log(
          "warn",
          "message_retry_scheduled",
          context,
          databaseErrorCode(error) ?? "TRANSIENT_INFRASTRUCTURE_ERROR",
        );
      }
    } finally {
      await stopHeartbeat();
      this.active.delete(receipt);
    }
  }

  async publishOne(): Promise<boolean> {
    return this.orm.em.fork().transactional(async (em) => {
      const due = await sql<{ id: string }>(
        em,
        `SELECT id FROM outbox WHERE published_at IS NULL AND (next_attempt_at IS NULL OR next_attempt_at<=now()) ORDER BY occurred_at,id LIMIT 1 FOR UPDATE SKIP LOCKED`,
      );
      if (!due[0]) return false;
      const row = await em.findOneOrFail(OutboxRow, due[0].id);
      const message = OutboxMessage.rehydrate(row);
      try {
        await this.sqs.publish(
          message.id,
          message.aggregateId,
          message.payload,
        );
        await this.hooks.afterPublish?.(message.id);
        message.markPublished(new Date());
        row.publishedAt = message.publishedAt;
        row.nextAttemptAt = undefined;
      } catch {
        message.scheduleRetry(new Date());
        row.attempts = message.attempts;
        row.nextAttemptAt = message.nextAttemptAt;
        this.metrics.retries.inc({ source: "outbox" });
        log(
          "warn",
          "outbox_retry_scheduled",
          {
            walletId: row.aggregateId,
            correlationId:
              typeof row.payload.correlationId === "string"
                ? row.payload.correlationId
                : undefined,
          },
          "SQS_UNAVAILABLE",
        );
      }
      return true;
    });
  }

  async updateLag(): Promise<void> {
    const result = await sql<{ lag: string }>(
      this.orm.em.fork(),
      `SELECT COALESCE(EXTRACT(EPOCH FROM now()-MIN(occurred_at)),0)::text AS lag FROM outbox WHERE published_at IS NULL`,
    );
    this.metrics.outboxLag.set(Math.max(0, Number(result[0]?.lag ?? "0")));
    this.metrics.dlqDepth.set(await this.sqs.deadLetterDepth());
  }

  async stop(): Promise<void> {
    this.abort.abort();
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        this.running,
        new Promise<void>((resolve) => {
          timer = setTimeout(resolve, 25_000);
        }),
      ]);
    } finally {
      clearTimeout(timer);
      await Promise.allSettled([...this.active.values()].map((stop) => stop()));
      await Promise.allSettled(
        [...this.active.keys()].map((receipt) =>
          this.sqs.visibility(receipt, 0),
        ),
      );
    }
  }
}
