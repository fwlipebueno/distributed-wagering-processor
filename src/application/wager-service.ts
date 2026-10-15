import { randomUUID } from "node:crypto";
import { LockMode } from "@mikro-orm/core";
import type { EntityManager, MikroORM } from "@mikro-orm/postgresql";
import {
  DomainError,
  type FailureCode,
  RequestError,
} from "../domain/errors.js";
import {
  type EventContext,
  type IntegrationEvent,
  WagerTransactionFailed,
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WalletBalanceChanged,
} from "../domain/events.js";
import { InboxMessage, OutboxMessage } from "../domain/messages.js";
import { Money } from "../domain/money.js";
import {
  validateWager,
  WagerKind,
  WagerStatus,
  WagerTransaction,
} from "../domain/transaction.js";
import {
  LedgerDirection,
  Wallet,
  WalletLedgerEntry,
} from "../domain/wallet.js";
import type { Config } from "../infrastructure/config.js";
import {
  databaseErrorCode,
  sql,
  transientError,
} from "../infrastructure/database.js";
import {
  InboxRow,
  LedgerRow,
  OutboxRow,
  type TransactionResult,
  WagerRow,
  WalletRow,
} from "../infrastructure/entities.js";
import { log, type Metrics } from "../infrastructure/observability.js";
import {
  assertReplay,
  parseWager,
  payloadHash,
  walletSchema,
} from "./contracts.js";

const consumerName = "wager-processor-v1";
export interface InboxContext {
  messageId: string;
  payloadHash: string;
}

function walletDomain(row: WalletRow): Wallet {
  return Wallet.rehydrate({
    ...row,
    balance: Money.rehydrate({ amount: row.balance, currency: row.currency }),
  });
}

function transactionDomain(row: WagerRow): WagerTransaction {
  return WagerTransaction.rehydrate({
    ...row,
    money: Money.rehydrate({ amount: row.amount, currency: row.currency }),
  });
}

function ledgerRow(entry: WalletLedgerEntry): LedgerRow {
  return Object.assign(new LedgerRow(), {
    id: entry.id,
    walletId: entry.walletId,
    transactionId: entry.transactionId,
    direction: entry.direction,
    amount: entry.money.toString(),
    currency: entry.money.currency,
    balanceBefore: entry.balanceBefore.toString(),
    balanceAfter: entry.balanceAfter.toString(),
    createdAt: entry.createdAt,
  });
}

function outboxRow(event: IntegrationEvent<unknown>): OutboxRow {
  const message = OutboxMessage.enqueue(event);
  return Object.assign(new OutboxRow(), {
    id: message.id,
    aggregateId: message.aggregateId,
    eventType: message.eventType,
    payload: message.payload,
    occurredAt: message.occurredAt,
    attempts: message.attempts,
  });
}

export class WagerService {
  constructor(
    private readonly orm: MikroORM,
    private readonly metrics: Metrics,
    private readonly config: Config,
  ) {}

  async openWallet(input: unknown, ctx: EventContext) {
    const parsed = walletSchema.safeParse(input);
    if (!parsed.success) throw new RequestError("INVALID_PAYLOAD", 400);
    let initial: Money;
    try {
      initial = Money.from(parsed.data.initialBalance);
    } catch (error) {
      if (error instanceof DomainError) throw new RequestError(error.code, 400);
      throw error;
    }
    const wallet = Wallet.open({
      id: randomUUID(),
      playerId: parsed.data.playerId,
      initialBalance: initial,
    });
    try {
      await this.orm.em.fork().transactional(async (em) => {
        const row = Object.assign(new WalletRow(), {
          id: wallet.id,
          playerId: wallet.playerId,
          balance: wallet.balance.toString(),
          currency: wallet.currency,
          version: wallet.version,
          createdAt: wallet.createdAt,
          updatedAt: wallet.updatedAt,
        });
        em.persist(row);
        await em.flush();
        if (initial.isPositive()) {
          const t = WagerTransaction.create({
            id: randomUUID(),
            providerId: "internal",
            externalTransactionId: wallet.id,
            idempotencyKey: `internal:${wallet.id}`,
            payloadHash: payloadHash(parsed.data),
            walletId: wallet.id,
            playerId: wallet.playerId,
            roundId: "opening",
            gameId: "opening",
            kind: WagerKind.Opening,
            money: initial,
            createdAt: new Date(),
          });
          t.markProcessed(undefined, new Date());
          const wager = this.newRow(t, ctx);
          wager.result = this.result(t, wallet);
          em.persist(wager);
          await em.flush();
          const entry = WalletLedgerEntry.create({
            id: randomUUID(),
            walletId: wallet.id,
            transactionId: t.id,
            direction: LedgerDirection.Credit,
            money: initial,
            balanceBefore: Money.zero(wallet.currency),
            balanceAfter: initial,
            createdAt: new Date(),
          });
          em.persist([
            ledgerRow(entry),
            outboxRow(WagerTransactionProcessed.from(t, ctx)),
            outboxRow(WalletBalanceChanged.from(wallet, entry, ctx)),
          ]);
        }
      });
    } catch (error) {
      if (databaseErrorCode(error) === "23505")
        throw new RequestError("WALLET_ALREADY_EXISTS", 409);
      throw error;
    }
    if (initial.isPositive())
      this.metrics.transactions.inc({ status: WagerStatus.Processed });
    log("info", "wallet_opened", { ...ctx, walletId: wallet.id });
    return this.walletResponse(wallet);
  }

  async execute(
    input: unknown,
    key: unknown,
    ctx: EventContext,
    inbox?: InboxContext,
  ): Promise<TransactionResult> {
    const { request, key: validKey, hash } = parseWager(input, key);
    const stopTimer = this.metrics.latency.startTimer();
    try {
      const result = await this.financialTransaction(async (em) => {
        let received: InboxRow | undefined;
        if (inbox) {
          const domain = InboxMessage.receive({ ...inbox, consumerName });
          await sql(
            em,
            `INSERT INTO inbox(consumer_name,message_id,payload_hash,received_at) VALUES(?,?,?,?) ON CONFLICT DO NOTHING`,
            [
              consumerName,
              inbox.messageId,
              domain.payloadHash,
              domain.receivedAt,
            ],
          );
          received = await em.findOneOrFail(
            InboxRow,
            { consumerName, messageId: inbox.messageId },
            { lockMode: LockMode.PESSIMISTIC_WRITE },
          );
          if (received.payloadHash !== inbox.payloadHash)
            throw new RequestError("INBOX_PAYLOAD_CONFLICT", 409);
          if (received.processedAt && received.transactionId) {
            const original = await em.findOneOrFail(
              WagerRow,
              received.transactionId,
            );
            return this.replay(original, validKey, hash);
          }
        }
        let existing = await em.findOne(WagerRow, {
          $or: [
            { idempotencyKey: validKey },
            {
              providerId: request.providerId,
              externalTransactionId: request.externalTransactionId,
            },
          ],
        });
        if (existing) {
          const replay = this.replay(existing, validKey, hash);
          if (received) await this.completeInbox(em, received, existing.id);
          return replay;
        }
        const row = await em.findOne(WalletRow, request.walletId, {
          lockMode: LockMode.PESSIMISTIC_WRITE,
        });
        if (!row) throw new RequestError("WALLET_NOT_FOUND", 404);
        existing = await em.findOne(
          WagerRow,
          {
            $or: [
              { idempotencyKey: validKey },
              {
                providerId: request.providerId,
                externalTransactionId: request.externalTransactionId,
              },
            ],
          },
          { refresh: true },
        );
        if (existing) {
          const replay = this.replay(existing, validKey, hash);
          if (received) await this.completeInbox(em, received, existing.id);
          return replay;
        }
        const t = WagerTransaction.create({
          ...request,
          id: randomUUID(),
          idempotencyKey: validKey,
          payloadHash: hash,
          money: Money.from(request.money),
          createdAt: new Date(),
        });
        const wager = this.newRow(t, ctx);
        em.persist(wager);
        await em.flush();
        const result = await this.apply(em, wager, row, ctx, false);
        if (received) await this.completeInbox(em, received, wager.id);
        return result;
      });
      if (result.idempotentReplay)
        this.metrics.duplicates.inc({ source: inbox ? "sqs" : "http" });
      else this.metrics.transactions.inc({ status: result.status });
      log(
        "info",
        result.idempotentReplay
          ? "transaction_replayed"
          : "transaction_committed",
        {
          ...ctx,
          messageId: inbox?.messageId,
          transactionId: result.transactionId,
          walletId: request.walletId,
          providerId: request.providerId,
        },
        result.failureCode,
      );
      return result;
    } finally {
      stopTimer();
    }
  }

  private async financialTransaction<T>(
    work: (em: EntityManager) => Promise<T>,
  ): Promise<T> {
    for (let attempt = 0; ; attempt += 1) {
      try {
        return await this.orm.em.fork().transactional(async (em) => {
          await sql(em, "SELECT set_config('lock_timeout',?,true)", [
            `${this.config.SQL_LOCK_TIMEOUT_MS}ms`,
          ]);
          return work(em);
        });
      } catch (error) {
        if (error instanceof RequestError || error instanceof DomainError)
          throw error;
        const code = databaseErrorCode(error);
        if (["55P03", "40P01", "40001"].includes(code ?? ""))
          this.metrics.lockConflicts.inc();
        if (attempt >= 2 || (code !== "23505" && !transientError(error)))
          throw error;
        this.metrics.retries.inc({ source: "sql" });
        await Bun.sleep(20 * 2 ** attempt + Math.floor(Math.random() * 20));
      }
    }
  }

  private replay(row: WagerRow, key: string, hash: string): TransactionResult {
    assertReplay(row, key, hash);
    if (!row.result) throw new Error("Committed wager without result");
    return { ...structuredClone(row.result), idempotentReplay: true };
  }

  private newRow(t: WagerTransaction, ctx: EventContext): WagerRow {
    return Object.assign(new WagerRow(), {
      id: t.id,
      providerId: t.providerId,
      externalTransactionId: t.externalTransactionId,
      idempotencyKey: t.idempotencyKey,
      payloadHash: t.payloadHash,
      walletId: t.walletId,
      playerId: t.playerId,
      roundId: t.roundId,
      gameId: t.gameId,
      kind: t.kind,
      amount: t.money.toString(),
      currency: t.money.currency,
      referenceExternalTransactionId: t.referenceExternalTransactionId,
      referenceTransactionId: t.referenceTransactionId,
      status: t.status,
      createdAt: t.createdAt,
      processedAt: t.processedAt,
      attempts: 0,
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
    });
  }

  private result(t: WagerTransaction, wallet: Wallet): TransactionResult {
    return {
      transactionId: t.id,
      status: t.status,
      balance: wallet.balance.toJSON(),
      ...(t.failureCode ? { failureCode: t.failureCode } : {}),
      idempotentReplay: false,
    };
  }

  private async completeInbox(
    em: EntityManager,
    row: InboxRow,
    transactionId: string,
  ): Promise<void> {
    const inbox = InboxMessage.rehydrate(row);
    inbox.markProcessed(new Date());
    row.processedAt = inbox.processedAt;
    row.transactionId = transactionId;
    await em.flush();
  }

  private async apply(
    em: EntityManager,
    row: WagerRow,
    walletRow: WalletRow,
    ctx: EventContext,
    retry: boolean,
  ): Promise<TransactionResult> {
    const t = transactionDomain(row);
    const wallet = walletDomain(walletRow);
    const referenceRow = t.referenceExternalTransactionId
      ? await em.findOne(WagerRow, {
          providerId: t.providerId,
          externalTransactionId: t.referenceExternalTransactionId,
        })
      : undefined;
    const reference = referenceRow
      ? transactionDomain(referenceRow)
      : undefined;
    const reversed =
      reference && t.requiresReference()
        ? (await em.count(WagerRow, {
            referenceTransactionId: reference.id,
            kind: t.kind,
            status: WagerStatus.Processed,
          })) > 0
        : false;
    let decision = validateWager(t, wallet, reference, reversed);
    let entry: WalletLedgerEntry | undefined;
    let event: IntegrationEvent<unknown> | undefined;
    if (decision === "PENDING_REFERENCE") {
      if (retry) row.attempts += 1;
      const expired =
        Date.now() - row.createdAt.getTime() >= this.config.REFERENCE_TTL_MS;
      if (row.attempts >= this.config.REFERENCE_MAX_ATTEMPTS || expired) {
        decision = "REFERENCE_NOT_FOUND";
      } else {
        const first = t.status !== WagerStatus.PendingReference;
        t.markPendingReference();
        row.nextAttemptAt = new Date(
          Date.now() +
            Math.min(
              60_000,
              this.config.REFERENCE_BACKOFF_MS *
                2 ** Math.min(row.attempts, 16),
            ),
        );
        if (first) event = WagerTransactionPendingReference.from(t, ctx);
      }
    }
    if (decision === "READY") {
      try {
        if (t.affectsBalance()) {
          entry =
            t.ledgerDirectionFor(reference) === LedgerDirection.Debit
              ? wallet.debit(t.id, t.money, t.kind === WagerKind.Rollback)
              : wallet.credit(t.id, t.money);
        }
        t.markProcessed(reference?.id, new Date());
        event = WagerTransactionProcessed.from(t, ctx);
      } catch (error) {
        if (
          !(error instanceof DomainError) ||
          ![
            "INSUFFICIENT_FUNDS",
            "REVERSAL_INSUFFICIENT_FUNDS",
            "BALANCE_LIMIT_EXCEEDED",
          ].includes(error.code)
        )
          throw error;
        decision = error.code as FailureCode;
      }
    }
    if (decision !== "READY" && decision !== "PENDING_REFERENCE") {
      t.reject(decision);
      event = WagerTransactionRejected.from(t, ctx);
    }
    row.status = t.status;
    row.referenceTransactionId = t.referenceTransactionId;
    row.failureCode = t.failureCode;
    row.processedAt = t.processedAt;
    if (t.isTerminal()) row.nextAttemptAt = undefined;
    row.result = this.result(t, wallet);
    walletRow.balance = wallet.balance.toString();
    walletRow.version = wallet.version;
    walletRow.updatedAt = wallet.updatedAt;
    await em.flush();
    if (entry)
      em.persist([
        ledgerRow(entry),
        outboxRow(WalletBalanceChanged.from(wallet, entry, ctx)),
      ]);
    if (event) em.persist(outboxRow(event));
    return row.result;
  }

  async retryReferences(): Promise<number> {
    const candidates = await this.orm.em.fork().find(
      WagerRow,
      {
        status: WagerStatus.PendingReference,
        nextAttemptAt: { $lte: new Date() },
      },
      { limit: 25, orderBy: { nextAttemptAt: "ASC", id: "ASC" } },
    );
    let count = 0;
    for (const candidate of candidates) {
      try {
        const result = await this.financialTransaction(async (em) => {
          const wallet = await em.findOneOrFail(WalletRow, candidate.walletId, {
            lockMode: LockMode.PESSIMISTIC_WRITE,
          });
          const row = await em.findOneOrFail(WagerRow, candidate.id, {
            lockMode: LockMode.PESSIMISTIC_WRITE,
          });
          if (
            row.status !== WagerStatus.PendingReference ||
            !row.nextAttemptAt ||
            row.nextAttemptAt > new Date()
          )
            return undefined;
          return this.apply(
            em,
            row,
            wallet,
            { correlationId: row.correlationId, causationId: row.causationId },
            true,
          );
        });
        if (result) {
          count += 1;
          this.metrics.retries.inc({ source: "reference" });
          if (result.status !== WagerStatus.PendingReference)
            this.metrics.transactions.inc({ status: result.status });
          log(
            "info",
            "reference_retried",
            {
              correlationId: candidate.correlationId,
              transactionId: candidate.id,
              walletId: candidate.walletId,
              providerId: candidate.providerId,
            },
            result.failureCode,
          );
        }
      } catch (error) {
        log(
          "error",
          "reference_retry_failed",
          {
            correlationId: candidate.correlationId,
            transactionId: candidate.id,
            walletId: candidate.walletId,
            providerId: candidate.providerId,
          },
          databaseErrorCode(error) ?? "INFRASTRUCTURE_ERROR",
        );
        if (!transientError(error)) await this.failPending(candidate.id);
      }
    }
    return count;
  }

  private async failPending(id: string): Promise<void> {
    const candidate = await this.orm.em.fork().findOneOrFail(WagerRow, id);
    let changed = false;
    await this.orm.em.fork().transactional(async (em) => {
      const wallet = walletDomain(
        await em.findOneOrFail(WalletRow, candidate.walletId, {
          lockMode: LockMode.PESSIMISTIC_WRITE,
        }),
      );
      const row = await em.findOneOrFail(WagerRow, id, {
        lockMode: LockMode.PESSIMISTIC_WRITE,
      });
      const t = transactionDomain(row);
      if (t.isTerminal()) return;
      t.fail("PERMANENT_INFRASTRUCTURE_ERROR");
      row.status = t.status;
      row.failureCode = t.failureCode;
      row.processedAt = t.processedAt;
      row.nextAttemptAt = undefined;
      row.result = this.result(t, wallet);
      em.persist(
        outboxRow(
          WagerTransactionFailed.from(t, {
            correlationId: row.correlationId,
            causationId: row.causationId,
          }),
        ),
      );
      changed = true;
    });
    if (changed) {
      this.metrics.transactions.inc({ status: WagerStatus.Failed });
      log(
        "error",
        "transaction_failed",
        {
          correlationId: candidate.correlationId,
          transactionId: candidate.id,
          walletId: candidate.walletId,
          providerId: candidate.providerId,
        },
        "PERMANENT_INFRASTRUCTURE_ERROR",
      );
    }
  }

  private walletResponse(wallet: Wallet) {
    return {
      id: wallet.id,
      playerId: wallet.playerId,
      balance: wallet.balance.toJSON(),
      version: wallet.version,
    };
  }

  async getWallet(id: string) {
    const row = await this.orm.em.fork().findOne(WalletRow, id);
    if (!row) throw new RequestError("WALLET_NOT_FOUND", 404);
    return this.walletResponse(walletDomain(row));
  }

  async getTransaction(
    where:
      | { id: string }
      | { providerId: string; externalTransactionId: string },
  ) {
    const row = await this.orm.em.fork().findOne(WagerRow, where);
    if (!row) throw new RequestError("TRANSACTION_NOT_FOUND", 404);
    return {
      ...row.result,
      providerId: row.providerId,
      externalTransactionId: row.externalTransactionId,
      walletId: row.walletId,
      playerId: row.playerId,
      roundId: row.roundId,
      gameId: row.gameId,
      kind: row.kind,
      money: { amount: row.amount, currency: row.currency },
      referenceExternalTransactionId: row.referenceExternalTransactionId,
      referenceTransactionId: row.referenceTransactionId,
      createdAt: row.createdAt.toISOString(),
      processedAt: row.processedAt?.toISOString(),
    };
  }

  async ledger(walletId: string, cursor: string | undefined, limit: number) {
    await this.getWallet(walletId);
    let position = "0";
    if (cursor) {
      try {
        if (!/^[A-Za-z0-9_-]+$/.test(cursor)) throw new Error();
        const decoded: unknown = JSON.parse(
          Buffer.from(cursor, "base64url").toString(),
        );
        if (
          !decoded ||
          typeof decoded !== "object" ||
          !("walletId" in decoded) ||
          !("position" in decoded) ||
          decoded.walletId !== walletId ||
          typeof decoded.position !== "string" ||
          !/^\d{1,19}$/.test(decoded.position) ||
          BigInt(decoded.position) > 9223372036854775807n
        )
          throw new Error();
        position = decoded.position;
      } catch {
        throw new RequestError("INVALID_CURSOR", 400);
      }
    }
    const rows = await this.orm.em
      .fork()
      .find(
        LedgerRow,
        { walletId, position: { $gt: position } },
        { orderBy: { position: "ASC" }, limit: limit + 1 },
      );
    const hasNext = rows.length > limit;
    const selected = rows.slice(0, limit);
    const last = selected.at(-1);
    return {
      entries: selected.map((row) => ({
        id: row.id,
        walletId: row.walletId,
        transactionId: row.transactionId,
        direction: row.direction,
        money: { amount: row.amount, currency: row.currency },
        balanceBefore: { amount: row.balanceBefore, currency: row.currency },
        balanceAfter: { amount: row.balanceAfter, currency: row.currency },
        createdAt: row.createdAt.toISOString(),
      })),
      nextCursor:
        hasNext && last
          ? Buffer.from(
              JSON.stringify({ walletId, position: last.position }),
            ).toString("base64url")
          : null,
    };
  }

  async reconcile(walletId: string, ctx: EventContext) {
    const response = await this.orm.em.fork().transactional(async (em) => {
      const row = await em.findOne(WalletRow, walletId, {
        lockMode: LockMode.PESSIMISTIC_READ,
      });
      if (!row) throw new RequestError("WALLET_NOT_FOUND", 404);
      const totals = await sql<{ amount: string; count: string }>(
        em,
        `SELECT COALESCE(SUM(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END),0)::numeric(20,2)::text AS amount, COUNT(*)::text AS count FROM ledger WHERE wallet_id=?`,
        [walletId],
      );
      const total = totals[0];
      if (!total) throw new Error("Missing reconciliation aggregate");
      const stored = Money.rehydrate({
        amount: row.balance,
        currency: row.currency,
      });
      const calculated = Money.rehydrate({
        amount: total.amount,
        currency: row.currency,
      });
      return {
        walletId,
        storedBalance: stored.toJSON(),
        calculatedBalance: calculated.toJSON(),
        difference: stored.subtract(calculated).toJSON(),
        consistent: stored.equals(calculated),
        checkedEntries: Number(total.count),
      };
    });
    if (!response.consistent) {
      this.metrics.inconsistencies.inc();
      log("error", "reconciliation_mismatch", { ...ctx, walletId });
    }
    return response;
  }
}
