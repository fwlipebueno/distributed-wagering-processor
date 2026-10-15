import { afterAll, afterEach, beforeAll, expect, spyOn, test } from "bun:test";
import { randomUUID } from "node:crypto";
import {
  DeleteMessageCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  RequestThrottled,
  SendMessageCommand,
} from "@aws-sdk/client-sqs";
import { LockMode } from "@mikro-orm/core";
import type { WagerRequest } from "../../src/application/contracts.js";
import { WagerKind, WagerStatus } from "../../src/domain/transaction.js";
import { Wallet } from "../../src/domain/wallet.js";
import { createApplication } from "../../src/http/application.js";
import type { Config } from "../../src/infrastructure/config.js";
import { readConfig } from "../../src/infrastructure/config.js";
import { connectDatabase, sql } from "../../src/infrastructure/database.js";
import {
  type TransactionResult,
  WalletRow,
} from "../../src/infrastructure/entities.js";
import { Workers } from "../../src/infrastructure/workers.js";
import {
  api,
  ChildProcess,
  openWallet,
  request,
  submit,
  type WalletDTO,
  waitUntil,
} from "./helpers.js";

type App = Awaited<ReturnType<typeof createApplication>>;
let app: App;
let admin: Awaited<ReturnType<typeof connectDatabase>>;
const databaseName = `wager_test_${Date.now()}_${process.pid}`;
let config = readConfig();
const children: ChildProcess[] = [];

async function child(
  port: number,
  role: "api" | "worker",
  mode?: string,
  overrides: Partial<Config> = {},
) {
  const process = new ChildProcess(
    { ...config, ...overrides },
    port,
    role,
    mode,
  );
  children.push(process);
  await process.ready();
  return process;
}

async function count(
  table: "ledger" | "inbox" | "outbox" | "wagers",
  where = "true",
  params: unknown[] = [],
) {
  const result = await sql<{ count: string }>(
    app.orm.em.fork(),
    `SELECT count(*)::text AS count FROM ${table} WHERE ${where}`,
    params,
  );
  return Number(result[0]?.count ?? "0");
}

async function holdWallet(walletId: string) {
  const acquired = Promise.withResolvers<void>();
  const released = Promise.withResolvers<void>();
  const finished = app.orm.em.fork().transactional(async (em) => {
    await em.findOneOrFail(WalletRow, walletId, {
      lockMode: LockMode.PESSIMISTIC_WRITE,
    });
    acquired.resolve();
    await released.promise;
  });
  await Promise.race([acquired.promise, finished]);
  return { release: released.resolve, finished };
}

async function waitingLocks(event: "advisory" | "wallet", minimum: number) {
  await waitUntil(
    async () => {
      const rows = await sql<{ count: string }>(
        app.orm.em.fork(),
        "SELECT count(*)::text AS count FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND ((?='wallet' AND wait_event IN ('transactionid','tuple')) OR wait_event=?)",
        [event, event],
      );
      return Number(rows[0]?.count ?? "0") >= minimum;
    },
    2000,
    `Expected ${minimum} PostgreSQL ${event} waiters`,
  );
}

async function invariant() {
  const rows = await sql<{
    id: string;
    balance: string;
    calculated: string;
    version: number;
    expected_version: string;
  }>(
    app.orm.em.fork(),
    `
    SELECT w.id, w.balance::text, COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount ELSE -l.amount END),0)::numeric(20,2)::text AS calculated,
      w.version, (1+COUNT(*) FILTER (WHERE t.kind<>'OPENING'))::text AS expected_version
    FROM wallets w LEFT JOIN ledger l ON l.wallet_id=w.id LEFT JOIN wagers t ON t.id=l.transaction_id GROUP BY w.id`,
  );
  for (const row of rows) {
    expect(row.balance).toBe(row.calculated);
    expect(row.balance.startsWith("-")).toBe(false);
    expect(row.version).toBe(Number(row.expected_version));
  }
}

function envelope(body: WagerRequest, messageId = randomUUID()) {
  return {
    messageId,
    type: "WagerTransactionRequested",
    occurredAt: new Date().toISOString(),
    data: {
      ...body,
      idempotencyKey: `${body.providerId}:${body.externalTransactionId}`,
    },
  };
}

async function send(body: unknown, group = randomUUID()) {
  return app.sqs.client.send(
    new SendMessageCommand({
      QueueUrl: app.sqs.urls.requests,
      MessageBody: JSON.stringify(body),
      MessageGroupId: group,
      MessageDeduplicationId: randomUUID(),
    }),
  );
}

async function drainEvents() {
  const events: Record<string, unknown>[] = [];
  for (;;) {
    const batch = await app.sqs.client.send(
      new ReceiveMessageCommand({
        QueueUrl: app.sqs.urls.events,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 0,
      }),
    );
    if (!batch.Messages?.length) return events;
    for (const message of batch.Messages) {
      events.push(JSON.parse(message.Body ?? "{}") as Record<string, unknown>);
      await app.sqs.client.send(
        new DeleteMessageCommand({
          QueueUrl: app.sqs.urls.events,
          ReceiptHandle: message.ReceiptHandle,
        }),
      );
    }
  }
}

beforeAll(async () => {
  admin = await connectDatabase(config.DATABASE_URL);
  await sql(admin.em.fork(), `CREATE DATABASE "${databaseName}"`);
  const url = new URL(config.DATABASE_URL);
  url.pathname = `/${databaseName}`;
  config = readConfig({
    ...Object.fromEntries(
      Object.entries(config).map(([k, v]) => [k, String(v)]),
    ),
    DATABASE_URL: url.toString(),
    APP_ROLE: "api",
    PORT: "4100",
    SQL_LOCK_TIMEOUT_MS: "100",
    REFERENCE_BACKOFF_MS: "20",
    REFERENCE_MAX_ATTEMPTS: "3",
    SQS_QUEUE_PREFIX: `test_${Date.now()}_${process.pid}_`,
  });
  const orm = await connectDatabase(config.DATABASE_URL);
  await orm.getMigrator().up();
  await orm.close(true);
  app = await createApplication({ config });
  await Promise.all([child(4101, "api"), child(4102, "api")]);
  expect(
    new Set([process.pid, ...children.map((c) => c.process.pid)]).size,
  ).toBe(3);
  console.log(
    JSON.stringify({
      evidence: "three_independent_processes",
      pids: [process.pid, ...children.map((c) => c.process.pid)],
      database: databaseName,
    }),
  );
}, 60000);

afterEach(async () => {
  if (app) await invariant();
});

afterAll(async () => {
  await Promise.all(children.map((c) => c.stop("SIGKILL")));
  if (app) {
    await Promise.all(
      Object.values(app.sqs.urls).map((QueueUrl) =>
        app.sqs.client.send(new DeleteQueueCommand({ QueueUrl })),
      ),
    );
    await invariant();
    await app.close();
  }
  if (admin) {
    await sql(admin.em.fork(), `DROP DATABASE "${databaseName}" WITH (FORCE)`);
    await admin.close(true);
  }
});

test("versioned migration reverses and reapplies on PostgreSQL", async () => {
  await app.orm.getMigrator().down({ to: 0 });
  const removed = await sql<{ table: string | null }>(
    app.orm.em.fork(),
    "SELECT to_regclass('public.wallets')::text AS table",
  );
  expect(removed[0]?.table).toBeNull();
  await app.orm.getMigrator().up();
  const applied = await app.orm.getMigrator().getExecutedMigrations();
  expect(applied).toHaveLength(3);
  const index = await sql<{ definition: string }>(
    app.orm.em.fork(),
    "SELECT indexdef AS definition FROM pg_indexes WHERE indexname='ledger_transaction_id'",
  );
  expect(index[0]?.definition).toContain("(transaction_id)");
});

for (const transient of [false, true]) {
  test(`${transient ? "network errors retry" : "unknown errors fail immediately"} through SQL, HTTP and real SQS`, async () => {
    const w = await openWallet();
    const body = request(w);
    const message = envelope(body);
    const error = transient
      ? new TypeError("injected wrapped network failure", {
          cause: Object.assign(new Error("connection reset"), {
            code: "ECONNRESET",
          }),
        })
      : new Error("injected unexpected failure");
    const before = (await app.metrics.retries.get()).values;
    const sqlBefore = before.find((v) => v.labels.source === "sql")?.value ?? 0;
    const sqsBefore = before.find((v) => v.labels.source === "sqs")?.value ?? 0;
    let attempts = 0;
    // Failure occurs after wallet lock and wager/inbox INSERTs, before commit.
    const debit = Wallet.prototype.debit;
    const fault = spyOn(Wallet.prototype, "debit").mockImplementation(function (
      this: Wallet,
      transactionId,
      money,
      reversal,
    ) {
      if (this.id === w.id) {
        attempts += 1;
        throw error;
      }
      return debit.call(this, transactionId, money, reversal);
    });
    let receipt: string | undefined;
    try {
      const response = await api<{ error: { code: string } }>(
        "/wagering/transactions",
        {
          body,
          key: message.data.idempotencyKey,
        },
      );
      expect(response.status).toBe(transient ? 503 : 500);
      expect(response.body.error.code).toBe(
        transient ? "INFRASTRUCTURE_UNAVAILABLE" : "INTERNAL_ERROR",
      );
      expect(attempts).toBe(transient ? 3 : 1);
      await send(message);
      const delivery = (await app.sqs.receive()).Messages?.[0];
      if (!delivery?.ReceiptHandle)
        throw new Error("Missing fault-injection delivery");
      expect(delivery.Attributes?.ApproximateReceiveCount).toBe("1");
      receipt = delivery.ReceiptHandle;
      await app.workers.handle(delivery);
      expect(attempts).toBe(transient ? 6 : 2);
      const after = (await app.metrics.retries.get()).values;
      expect(after.find((v) => v.labels.source === "sql")?.value ?? 0).toBe(
        sqlBefore + (transient ? 4 : 0),
      );
      expect(after.find((v) => v.labels.source === "sqs")?.value ?? 0).toBe(
        sqsBefore + (transient ? 1 : 0),
      );
      expect(
        await count("wagers", "external_transaction_id=?", [
          body.externalTransactionId,
        ]),
      ).toBe(0);
      expect(await count("inbox", "message_id=?", [message.messageId])).toBe(0);
      expect(
        await count("ledger", "wallet_id=? AND direction='DEBIT'", [w.id]),
      ).toBe(0);
      expect(
        await count("outbox", "payload->>'correlationId'=?", [
          message.messageId,
        ]),
      ).toBe(0);
      expect(await app.sqs.deadLetterDepth()).toBe(transient ? 0 : 1);
      if (!transient) {
        const dead = (
          await app.sqs.client.send(
            new ReceiveMessageCommand({ QueueUrl: app.sqs.urls.dlq }),
          )
        ).Messages?.[0];
        expect(JSON.parse(dead?.Body ?? "null")).toEqual(message);
        await app.sqs.client.send(
          new DeleteMessageCommand({
            QueueUrl: app.sqs.urls.dlq,
            ReceiptHandle: dead?.ReceiptHandle,
          }),
        );
      }
    } finally {
      fault.mockRestore();
    }
    if (transient) {
      if (!receipt) throw new Error("Missing retry receipt");
      await app.sqs.visibility(receipt, 0);
      const retry = (await app.sqs.receive()).Messages?.[0];
      if (!retry) throw new Error("Missing network redelivery");
      expect(retry.Attributes?.ApproximateReceiveCount).toBe("2");
      await app.workers.handle(retry);
      expect(
        await count("inbox", "message_id=? AND processed_at IS NOT NULL", [
          message.messageId,
        ]),
      ).toBe(1);
      expect(
        await count("ledger", "wallet_id=? AND direction='DEBIT'", [w.id]),
      ).toBe(1);
    }
    expect((await api<WalletDTO>(`/wallets/${w.id}`)).body).toMatchObject({
      balance: { amount: transient ? "90.00" : "100.00" },
      version: transient ? 2 : 1,
    });
    const queue = await app.sqs.client.send(
      new GetQueueAttributesCommand({
        QueueUrl: app.sqs.urls.requests,
        AttributeNames: [
          "ApproximateNumberOfMessages",
          "ApproximateNumberOfMessagesNotVisible",
        ],
      }),
    );
    expect(queue.Attributes?.ApproximateNumberOfMessages).toBe("0");
    expect(queue.Attributes?.ApproximateNumberOfMessagesNotVisible).toBe("0");
  });
}

test("AWS throttling after commit retries the ACK without repeating the financial effect", async () => {
  const w = await openWallet();
  const message = envelope(request(w));
  await send(message);
  const delivery = (await app.sqs.receive()).Messages?.[0];
  if (!delivery?.ReceiptHandle) throw new Error("Missing throttled delivery");
  const ack = spyOn(app.sqs, "ack").mockRejectedValueOnce(
    new RequestThrottled({
      message: "throttled ACK",
      $metadata: { httpStatusCode: 400 },
    }),
  );
  try {
    await app.workers.handle(delivery);
    expect(ack).toHaveBeenCalledTimes(1);
  } finally {
    ack.mockRestore();
  }
  expect(await app.sqs.deadLetterDepth()).toBe(0);
  expect(
    await count("inbox", "message_id=? AND processed_at IS NOT NULL", [
      message.messageId,
    ]),
  ).toBe(1);
  expect(
    await count("ledger", "wallet_id=? AND direction='DEBIT'", [w.id]),
  ).toBe(1);
  await app.sqs.visibility(delivery.ReceiptHandle, 0);
  const retry = (await app.sqs.receive()).Messages?.[0];
  if (!retry) throw new Error("Missing throttling redelivery");
  expect(retry.Attributes?.ApproximateReceiveCount).toBe("2");
  await app.workers.handle(retry);
  expect(await count("inbox", "message_id=?", [message.messageId])).toBe(1);
  expect(
    await count("ledger", "wallet_id=? AND direction='DEBIT'", [w.id]),
  ).toBe(1);
  expect((await api<WalletDTO>(`/wallets/${w.id}`)).body).toMatchObject({
    balance: { amount: "90.00" },
    version: 2,
  });
  expect((await app.sqs.receive()).Messages ?? []).toHaveLength(0);
});

test("opening is atomic, zero opening has no ledger, duplicates conflict and money stays exact", async () => {
  const w = await openWallet();
  expect(w.version).toBe(1);
  expect(await count("ledger", "wallet_id=?", [w.id])).toBe(1);
  expect(
    await count(
      "wagers",
      "wallet_id=? AND kind='OPENING' AND status='PROCESSED'",
      [w.id],
    ),
  ).toBe(1);
  const duplicate = await api("/wallets", {
    body: { playerId: w.playerId, initialBalance: w.balance },
  });
  expect(duplicate.status).toBe(409);
  const zero = await openWallet("0.00");
  expect(await count("ledger", "wallet_id=?", [zero.id])).toBe(0);
  expect(await count("wagers", "wallet_id=?", [zero.id])).toBe(0);
  const large = await openWallet("9007199254740993.01");
  const result = await submit(
    request(large, {
      kind: WagerKind.Win,
      money: { amount: "0.02", currency: "BRL" },
    }),
  );
  expect(result.body.balance.amount).toBe("9007199254740993.03");
});

test("50 identical HTTP bets across three processes debit once and replay the historical balance", async () => {
  const w = await openWallet();
  const bet = request(w, { money: { amount: "80.00", currency: "BRL" } });
  const origins = [
    "http://127.0.0.1:4100",
    "http://127.0.0.1:4101",
    "http://127.0.0.1:4102",
  ];
  const responses = await Promise.all(
    Array.from({ length: 50 }, (_, i) => submit(bet, origins[i % 3])),
  );
  expect(
    responses.every(
      (r) => r.status === 200 && r.body.balance.amount === "20.00",
    ),
  ).toBe(true);
  expect(new Set(responses.map((r) => r.body.transactionId)).size).toBe(1);
  expect(responses.filter((r) => !r.body.idempotentReplay)).toHaveLength(1);
  expect(
    await count("ledger", "wallet_id=? AND direction='DEBIT'", [w.id]),
  ).toBe(1);
  await submit(
    request(w, {
      kind: WagerKind.Win,
      money: { amount: "50.00", currency: "BRL" },
    }),
  );
  const replay = await submit(bet);
  const original = responses[0];
  if (!original) throw new Error("No duplicate responses");
  expect(replay.body).toEqual({ ...original.body, idempotentReplay: true });
  expect((await api<WalletDTO>(`/wallets/${w.id}`)).body.balance.amount).toBe(
    "70.00",
  );
  expect((await submit({ ...bet, gameId: "changed" })).status).toBe(409);
  expect((await submit(bet, undefined, "different-key")).status).toBe(409);
});

test("cross-wallet identity races reject the losing payload without moving its wallet", async () => {
  const peers = await Promise.all([
    child(4120, "api", undefined, { SQL_LOCK_TIMEOUT_MS: 5000 }),
    child(4121, "api", undefined, { SQL_LOCK_TIMEOUT_MS: 5000 }),
  ]);
  // Both inserts reach the unique indexes only after the barrier is released.
  // A shared advisory lock lets them proceed together; it exists only in this fixture.
  await sql(
    app.orm.em.fork(),
    `CREATE FUNCTION gate_test_identity() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock_shared(20141014,1); RETURN NEW; END $$;
     CREATE TRIGGER gate_test_identity BEFORE INSERT ON wagers FOR EACH ROW WHEN (NEW.provider_id='identity-race') EXECUTE FUNCTION gate_test_identity()`,
  );
  try {
    for (const collision of ["key", "external", "both"] as const) {
      const wallets = await Promise.all([openWallet(), openWallet()]);
      const external = randomUUID();
      const key = randomUUID();
      const bodies = wallets.map((wallet, i) =>
        request(wallet, {
          providerId: "identity-race",
          externalTransactionId:
            collision === "key" && i === 1 ? randomUUID() : external,
        }),
      );
      const acquired = Promise.withResolvers<void>();
      const released = Promise.withResolvers<void>();
      const barrier = app.orm.em.fork().transactional(async (em) => {
        await sql(em, "SELECT pg_advisory_xact_lock(20141014,1)");
        acquired.resolve();
        await released.promise;
      });
      await Promise.race([acquired.promise, barrier]);
      const pending = Promise.all(
        bodies.map((body, i) =>
          submit(
            body,
            `http://127.0.0.1:${4120 + i}`,
            collision === "external" && i === 1 ? randomUUID() : key,
          ),
        ),
      );
      let responses: Awaited<typeof pending>;
      try {
        await waitingLocks("advisory", 2);
      } finally {
        released.resolve();
        await barrier;
        responses = await pending;
      }
      expect(responses.map((r) => r.status).sort()).toEqual([200, 409]);
      for (let i = 0; i < wallets.length; i += 1) {
        const wallet = wallets[i];
        const response = responses[i];
        if (!wallet || !response) throw new Error("Missing race participant");
        const won = response.status === 200;
        expect(
          (await api<WalletDTO>(`/wallets/${wallet.id}`)).body,
        ).toMatchObject({
          balance: { amount: won ? "90.00" : "100.00" },
          version: won ? 2 : 1,
        });
        expect(
          await count("wagers", "wallet_id=? AND kind='BET'", [wallet.id]),
        ).toBe(won ? 1 : 0);
        expect(
          await count("ledger", "wallet_id=? AND direction='DEBIT'", [
            wallet.id,
          ]),
        ).toBe(won ? 1 : 0);
        expect(await count("outbox", "aggregate_id=?", [wallet.id])).toBe(
          won ? 4 : 2,
        );
      }
    }
  } finally {
    await sql(
      app.orm.em.fork(),
      "DROP TRIGGER gate_test_identity ON wagers; DROP FUNCTION gate_test_identity()",
    );
    await Promise.all(peers.map((peer) => peer.stop()));
  }
});

test("HTTP and SQS race on one wallet, then distinct deliveries replay the persisted historical result", async () => {
  const w = await openWallet();
  const body = request(w);
  const message = envelope(body);
  const http = await child(4122, "api", undefined, {
    SQL_LOCK_TIMEOUT_MS: 5000,
  });
  const worker = await child(4123, "worker", undefined, {
    SQL_LOCK_TIMEOUT_MS: 5000,
  });
  const lock = await holdWallet(w.id);
  const httpResult = submit(body, "http://127.0.0.1:4122");
  try {
    await send(message);
    await waitingLocks("wallet", 2);
  } finally {
    lock.release();
    await lock.finished;
  }
  try {
    const response = await httpResult;
    expect(response.status).toBe(200);
    await waitUntil(
      async () =>
        (await count(
          "inbox",
          "message_id=? AND transaction_id=? AND processed_at IS NOT NULL",
          [message.messageId, response.body.transactionId],
        )) === 1,
    );
    expect(
      await count("ledger", "transaction_id=?", [response.body.transactionId]),
    ).toBe(1);
    expect(
      await count("outbox", "payload->'data'->>'transactionId'=?", [
        response.body.transactionId,
      ]),
    ).toBe(2);
    expect(response.body.balance.amount).toBe("90.00");
    await submit(
      request(w, {
        kind: WagerKind.Win,
        money: { amount: "50.00", currency: "BRL" },
      }),
    );
    const replay = await submit(body, "http://127.0.0.1:4102");
    expect(replay.body).toEqual({ ...response.body, idempotentReplay: true });
    const redelivery = envelope(body);
    expect(redelivery.messageId).not.toBe(message.messageId);
    await send(redelivery);
    await waitUntil(
      async () =>
        (await count(
          "inbox",
          "message_id=? AND transaction_id=? AND processed_at IS NOT NULL",
          [redelivery.messageId, response.body.transactionId],
        )) === 1,
    );
    expect(
      (await app.wagers.getTransaction({ id: response.body.transactionId }))
        .balance?.amount,
    ).toBe("90.00");
    expect((await api<WalletDTO>(`/wallets/${w.id}`)).body.balance.amount).toBe(
      "140.00",
    );
    expect(
      await count("ledger", "wallet_id=? AND direction='DEBIT'", [w.id]),
    ).toBe(1);
    expect(
      await count("outbox", "payload->'data'->>'transactionId'=?", [
        response.body.transactionId,
      ]),
    ).toBe(2);
    await waitUntil(
      () =>
        worker.logs.includes(redelivery.messageId) &&
        worker.logs.includes("transaction_replayed"),
    );
  } finally {
    await Promise.all([http.stop(), worker.stop()]);
  }
});

test("a real PostgreSQL deadlock rolls back its victim and retries the financial transaction once", async () => {
  const w = await openWallet();
  const body = request(w, { providerId: "deadlock-test" });
  const peer = await child(4124, "api", undefined, {
    SQL_LOCK_TIMEOUT_MS: 5000,
  });
  // Only this fixture introduces the reverse lock order. The application's
  // transaction is made the detecting victim; the opposing transaction then exits.
  await sql(
    app.orm.em.fork(),
    `
    CREATE FUNCTION gate_test_deadlock() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
      PERFORM set_config('deadlock_timeout','50ms',true);
      PERFORM pg_advisory_xact_lock(20141014,2);
      RETURN NEW;
    END $$;
    CREATE TRIGGER gate_test_deadlock BEFORE INSERT ON wagers FOR EACH ROW WHEN (NEW.provider_id='deadlock-test') EXECUTE FUNCTION gate_test_deadlock()`,
  );
  const acquired = Promise.withResolvers<void>();
  const compete = Promise.withResolvers<void>();
  const opponent = app.orm.em.fork().transactional(async (em) => {
    await sql(em, "SET LOCAL deadlock_timeout='10s'");
    await sql(em, "SELECT pg_advisory_xact_lock(20141014,2)");
    acquired.resolve();
    await compete.promise;
    await em.findOneOrFail(WalletRow, w.id, {
      lockMode: LockMode.PESSIMISTIC_WRITE,
    });
  });
  try {
    await Promise.race([acquired.promise, opponent]);
    const pending = submit(body, "http://127.0.0.1:4124");
    try {
      await waitingLocks("advisory", 1);
    } finally {
      compete.resolve();
    }
    const [response] = await Promise.all([pending, opponent]);
    expect(response.status).toBe(200);
    expect(response.body.balance.amount).toBe("90.00");
    expect(await count("wagers", "wallet_id=? AND kind='BET'", [w.id])).toBe(1);
    expect(
      await count("ledger", "transaction_id=?", [response.body.transactionId]),
    ).toBe(1);
    expect(
      await count("outbox", "payload->'data'->>'transactionId'=?", [
        response.body.transactionId,
      ]),
    ).toBe(2);
    const metrics = await (await fetch("http://127.0.0.1:4124/metrics")).text();
    expect(metrics).toContain("wager_lock_conflicts_total 1");
    expect(metrics).toContain('wager_retries_total{source="sql"} 1');
  } finally {
    compete.resolve();
    await opponent;
    await sql(
      app.orm.em.fork(),
      "DROP TRIGGER gate_test_deadlock ON wagers; DROP FUNCTION gate_test_deadlock()",
    );
    await peer.stop();
  }
});

test("numeric(20,2) boundary credits reject atomically and retain the original rejection on replay", async () => {
  const max = "999999999999999999.99";
  const w = await openWallet("999999999999999999.98");
  const cent = { amount: "0.01", currency: "BRL" };
  const credited = await submit(
    request(w, { kind: WagerKind.Win, money: cent }),
  );
  expect(credited.status).toBe(200);
  expect(credited.body.balance.amount).toBe(max);
  const overflowing = request(w, { kind: WagerKind.Win, money: cent });
  const rejected = await submit(overflowing);
  expect(rejected.status).toBe(422);
  expect(rejected.body).toMatchObject({
    status: WagerStatus.Rejected,
    failureCode: "BALANCE_LIMIT_EXCEEDED",
    balance: { amount: max },
  });
  expect(
    await count("ledger", "transaction_id=?", [rejected.body.transactionId]),
  ).toBe(0);
  expect(
    await count(
      "outbox",
      "payload->'data'->>'transactionId'=? AND event_type='WagerTransactionRejected'",
      [rejected.body.transactionId],
    ),
  ).toBe(1);
  expect(
    await count(
      "outbox",
      "payload->'data'->>'transactionId'=? AND event_type<>'WagerTransactionRejected'",
      [rejected.body.transactionId],
    ),
  ).toBe(0);
  expect((await api<WalletDTO>(`/wallets/${w.id}`)).body).toMatchObject({
    balance: { amount: max },
    version: 2,
  });
  const invalid = request(w, {
    kind: WagerKind.Win,
    money: { amount: "1000000000000000000.00", currency: "BRL" },
  });
  expect((await submit(invalid)).status).toBe(400);
  expect(
    await count("wagers", "external_transaction_id=?", [
      invalid.externalTransactionId,
    ]),
  ).toBe(0);
  const debitBody = request(w, { money: { amount: max, currency: "BRL" } });
  const debit = await submit(debitBody);
  expect(debit.status).toBe(200);
  expect(debit.body.balance.amount).toBe("0.00");
  const replay = await submit(overflowing);
  expect(replay.status).toBe(422);
  expect(replay.body).toEqual({ ...rejected.body, idempotentReplay: true });
  const restored = await submit(
    request(w, {
      kind: WagerKind.Rollback,
      money: { amount: max, currency: "BRL" },
      referenceExternalTransactionId: debitBody.externalTransactionId,
    }),
  );
  expect(restored.status).toBe(200);
  expect(restored.body.balance.amount).toBe(max);
  expect((await api<WalletDTO>(`/wallets/${w.id}`)).body.version).toBe(4);
});

test("two 80.00 bets compete over 100.00: one processed, one rejected, final 20.00", async () => {
  const w = await openWallet();
  const bets = [
    request(w, { money: { amount: "80.00", currency: "BRL" } }),
    request(w, { money: { amount: "80.00", currency: "BRL" } }),
  ];
  const results = await Promise.all(
    bets.map((b, i) => submit(b, `http://127.0.0.1:${4101 + i}`)),
  );
  expect(results.map((r) => r.status).sort()).toEqual([200, 422]);
  expect(results.find((r) => r.status === 422)?.body.failureCode).toBe(
    "INSUFFICIENT_FUNDS",
  );
  expect((await api<WalletDTO>(`/wallets/${w.id}`)).body.balance.amount).toBe(
    "20.00",
  );
  expect(
    await count("ledger", "wallet_id=? AND direction='DEBIT'", [w.id]),
  ).toBe(1);
  for (const bet of bets)
    expect((await submit(bet)).body.idempotentReplay).toBe(true);
});

test("a locked wallet cannot block an independent wallet", async () => {
  const a = await openWallet();
  const b = await openWallet();
  let unlock: () => void = () => {};
  let locked: () => void = () => {};
  const acquired = new Promise<void>((resolve) => {
    locked = resolve;
  });
  const gate = new Promise<void>((resolve) => {
    unlock = resolve;
  });
  const lock = app.orm.em.fork().transactional(async (em) => {
    await em.findOneOrFail(WalletRow, a.id, {
      lockMode: LockMode.PESSIMISTIC_WRITE,
    });
    locked();
    await gate;
  });
  await acquired;
  const blocked = submit(request(a), "http://127.0.0.1:4101");
  try {
    expect((await submit(request(b), "http://127.0.0.1:4102")).status).toBe(
      200,
    );
  } finally {
    unlock();
    await lock;
  }
  expect((await blocked).status).toBe(200);
  const wallets = await Promise.all(
    Array.from({ length: 12 }, () => openWallet()),
  );
  expect(
    (await Promise.all(wallets.map((w) => submit(request(w))))).every(
      (r) => r.status === 200,
    ),
  ).toBe(true);
});

test("REFUND and ROLLBACK before BET are resolved by the scheduled worker", async () => {
  const worker = await child(4110, "worker");
  try {
    for (const kind of [WagerKind.Refund, WagerKind.Rollback] as const) {
      const w = await openWallet();
      const bet = request(w);
      const reversal = request(w, {
        kind,
        referenceExternalTransactionId: bet.externalTransactionId,
      });
      const pending = await submit(reversal);
      expect(pending.status).toBe(202);
      expect(await count("ledger", "wallet_id=?", [w.id])).toBe(1);
      expect((await submit(bet)).status).toBe(200);
      await waitUntil(
        async () =>
          (await app.wagers.getTransaction({ id: pending.body.transactionId }))
            .status === WagerStatus.Processed,
      );
      expect((await submit(reversal)).body.balance.amount).toBe("100.00");
      expect(
        await count(
          "outbox",
          "event_type='WagerTransactionPendingReference' AND payload->'data'->>'transactionId'=?",
          [pending.body.transactionId],
        ),
      ).toBe(1);
    }
  } finally {
    await worker.stop();
  }
});

test("concurrent full reversals allow one per type and preserve original references", async () => {
  const w = await openWallet();
  const bet = request(w);
  await submit(bet);
  const refunds = Array.from({ length: 10 }, () =>
    request(w, {
      kind: WagerKind.Refund,
      referenceExternalTransactionId: bet.externalTransactionId,
    }),
  );
  const results = await Promise.all(
    refunds.map((r, i) => submit(r, `http://127.0.0.1:${4100 + (i % 3)}`)),
  );
  expect(results.filter((r) => r.status === 200)).toHaveLength(1);
  expect(
    results.filter((r) => r.body.failureCode === "REFERENCE_ALREADY_REVERSED"),
  ).toHaveLength(9);
  const rolls = await Promise.all(
    Array.from({ length: 8 }, (_, i) =>
      submit(
        request(w, {
          kind: WagerKind.Rollback,
          referenceExternalTransactionId: bet.externalTransactionId,
        }),
        `http://127.0.0.1:${4100 + (i % 3)}`,
      ),
    ),
  );
  expect(rolls.filter((r) => r.status === 200)).toHaveLength(1);
  expect((await api<WalletDTO>(`/wallets/${w.id}`)).body.balance.amount).toBe(
    "110.00",
  );
});

test("LOSS has no movement; invalid references, currency, player and debit reversals are rejected auditably", async () => {
  const w = await openWallet();
  const loss = await submit(
    request(w, {
      kind: WagerKind.Loss,
      money: { amount: "0.00", currency: "BRL" },
    }),
  );
  expect(loss.status).toBe(200);
  expect(
    await count("ledger", "transaction_id=?", [loss.body.transactionId]),
  ).toBe(0);
  expect((await api<WalletDTO>(`/wallets/${w.id}`)).body.version).toBe(1);
  expect(
    (
      await submit(
        request(w, {
          kind: WagerKind.Win,
          money: { amount: "10.00", currency: "USD" },
        }),
      )
    ).body.failureCode,
  ).toBe("CURRENCY_MISMATCH");
  expect(
    (await submit(request(w, { playerId: randomUUID() }))).body.failureCode,
  ).toBe("PLAYER_MISMATCH");
  const bet = request(w);
  await submit(bet);
  for (const props of [
    { money: { amount: "5.00", currency: "BRL" } },
    { roundId: "other" },
  ]) {
    const result = await submit(
      request(w, {
        kind: WagerKind.Refund,
        referenceExternalTransactionId: bet.externalTransactionId,
        ...props,
      }),
    );
    expect(result.status).toBe(422);
  }
  const other = await openWallet();
  const otherBet = request(other);
  await submit(otherBet);
  expect(
    (
      await submit(
        request(w, {
          kind: WagerKind.Rollback,
          referenceExternalTransactionId: otherBet.externalTransactionId,
        }),
      )
    ).body.failureCode,
  ).toBe("REFERENCE_CONTEXT_MISMATCH");
  const win = request(w, {
    kind: WagerKind.Win,
    money: { amount: "20.00", currency: "BRL" },
  });
  await submit(win);
  expect(
    (
      await submit(
        request(w, {
          kind: WagerKind.Refund,
          referenceExternalTransactionId: win.externalTransactionId,
          money: win.money,
        }),
      )
    ).body.failureCode,
  ).toBe("REFERENCE_KIND_INVALID");
  await submit(request(w, { money: { amount: "100.00", currency: "BRL" } }));
  const reverse = await submit(
    request(w, {
      kind: WagerKind.Rollback,
      money: win.money,
      referenceExternalTransactionId: win.externalTransactionId,
    }),
  );
  expect(reverse.body.failureCode).toBe("REVERSAL_INSUFFICIENT_FUNDS");
  expect(
    await count("ledger", "transaction_id=?", [reverse.body.transactionId]),
  ).toBe(0);
  const rejected = request(w, { money: { amount: "100.00", currency: "BRL" } });
  await submit(rejected);
  expect(
    (
      await submit(
        request(w, {
          kind: WagerKind.Rollback,
          money: rejected.money,
          referenceExternalTransactionId: rejected.externalTransactionId,
        }),
      )
    ).body.failureCode,
  ).toBe("REFERENCE_NOT_PROCESSED");
});

test("exhausted references stay rejected after the reference arrives, including HTTP and SQS replay", async () => {
  const w = await openWallet();
  const reference = request(w);
  const refund = request(w, {
    kind: WagerKind.Refund,
    referenceExternalTransactionId: reference.externalTransactionId,
  });
  const pending = await submit(refund);
  for (let attempt = 0; attempt < config.REFERENCE_MAX_ATTEMPTS; attempt += 1) {
    await sql(
      app.orm.em.fork(),
      "UPDATE wagers SET next_attempt_at=now()-interval '1 second' WHERE id=?",
      [pending.body.transactionId],
    );
    await app.wagers.retryReferences();
  }
  const result = await app.wagers.getTransaction({
    id: pending.body.transactionId,
  });
  expect(result.status).toBe(WagerStatus.Rejected);
  expect(result.failureCode).toBe("REFERENCE_NOT_FOUND");
  expect(
    await count("ledger", "transaction_id=?", [pending.body.transactionId]),
  ).toBe(0);
  expect(
    await count(
      "outbox",
      "event_type='WagerTransactionRejected' AND payload->'data'->>'transactionId'=?",
      [pending.body.transactionId],
    ),
  ).toBe(1);
  const terminal = await submit(refund);
  expect(terminal.status).toBe(422);
  expect((await submit(reference)).status).toBe(200);
  await app.wagers.retryReferences();
  const replay = await submit(refund);
  expect(replay.status).toBe(422);
  expect(replay.body).toEqual({ ...terminal.body, idempotentReplay: true });
  expect(replay.body.balance.amount).toBe("100.00");
  const message = envelope(refund);
  await send(message);
  const received = await app.sqs.receive();
  if (!received.Messages?.[0])
    throw new Error("Missing terminal replay delivery");
  await app.workers.handle(received.Messages[0]);
  expect(
    await count(
      "inbox",
      "message_id=? AND transaction_id=? AND processed_at IS NOT NULL",
      [message.messageId, pending.body.transactionId],
    ),
  ).toBe(1);
  expect(
    await app.wagers.getTransaction({ id: pending.body.transactionId }),
  ).toEqual(result);
  expect(
    await count("ledger", "transaction_id=?", [pending.body.transactionId]),
  ).toBe(0);
  expect(
    await count("outbox", "payload->'data'->>'transactionId'=?", [
      pending.body.transactionId,
    ]),
  ).toBe(2);
  expect((await api<WalletDTO>(`/wallets/${w.id}`)).body).toMatchObject({
    balance: { amount: "90.00" },
    version: 2,
  });
});

test("database rejects negative balances, edits, deletes, truncates, naked updates and duplicate entries", async () => {
  const w = await openWallet();
  const wager = await submit(request(w));
  const queries: [string, unknown[]][] = [
    ["UPDATE wallets SET balance=-1 WHERE id=?", [w.id]],
    ["UPDATE wallets SET balance='NaN'::numeric WHERE id=?", [w.id]],
    ["UPDATE wallets SET currency='USD' WHERE id=?", [w.id]],
    ["UPDATE wallets SET balance=balance+1 WHERE id=?", [w.id]],
    ["UPDATE wallets SET version=version+1 WHERE id=?", [w.id]],
    ["UPDATE ledger SET amount=1 WHERE wallet_id=?", [w.id]],
    ["DELETE FROM ledger WHERE wallet_id=?", [w.id]],
    ["TRUNCATE ledger CASCADE", []],
    [
      "UPDATE wagers SET status='PENDING' WHERE id=?",
      [wager.body.transactionId],
    ],
    ["DELETE FROM wagers WHERE id=?", [wager.body.transactionId]],
    [
      "INSERT INTO ledger(id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after,created_at) SELECT ?,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after,created_at FROM ledger WHERE transaction_id=?",
      [randomUUID(), wager.body.transactionId],
    ],
  ];
  for (const [query, params] of queries)
    await expect(sql(app.orm.em.fork(), query, params)).rejects.toThrow();
});

test("outbox failure rolls back wallet, ledger, transaction and inbox together", async () => {
  const w = await openWallet();
  const body = request(w);
  const message = envelope(body);
  const before = {
    wagers: await count("wagers"),
    ledger: await count("ledger"),
    inbox: await count("inbox"),
    outbox: await count("outbox"),
  };
  await sql(
    app.orm.em.fork(),
    `CREATE FUNCTION reject_test_outbox() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.payload->>'correlationId'='${message.messageId}' THEN RAISE EXCEPTION 'injected SQL failure' USING ERRCODE='23514'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_test_outbox BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION reject_test_outbox()`,
  );
  try {
    await expect(
      app.wagers.execute(
        body,
        message.data.idempotencyKey,
        { correlationId: message.messageId },
        { messageId: message.messageId, payloadHash: "atomic-hash" },
      ),
    ).rejects.toThrow();
    expect({
      wagers: await count("wagers"),
      ledger: await count("ledger"),
      inbox: await count("inbox"),
      outbox: await count("outbox"),
    }).toEqual(before);
    expect((await api<WalletDTO>(`/wallets/${w.id}`)).body.balance.amount).toBe(
      "100.00",
    );
  } finally {
    await sql(
      app.orm.em.fork(),
      "DROP TRIGGER reject_test_outbox ON outbox; DROP FUNCTION reject_test_outbox()",
    );
  }
});

test("publisher cannot see or publish financial events before SQL commit", async () => {
  while (await app.workers.publishOne()) {}
  await drainEvents();
  const w = await openWallet();
  while (await app.workers.publishOne()) {}
  await drainEvents();
  await sql(
    app.orm.em.fork(),
    `CREATE FUNCTION sleep_test_outbox() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.payload->>'correlationId'='uncommitted-test' THEN PERFORM pg_sleep(1); END IF; RETURN NEW; END $$; CREATE TRIGGER sleep_test_outbox BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION sleep_test_outbox()`,
  );
  const body = request(w);
  try {
    const pending = api<TransactionResult>("/wagering/transactions", {
      body,
      key: `${body.providerId}:${body.externalTransactionId}`,
      correlation: "uncommitted-test",
    });
    await waitUntil(
      async () =>
        (
          await sql<{ count: string }>(
            app.orm.em.fork(),
            "SELECT count(*)::text AS count FROM pg_stat_activity WHERE datname=? AND wait_event='PgSleep'",
            [databaseName],
          )
        )[0]?.count !== "0",
    );
    expect(await app.workers.publishOne()).toBe(false);
    expect(await drainEvents()).toHaveLength(0);
    expect((await pending).status).toBe(200);
    while (await app.workers.publishOne()) {}
    expect(await drainEvents()).toHaveLength(2);
  } finally {
    await sql(
      app.orm.em.fork(),
      "DROP TRIGGER sleep_test_outbox ON outbox; DROP FUNCTION sleep_test_outbox()",
    );
  }
});

test("50 broker redeliveries bypass FIFO dedup but persistent inbox debits once", async () => {
  const w = await openWallet();
  const body = request(w);
  const message = envelope(body);
  await Promise.all(Array.from({ length: 50 }, () => send(message)));
  const workers = await Promise.all([
    child(4111, "worker"),
    child(4112, "worker"),
  ]);
  try {
    await waitUntil(async () => {
      const attrs = await app.sqs.client.send(
        new GetQueueAttributesCommand({
          QueueUrl: app.sqs.urls.requests,
          AttributeNames: [
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
          ],
        }),
      );
      return (
        attrs.Attributes?.ApproximateNumberOfMessages === "0" &&
        attrs.Attributes?.ApproximateNumberOfMessagesNotVisible === "0" &&
        (await count("inbox", "message_id=?", [message.messageId])) === 1
      );
    }, 30000);
    expect(
      await count("ledger", "wallet_id=? AND direction='DEBIT'", [w.id]),
    ).toBe(1);
    expect((await api<WalletDTO>(`/wallets/${w.id}`)).body.balance.amount).toBe(
      "90.00",
    );
    const replay = await submit(body);
    expect(replay.body.idempotentReplay).toBe(true);
    const originalInbox = await sql(
      app.orm.em.fork(),
      "SELECT * FROM inbox WHERE message_id=?",
      [message.messageId],
    );
    const originalTransaction = await app.wagers.getTransaction({
      id: replay.body.transactionId,
    });
    const conflict = {
      ...message,
      data: { ...message.data, gameId: "conflicting" },
    };
    await send(conflict);
    await waitUntil(async () =>
      workers.some((c) => c.logs.includes("INBOX_PAYLOAD_CONFLICT")),
    );
    expect(
      await count("ledger", "wallet_id=? AND direction='DEBIT'", [w.id]),
    ).toBe(1);
    expect(
      await sql(app.orm.em.fork(), "SELECT * FROM inbox WHERE message_id=?", [
        message.messageId,
      ]),
    ).toEqual(originalInbox);
    expect(
      await app.wagers.getTransaction({ id: replay.body.transactionId }),
    ).toEqual(originalTransaction);
    let diagnostic: unknown;
    await waitUntil(async () => {
      const dead = await app.sqs.client.send(
        new ReceiveMessageCommand({
          QueueUrl: app.sqs.urls.dlq,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 0,
        }),
      );
      for (const entry of dead.Messages ?? []) {
        const value = JSON.parse(entry.Body ?? "{}") as { messageId?: string };
        if (value.messageId === message.messageId) diagnostic = value;
        await app.sqs.client.send(
          new DeleteMessageCommand({
            QueueUrl: app.sqs.urls.dlq,
            ReceiptHandle: entry.ReceiptHandle,
          }),
        );
      }
      return diagnostic !== undefined;
    });
    expect(diagnostic).toEqual(conflict);
  } finally {
    await Promise.all(workers.map((w) => w.stop()));
  }
});

test("killed worker after commit and before ack redelivers without another financial effect", async () => {
  const w = await openWallet();
  const body = request(w);
  const message = envelope(body);
  const killed = await child(4113, "worker", "crash-after-commit");
  await send(message);
  await killed.marker("afterCommit");
  expect(
    await count("inbox", "message_id=? AND processed_at IS NOT NULL", [
      message.messageId,
    ]),
  ).toBe(1);
  expect((await api<WalletDTO>(`/wallets/${w.id}`)).body.balance.amount).toBe(
    "90.00",
  );
  await killed.stop("SIGKILL");
  const recovered = await child(4114, "worker");
  try {
    await waitUntil(
      () =>
        recovered.logs.includes("message_acknowledged") &&
        recovered.logs.includes(message.messageId),
      45000,
    );
    expect(recovered.logs).toContain("transaction_replayed");
    expect(
      await count("ledger", "wallet_id=? AND direction='DEBIT'", [w.id]),
    ).toBe(1);
  } finally {
    await recovered.stop();
  }
});

test("two concurrent publishers drain one outbox and recover after a publish/mark crash", async () => {
  while (await app.workers.publishOne()) {}
  await drainEvents();
  const w = await openWallet();
  await submit(request(w));
  const killed = await child(4115, "worker", "crash-after-publish");
  await killed.marker("afterPublish");
  const match = killed.logs
    .split("\n")
    .find((line) => line.includes('"marker":"afterPublish"'));
  if (!match) throw new Error("Missing crash marker");
  const marker = JSON.parse(match) as { eventId: string };
  expect(
    await count("outbox", "id=? AND published_at IS NULL", [marker.eventId]),
  ).toBe(1);
  await killed.stop("SIGKILL");
  for (let i = 0; i < 10; i += 1)
    await submit(
      request(w, {
        kind: WagerKind.Win,
        money: { amount: "1.00", currency: "BRL" },
      }),
    );
  const publishers = await Promise.all([
    child(4116, "worker"),
    child(4117, "worker"),
  ]);
  try {
    await waitUntil(
      async () => (await count("outbox", "published_at IS NULL")) === 0,
      20000,
    );
  } finally {
    await Promise.all(publishers.map((p) => p.stop()));
  }
  expect(
    await count("outbox", "id=? AND published_at IS NOT NULL", [
      marker.eventId,
    ]),
  ).toBe(1);
  const events = await drainEvents();
  const expected = await count("outbox", "aggregate_id=?", [w.id]);
  expect(events.filter((e) => e.aggregateId === w.id)).toHaveLength(expected);
  expect(new Set(events.map((e) => e.eventId)).size).toBe(events.length);
  expect(events.some((e) => e.eventId === marker.eventId)).toBe(true);
  expect(
    events.every(
      (e) =>
        typeof e.eventId === "string" &&
        e.version === 1 &&
        typeof e.correlationId === "string",
    ),
  ).toBe(true);
});

test("real lock errors retry through SQS, exhaust to DLQ, while business rejection is acked", async () => {
  const w = await openWallet();
  const lock = await holdWallet(w.id);
  const retrying = envelope(request(w));
  let receipt: string;
  try {
    await send(retrying);
    const failed = await app.sqs.receive();
    expect(failed.Messages).toHaveLength(1);
    const first = failed.Messages?.[0];
    if (!first?.ReceiptHandle) throw new Error("Missing SQS receipt");
    receipt = first.ReceiptHandle;
    await app.workers.handle(first);
    expect(await count("inbox", "message_id=?", [retrying.messageId])).toBe(0);
  } finally {
    lock.release();
    await lock.finished;
  }
  await app.sqs.visibility(receipt, 0);
  const retry = await app.sqs.receive();
  if (!retry.Messages?.[0]) throw new Error("Missing retry");
  expect(
    Number(retry.Messages[0].Attributes?.ApproximateReceiveCount),
  ).toBeGreaterThan(1);
  await app.workers.handle(retry.Messages[0]);
  expect(await count("inbox", "message_id=?", [retrying.messageId])).toBe(1);

  const lock2 = await holdWallet(w.id);
  const exhausted = envelope(request(w));
  await send(exhausted);
  const limited = new Workers(app.orm, app.wagers, app.sqs, app.metrics, {
    ...config,
    SQS_MAX_ATTEMPTS: 2,
  });
  try {
    for (let i = 0; i < 2; i += 1) {
      const response = await app.sqs.receive();
      const message = response.Messages?.[0];
      if (!message?.ReceiptHandle) throw new Error("Missing exhausted receipt");
      await limited.handle(message);
      if (i === 0) await app.sqs.visibility(message.ReceiptHandle, 0);
    }
  } finally {
    lock2.release();
    await lock2.finished;
  }
  expect(await count("inbox", "message_id=?", [exhausted.messageId])).toBe(0);
  await send({ invalid: true });
  const poison = await app.sqs.receive();
  if (!poison.Messages?.[0]) throw new Error("Missing poison");
  await app.workers.handle(poison.Messages[0]);
  const business = envelope(
    request(w, { money: { amount: "1000.00", currency: "BRL" } }),
  );
  await send(business);
  const rejection = await app.sqs.receive();
  if (!rejection.Messages?.[0]) throw new Error("Missing rejection");
  await app.workers.handle(rejection.Messages[0]);
  expect(
    await count("wagers", "external_transaction_id=? AND status='REJECTED'", [
      business.data.externalTransactionId,
    ]),
  ).toBe(1);
  const dead = await app.sqs.client.send(
    new ReceiveMessageCommand({
      QueueUrl: app.sqs.urls.dlq,
      MaxNumberOfMessages: 10,
    }),
  );
  expect(
    dead.Messages?.some((m) => m.Body?.includes(exhausted.messageId)),
  ).toBe(true);
  expect(dead.Messages?.some((m) => m.Body?.includes('"invalid":true'))).toBe(
    true,
  );
  expect(dead.Messages?.some((m) => m.Body?.includes(business.messageId))).toBe(
    false,
  );
  const metrics = await app.metrics.registry.metrics();
  expect(metrics).toContain('wager_retries_total{source="sqs"}');
  expect(metrics).toContain("wager_lock_conflicts_total");
  expect(metrics).toContain("wager_dlq_total");
});

test("SIGTERM completes an in-flight delivery and restart retains consistent wallets", async () => {
  const w = await openWallet();
  const message = envelope(request(w));
  const worker = await child(4118, "worker", "graceful-inflight");
  await send(message);
  await worker.marker("afterCommit");
  await worker.stop();
  expect(worker.logs).toContain("message_acknowledged");
  expect(worker.logs).toContain("service_stopped");
  const apiProcess = children[0];
  if (!apiProcess) throw new Error("Missing API child");
  await apiProcess.stop();
  const restarted = await child(4101, "api");
  expect(
    (
      await api<WalletDTO>(`/wallets/${w.id}`, {
        origin: "http://127.0.0.1:4101",
      })
    ).body.balance.amount,
  ).toBe("90.00");
  const { idempotencyKey: _key, ...business } = message.data;
  expect(
    (await submit(business, "http://127.0.0.1:4101")).body.idempotentReplay,
  ).toBe(true);
  expect(restarted.process.pid).not.toBe(apiProcess.process.pid);
});

test("opaque cursor, query endpoints, errors, reconciliation and health contracts", async () => {
  const w = await openWallet();
  const body = request(w);
  const tx = await submit(body);
  await submit(request(w, { kind: WagerKind.Win }));
  interface Page {
    entries: { id: string }[];
    nextCursor: string | null;
  }
  const first = await api<Page>(`/wallets/${w.id}/ledger?limit=1`);
  expect(first.body.entries).toHaveLength(1);
  expect(typeof first.body.nextCursor).toBe("string");
  const second = await api<Page>(
    `/wallets/${w.id}/ledger?limit=1&cursor=${first.body.nextCursor}`,
  );
  expect(second.body.entries[0]?.id).not.toBe(first.body.entries[0]?.id);
  expect((await api(`/wallets/${w.id}/ledger?cursor=broken`)).status).toBe(400);
  const other = await openWallet();
  expect(
    (await api(`/wallets/${other.id}/ledger?cursor=${first.body.nextCursor}`))
      .status,
  ).toBe(400);
  expect((await api(`/wallets/${w.id}/ledger?limit=0`)).status).toBe(400);
  expect(
    (await api(`/wagering/transactions/${tx.body.transactionId}`)).status,
  ).toBe(200);
  expect(
    (
      await api(
        `/providers/provider-a/wagering/transactions/${body.externalTransactionId}`,
      )
    ).status,
  ).toBe(200);
  expect((await api(`/wallets/${randomUUID()}`)).status).toBe(404);
  expect((await api("/wagering/transactions", { body })).status).toBe(400);
  expect(
    (
      await api("/wagering/transactions", {
        body: { ...body, kind: "OPENING" },
        key: "opening",
      })
    ).status,
  ).toBe(400);
  const reconciliation = await api<{
    consistent: boolean;
    difference: { amount: string };
    checkedEntries: number;
  }>(`/wallets/${w.id}/reconciliation`, { method: "POST" });
  expect(reconciliation.body.consistent).toBe(true);
  expect(reconciliation.body.difference.amount).toBe("0.00");
  expect(reconciliation.body.checkedEntries).toBe(3);
  expect((await api("/health/live")).status).toBe(200);
  expect((await api("/health/ready")).status).toBe(200);
  const metrics = await fetch("http://127.0.0.1:4100/metrics");
  expect(metrics.status).toBe(200);
  expect(await metrics.text()).toContain("wager_outbox_lag_seconds");
});

test("outbox persists broker failures and recovers; readiness detects SQS unavailability", async () => {
  while (await app.workers.publishOne()) {}
  await drainEvents();
  await openWallet();
  await app.sqs.client.send(
    new DeleteQueueCommand({ QueueUrl: app.sqs.urls.events }),
  );
  expect((await api("/health/ready")).status).toBe(503);
  expect((await api("/health/live")).status).toBe(200);
  expect(await app.workers.publishOne()).toBe(true);
  expect(
    await count(
      "outbox",
      "attempts=1 AND next_attempt_at>now() AND published_at IS NULL",
    ),
  ).toBe(1);
  await app.sqs.initialize();
  await sql(
    app.orm.em.fork(),
    "UPDATE outbox SET next_attempt_at=now() WHERE published_at IS NULL",
  );
  while (await app.workers.publishOne()) {}
  expect(await count("outbox", "published_at IS NULL")).toBe(0);
  expect((await api("/health/ready")).status).toBe(200);
});

test("reconciliation signals divergence, emits a metric and leaves the observed balance intact", async () => {
  const w = await openWallet();
  await sql(
    app.orm.em.fork(),
    "ALTER TABLE wallets DISABLE TRIGGER wallet_consistency",
  );
  try {
    await sql(
      app.orm.em.fork(),
      "UPDATE wallets SET balance='99.00' WHERE id=?",
      [w.id],
    );
  } finally {
    await sql(
      app.orm.em.fork(),
      "ALTER TABLE wallets ENABLE TRIGGER wallet_consistency",
    );
  }
  try {
    const before =
      (await app.metrics.inconsistencies.get()).values[0]?.value ?? 0;
    const response = await api<{
      consistent: boolean;
      difference: { amount: string };
      storedBalance: { amount: string };
      calculatedBalance: { amount: string };
    }>(`/wallets/${w.id}/reconciliation`, {
      method: "POST",
      correlation: "deliberate-test-corruption",
    });
    expect(response.body.consistent).toBe(false);
    expect(response.body.difference.amount).toBe("-1.00");
    expect(response.body.storedBalance.amount).toBe("99.00");
    expect(response.body.calculatedBalance.amount).toBe("100.00");
    expect((await api<WalletDTO>(`/wallets/${w.id}`)).body.balance.amount).toBe(
      "99.00",
    );
    expect((await app.metrics.inconsistencies.get()).values[0]?.value).toBe(
      before + 1,
    );
  } finally {
    await sql(
      app.orm.em.fork(),
      "UPDATE wallets SET balance='100.00' WHERE id=?",
      [w.id],
    );
  }
});

test("permanent infrastructure error on an accepted reference becomes immutable FAILED without a financial effect", async () => {
  const w = await openWallet();
  const bet = request(w);
  const pending = await submit(
    request(w, {
      kind: WagerKind.Refund,
      referenceExternalTransactionId: bet.externalTransactionId,
    }),
  );
  expect(pending.status).toBe(202);
  await submit(bet);
  await sql(
    app.orm.em.fork(),
    `CREATE FUNCTION permanent_test_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id='${pending.body.transactionId}' AND NEW.status='PROCESSED' THEN RAISE EXCEPTION 'permanent infrastructure failure' USING ERRCODE='23514'; END IF; RETURN NEW; END $$; CREATE TRIGGER permanent_test_failure BEFORE UPDATE ON wagers FOR EACH ROW EXECUTE FUNCTION permanent_test_failure()`,
  );
  try {
    await sql(
      app.orm.em.fork(),
      "UPDATE wagers SET next_attempt_at=now()-interval '1 second' WHERE id=?",
      [pending.body.transactionId],
    );
    await app.wagers.retryReferences();
    const failed = await app.wagers.getTransaction({
      id: pending.body.transactionId,
    });
    expect(failed.status).toBe(WagerStatus.Failed);
    expect(failed.failureCode).toBe("PERMANENT_INFRASTRUCTURE_ERROR");
    expect(failed.balance?.amount).toBe("90.00");
    expect(
      await count("ledger", "transaction_id=?", [pending.body.transactionId]),
    ).toBe(0);
    expect(
      await count(
        "outbox",
        "event_type='WagerTransactionFailed' AND payload->'data'->>'transactionId'=?",
        [pending.body.transactionId],
      ),
    ).toBe(1);
    await expect(
      sql(
        app.orm.em.fork(),
        "UPDATE wagers SET status='PENDING_REFERENCE' WHERE id=?",
        [pending.body.transactionId],
      ),
    ).rejects.toThrow();
  } finally {
    await sql(
      app.orm.em.fork(),
      "DROP TRIGGER permanent_test_failure ON wagers; DROP FUNCTION permanent_test_failure()",
    );
  }
});

test("SIGTERM exceeding drain grace stops visibility renewal and hands the delivery to another worker", async () => {
  const w = await openWallet();
  const body = request(w);
  const message = envelope(body);
  const draining = await child(4120, "worker", "crash-after-commit");
  await send(message);
  await draining.marker("afterCommit");
  await draining.stop("SIGTERM");
  expect(draining.logs).toContain("service_stopped");
  expect(draining.logs).not.toContain("message_acknowledged");
  const recovered = await child(4121, "worker");
  try {
    await waitUntil(
      () =>
        recovered.logs.includes("message_acknowledged") &&
        recovered.logs.includes(message.messageId),
      5000,
      "Drain did not return visibility",
    );
    expect(recovered.logs).toContain("transaction_replayed");
    expect(
      await count("ledger", "wallet_id=? AND direction='DEBIT'", [w.id]),
    ).toBe(1);
  } finally {
    await recovered.stop();
  }
});
