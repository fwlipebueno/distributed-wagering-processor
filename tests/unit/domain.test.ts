import { describe, expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import {
  QueueDoesNotExist,
  RequestThrottled,
  SQSServiceException,
} from "@aws-sdk/client-sqs";
import {
  assertReplay,
  canonicalJSON,
  parseMessage,
  parseWager,
  payloadHash,
} from "../../src/application/contracts.js";
import { InvalidTransactionStateError } from "../../src/domain/errors.js";
import {
  WagerTransactionProcessed,
  WalletBalanceChanged,
} from "../../src/domain/events.js";
import { InboxMessage, OutboxMessage } from "../../src/domain/messages.js";
import { Money } from "../../src/domain/money.js";
import {
  validateWager,
  WagerKind,
  WagerStatus,
  WagerTransaction,
} from "../../src/domain/transaction.js";
import {
  LedgerDirection,
  Wallet,
  WalletLedgerEntry,
} from "../../src/domain/wallet.js";
import { transientError } from "../../src/infrastructure/database.js";

const money = (amount = "10.00", currency = "BRL") =>
  Money.from({ amount, currency });
const wallet = () =>
  Wallet.open({
    id: randomUUID(),
    playerId: randomUUID(),
    initialBalance: money("100.00"),
  });
function transaction(
  w: Wallet,
  kind = WagerKind.Bet,
  overrides: Partial<Parameters<typeof WagerTransaction.create>[0]> = {},
) {
  return WagerTransaction.create({
    id: randomUUID(),
    providerId: "provider-a",
    externalTransactionId: randomUUID(),
    idempotencyKey: randomUUID(),
    payloadHash: "hash",
    walletId: w.id,
    playerId: w.playerId,
    roundId: "r",
    gameId: "g",
    kind,
    money: money(),
    createdAt: new Date(),
    ...overrides,
  });
}

describe("Money: exact decimal strings", () => {
  test("adds cents without rounding and remains immutable", () => {
    const a = money("0.10");
    expect(a.add(money("0.20")).toJSON()).toEqual({
      amount: "0.30",
      currency: "BRL",
    });
    expect(a.toString()).toBe("0.10");
    expect(Object.isFrozen(a)).toBe(true);
    expect(money("9007199254740993.01").add(money("0.02")).toString()).toBe(
      "9007199254740993.03",
    );
    expect(
      money("999999999999999999.99").add(money("0.01")).exceedsStorageLimit(),
    ).toBe(true);
  });
  test("subtracts, negates and compares signed domain values", () => {
    expect(money("0.00").subtract(money("1.01")).toString()).toBe("-1.01");
    expect(money("1.01").negate().negate().equals(money("1.01"))).toBe(true);
    expect(money("0.00").isZero()).toBe(true);
    expect(money().isPositive()).toBe(true);
    expect(money().negate().isNegative()).toBe(true);
    expect(money("0.01").isLessThan(money())).toBe(true);
  });
  for (const invalid of [
    "NaN",
    "Infinity",
    "1e2",
    "",
    "1.001",
    "1",
    "1.0",
    "-1.00",
    "+1.00",
    " 1.00",
    "01.00",
    "1000000000000000000.00",
  ]) {
    test(`rejects ${JSON.stringify(invalid)} instead of rounding`, () =>
      expect(() => money(invalid)).toThrow("INVALID_AMOUNT"));
  }
  test("rejects monetary numbers and unknown currencies", () => {
    expect(() =>
      Money.from({ amount: 1 as unknown as string, currency: "BRL" }),
    ).toThrow();
    expect(() => money("1.00", "XYZ")).toThrow("INVALID_CURRENCY");
  });
  test("every binary operation rejects currency mismatch", () => {
    for (const op of ["add", "subtract", "equals", "isLessThan"] as const) {
      expect(() => money()[op](money("10.00", "USD"))).toThrow(
        "CURRENCY_MISMATCH",
      );
    }
  });
  test("rehydration preserves signed persisted values without input transitions", () => {
    expect(
      Money.rehydrate({ amount: "-0.01", currency: "BRL" }).toString(),
    ).toBe("-0.01");
  });
});

describe("Wallet and immutable ledger", () => {
  test("opening version is one, changes produce matching ledger and increment once", () => {
    const w = wallet();
    expect(w.version).toBe(1);
    const debit = w.debit("bet", money("80.00"));
    expect(w.balance.toString()).toBe("20.00");
    expect(w.version).toBe(2);
    expect(debit.isBalanced()).toBe(true);
    expect(debit.direction).toBe(LedgerDirection.Debit);
    const credit = w.credit("win", money("5.00"));
    expect(w.balance.toString()).toBe("25.00");
    expect(w.version).toBe(3);
    expect(credit.isBalanced()).toBe(true);
    expect(Object.isFrozen(credit)).toBe(true);
    const originalDate = credit.createdAt.toISOString();
    credit.createdAt.setTime(0);
    expect(credit.createdAt.toISOString()).toBe(originalDate);
  });
  test("failed movements preserve balance, version and date", () => {
    const w = wallet();
    const before = w.updatedAt.toISOString();
    expect(() => w.debit("bet", money("101.00"))).toThrow("INSUFFICIENT_FUNDS");
    expect(() => w.debit("rollback", money("101.00"), true)).toThrow(
      "REVERSAL_INSUFFICIENT_FUNDS",
    );
    expect(() => w.credit("usd", money("1.00", "USD"))).toThrow(
      "CURRENCY_MISMATCH",
    );
    expect(() => w.credit("zero", money("0.00"))).toThrow("UNBALANCED_LEDGER");
    expect(w.balance.toString()).toBe("100.00");
    expect(w.version).toBe(1);
    expect(w.updatedAt.toISOString()).toBe(before);
  });
  test("rejects inconsistent ledger arithmetic and negative opening", () => {
    expect(() =>
      WalletLedgerEntry.create({
        id: "l",
        walletId: "w",
        transactionId: "t",
        direction: LedgerDirection.Credit,
        money: money(),
        balanceBefore: money(),
        balanceAfter: money(),
        createdAt: new Date(),
      }),
    ).toThrow("UNBALANCED_LEDGER");
    expect(() =>
      Wallet.open({ id: "w", playerId: "p", initialBalance: money().negate() }),
    ).toThrow("NEGATIVE_BALANCE");
  });
});

describe("Wager transitions and business rules", () => {
  test("directions of BET/WIN/REFUND/ROLLBACK and no LOSS ledger", () => {
    const w = wallet();
    const bet = transaction(w);
    bet.markProcessed(undefined, new Date());
    const win = transaction(w, WagerKind.Win);
    win.markProcessed(undefined, new Date());
    const refund = transaction(w, WagerKind.Refund, {
      referenceExternalTransactionId: bet.externalTransactionId,
    });
    const rollback = transaction(w, WagerKind.Rollback, {
      referenceExternalTransactionId: bet.externalTransactionId,
    });
    expect(bet.ledgerDirectionFor()).toBe(LedgerDirection.Debit);
    expect(win.ledgerDirectionFor()).toBe(LedgerDirection.Credit);
    expect(refund.ledgerDirectionFor()).toBe(LedgerDirection.Credit);
    expect(rollback.ledgerDirectionFor(bet)).toBe(LedgerDirection.Credit);
    expect(rollback.ledgerDirectionFor(win)).toBe(LedgerDirection.Debit);
    expect(rollback.ledgerDirectionFor(refund)).toBe(LedgerDirection.Debit);
    const loss = transaction(w, WagerKind.Loss, { money: money("0.00") });
    expect(loss.affectsBalance()).toBe(false);
    expect(() => loss.ledgerDirectionFor()).toThrow("LOSS_HAS_NO_LEDGER");
  });
  for (const status of [
    WagerStatus.Processed,
    WagerStatus.Rejected,
    WagerStatus.Failed,
  ]) {
    test(`${status} cannot transition`, () => {
      const t = transaction(wallet());
      if (status === WagerStatus.Processed)
        t.markProcessed(undefined, new Date());
      else if (status === WagerStatus.Rejected) t.reject("INSUFFICIENT_FUNDS");
      else t.fail("PERMANENT_INFRASTRUCTURE_ERROR");
      expect(t.isTerminal()).toBe(true);
      for (const mutate of [
        () => t.markPendingReference(),
        () => t.markProcessed(undefined, new Date()),
        () => t.reject("REFERENCE_NOT_FOUND"),
        () => t.fail("PERMANENT_INFRASTRUCTURE_ERROR"),
      ]) {
        expect(mutate).toThrow(InvalidTransactionStateError);
      }
    });
  }
  test("pending reference may retry then reach terminal", () => {
    const t = transaction(wallet());
    t.markPendingReference();
    t.markPendingReference();
    expect(t.status).toBe(WagerStatus.PendingReference);
    t.markProcessed("ref", new Date());
    expect(t.referenceTransactionId).toBe("ref");
  });
  test("reference and positivity requirements", () => {
    for (const kind of [WagerKind.Refund, WagerKind.Rollback])
      expect(() => transaction(wallet(), kind)).toThrow("REFERENCE_REQUIRED");
    expect(() =>
      transaction(wallet(), WagerKind.Bet, { money: money("0.00") }),
    ).toThrow("AMOUNT_MUST_BE_POSITIVE");
    expect(() =>
      transaction(wallet(), WagerKind.Bet, {
        referenceExternalTransactionId: "ref",
      }),
    ).toThrow("REFERENCE_NOT_ALLOWED");
  });
  test("LOSS rejects signed internal amounts while allowing zero and positive amounts", () => {
    for (const amount of [
      money("0.01").negate(),
      money("0.00").subtract(money("1.00")),
      Money.rehydrate({ amount: "-1.00", currency: "BRL" }),
    ]) {
      expect(() =>
        transaction(wallet(), WagerKind.Loss, { money: amount }),
      ).toThrow("INVALID_AMOUNT");
    }
    for (const amount of [money("0.00"), money("1.00")]) {
      const loss = transaction(wallet(), WagerKind.Loss, { money: amount });
      expect(loss.status).toBe(WagerStatus.Pending);
      expect(loss.affectsBalance()).toBe(false);
    }
  });
  test("validates full reference context, kind, amount, status and one reversal per kind", () => {
    const w = wallet();
    const bet = transaction(w);
    bet.markProcessed(undefined, new Date());
    const refund = transaction(w, WagerKind.Refund, {
      referenceExternalTransactionId: bet.externalTransactionId,
    });
    expect(validateWager(refund, w, bet, false)).toBe("READY");
    expect(validateWager(refund, w, undefined, false)).toBe(
      "PENDING_REFERENCE",
    );
    expect(validateWager(refund, w, bet, true)).toBe(
      "REFERENCE_ALREADY_REVERSED",
    );
    for (const overrides of [
      { providerId: "b" },
      { walletId: "other" },
      { playerId: "other" },
      { roundId: "other" },
      { money: money("10.00", "EUR") },
    ]) {
      const ref = transaction(w, WagerKind.Bet, overrides);
      ref.markProcessed(undefined, new Date());
      expect(validateWager(refund, w, ref, false)).toBe(
        "REFERENCE_CONTEXT_MISMATCH",
      );
    }
    const wrongAmount = transaction(w, WagerKind.Bet, {
      money: money("20.00"),
    });
    wrongAmount.markProcessed(undefined, new Date());
    expect(validateWager(refund, w, wrongAmount, false)).toBe(
      "REFERENCE_AMOUNT_MISMATCH",
    );
    const win = transaction(w, WagerKind.Win);
    win.markProcessed(undefined, new Date());
    expect(validateWager(refund, w, win, false)).toBe("REFERENCE_KIND_INVALID");
    const rejected = transaction(w);
    rejected.reject("INSUFFICIENT_FUNDS");
    expect(validateWager(refund, w, rejected, false)).toBe(
      "REFERENCE_NOT_PROCESSED",
    );
    expect(
      validateWager(
        transaction(w, WagerKind.Win, { money: money("1.00", "USD") }),
        w,
        undefined,
        false,
      ),
    ).toBe("CURRENCY_MISMATCH");
    expect(
      validateWager(
        transaction(w, WagerKind.Bet, { playerId: "other" }),
        w,
        undefined,
        false,
      ),
    ).toBe("PLAYER_MISMATCH");
  });
});

describe("Canonical idempotency and contracts", () => {
  const request = {
    providerId: "provider-a",
    externalTransactionId: "e",
    playerId: randomUUID(),
    walletId: randomUUID(),
    roundId: "r",
    gameId: "g",
    kind: WagerKind.Bet,
    money: { amount: "1.00", currency: "BRL" },
  };
  test("recursively sorts keys and preserves business values", () => {
    expect(canonicalJSON({ b: 2, a: { z: 1, a: 3 } })).toBe(
      '{"a":{"a":3,"z":1},"b":2}',
    );
    expect(payloadHash(request)).toBe(
      payloadHash({ ...request, money: { currency: "BRL", amount: "1.00" } }),
    );
    const parsed = parseWager(request, "custom-key");
    expect(parsed.hash).toBe(parseWager(request, "another-key").hash);
    expect(() =>
      assertReplay(
        { idempotencyKey: "k", payloadHash: parsed.hash },
        "k",
        payloadHash({ ...request, gameId: "other" }),
      ),
    ).toThrow("IDEMPOTENCY_CONFLICT");
    expect(() =>
      assertReplay(
        { idempotencyKey: "k", payloadHash: parsed.hash },
        "other-key",
        parsed.hash,
      ),
    ).toThrow("IDEMPOTENCY_CONFLICT");
  });
  test("header is required, internal OPENING and unknown fields are rejected", () => {
    expect(() => parseWager(request, undefined)).toThrow(
      "INVALID_IDEMPOTENCY_KEY",
    );
    expect(() => parseWager({ ...request, kind: "OPENING" }, "k")).toThrow(
      "INVALID_PAYLOAD",
    );
    expect(() => parseWager({ ...request, extra: true }, "k")).toThrow(
      "INVALID_PAYLOAD",
    );
    expect(() =>
      parseWager({ ...request, providerId: "internal" }, "k"),
    ).toThrow("INVALID_PAYLOAD");
  });
  test("queue validates identity, kind, envelope and transport-independent business hash", () => {
    const envelope = {
      messageId: "m",
      type: "WagerTransactionRequested",
      occurredAt: new Date().toISOString(),
      data: { ...request, idempotencyKey: "k" },
    };
    expect(parseMessage(JSON.stringify(envelope)).hash).toBe(
      parseWager(request, "k").hash,
    );
    expect(() =>
      parseMessage(
        JSON.stringify({
          ...envelope,
          data: { ...envelope.data, kind: "OPENING" },
        }),
      ),
    ).toThrow();
    expect(() => parseMessage("{")).toThrow("INVALID_JSON");
  });
});

test("events and message state keep stable envelopes, due dates and processing state", () => {
  const w = wallet();
  const t = transaction(w);
  const entry = w.debit(t.id, t.money);
  t.markProcessed(undefined, new Date());
  const event = WalletBalanceChanged.from(w, entry, {
    correlationId: "c",
    causationId: "m",
  });
  expect(event.toJSON()).toMatchObject({
    eventType: "WalletBalanceChanged",
    version: 1,
    correlationId: "c",
    causationId: "m",
    data: { money: { amount: "10.00", currency: "BRL" }, walletVersion: 2 },
  });
  const outbox = OutboxMessage.enqueue(
    WagerTransactionProcessed.from(t, { correlationId: "c" }),
  );
  const now = new Date();
  expect(outbox.isDue(now)).toBe(true);
  outbox.scheduleRetry(now);
  expect(outbox.attempts).toBe(1);
  expect(outbox.isDue(now)).toBe(false);
  expect(outbox.isDue(new Date(now.getTime() + 1000))).toBe(true);
  outbox.markPublished(now);
  expect(outbox.isPending()).toBe(false);
  expect(() => outbox.scheduleRetry(now)).toThrow();
  const inbox = InboxMessage.receive({
    messageId: "m",
    consumerName: "c",
    payloadHash: "h",
  });
  expect(inbox.isProcessed()).toBe(false);
  inbox.markProcessed(now);
  expect(inbox.isProcessed()).toBe(true);
  expect(() => inbox.markProcessed(now)).toThrow();
});

describe("Domain dates do not expose mutable state", () => {
  const instant = "2026-10-14T12:00:00.000Z";

  test("wallet creation and rehydration isolate creation and update dates", () => {
    const opened = wallet();
    const original = opened.createdAt.toISOString();
    opened.createdAt.setTime(0);
    expect(opened.createdAt.toISOString()).toBe(original);
    expect(opened.updatedAt.toISOString()).toBe(original);
    const createdAt = new Date(instant);
    const updatedAt = new Date(instant);
    const restored = Wallet.rehydrate({
      id: "w",
      playerId: "p",
      balance: money(),
      version: 7,
      createdAt,
      updatedAt,
    });
    createdAt.setTime(0);
    updatedAt.setTime(0);
    restored.createdAt.setTime(0);
    restored.updatedAt.setTime(0);
    expect(restored.createdAt.toISOString()).toBe(instant);
    expect(restored.updatedAt.toISOString()).toBe(instant);
    expect(restored.version).toBe(7);
    expect(restored.balance.toString()).toBe("10.00");
  });

  test("wager factories isolate input dates without changing persisted status", () => {
    const createdAt = new Date(instant);
    const props = {
      id: "t",
      providerId: "p",
      externalTransactionId: "e",
      idempotencyKey: "k",
      payloadHash: "h",
      walletId: "w",
      playerId: "player",
      roundId: "r",
      gameId: "g",
      kind: WagerKind.Bet,
      money: money(),
      createdAt,
    };
    const created = WagerTransaction.create(props);
    const processedAt = new Date(instant);
    const restored = WagerTransaction.rehydrate({
      ...props,
      status: WagerStatus.Processed,
      processedAt,
    });
    createdAt.setTime(0);
    processedAt.setTime(0);
    created.createdAt.setTime(0);
    restored.createdAt.setTime(0);
    restored.processedAt?.setTime(0);
    expect(created.createdAt.toISOString()).toBe(instant);
    expect(created.status).toBe(WagerStatus.Pending);
    expect(restored.createdAt.toISOString()).toBe(instant);
    expect(restored.processedAt?.toISOString()).toBe(instant);
    expect(restored.status).toBe(WagerStatus.Processed);
  });

  test("inbox dates cannot be changed through factory input or getters", () => {
    const received = InboxMessage.receive({
      messageId: "m",
      consumerName: "c",
      payloadHash: "h",
    });
    const original = received.receivedAt.toISOString();
    received.receivedAt.setTime(0);
    expect(received.receivedAt.toISOString()).toBe(original);
    const receivedAt = new Date(instant);
    const processedAt = new Date(instant);
    const restored = InboxMessage.rehydrate({
      messageId: "m",
      consumerName: "c",
      payloadHash: "h",
      receivedAt,
      processedAt,
    });
    receivedAt.setTime(0);
    processedAt.setTime(0);
    restored.receivedAt.setTime(0);
    restored.processedAt?.setTime(0);
    expect(restored.receivedAt.toISOString()).toBe(instant);
    expect(restored.processedAt?.toISOString()).toBe(instant);
    expect(restored.isProcessed()).toBe(true);
  });

  test("outbox input dates cannot change its schedule or publication state", () => {
    const occurredAt = new Date(instant);
    const nextAttemptAt = new Date(occurredAt.getTime() + 1000);
    const publishedAt = new Date(instant);
    const state = {
      id: "o",
      aggregateId: "w",
      eventType: "E",
      payload: {},
      occurredAt,
      nextAttemptAt,
      attempts: 2,
    };
    const pending = OutboxMessage.rehydrate(state);
    const published = OutboxMessage.rehydrate({ ...state, publishedAt });
    occurredAt.setTime(0);
    nextAttemptAt.setTime(0);
    publishedAt.setTime(0);
    pending.occurredAt.setTime(0);
    pending.nextAttemptAt?.setTime(0);
    published.publishedAt?.setTime(0);
    expect(pending.occurredAt.toISOString()).toBe(instant);
    expect(pending.isDue(new Date(instant))).toBe(false);
    expect(pending.nextAttemptAt?.getTime()).toBe(
      new Date(instant).getTime() + 1000,
    );
    expect(pending.attempts).toBe(2);
    expect(published.publishedAt?.toISOString()).toBe(instant);
    expect(published.isPending()).toBe(false);
  });

  test("event timestamp access cannot change its serialized envelope", () => {
    const t = transaction(wallet());
    t.markProcessed(undefined, new Date(instant));
    const event = WagerTransactionProcessed.from(t, { correlationId: "c" });
    const original = event.toJSON();
    event.occurredAt.setTime(0);
    expect(event.toJSON()).toEqual(original);
  });
});

describe("Transient infrastructure errors require a recognized signal", () => {
  test("unknown errors and permanent PostgreSQL errors are not retryable", () => {
    for (const error of [
      new Error("unexpected invariant failure"),
      new Error("wrapper", { cause: new Error("unknown failure") }),
      new TypeError("invalid object"),
      new RangeError("invalid range"),
      new SyntaxError("invalid syntax"),
      undefined,
      null,
      "failure",
      {},
      ...["23505", "23514", "42601", "28P01", "3D000"].map((code) =>
        Object.assign(new Error("permanent SQL failure"), { code }),
      ),
    ])
      expect(transientError(error)).toBe(false);
  });

  test("TypeError and RangeError wrappers preserve the classification of their causes", () => {
    const network = Object.assign(new Error("connection reset"), {
      code: "ECONNRESET",
    });
    const permanent = Object.assign(new Error("constraint violation"), {
      code: "23514",
    });
    for (const ErrorType of [TypeError, RangeError]) {
      expect(transientError(new ErrorType("generic failure"))).toBe(false);
      expect(
        transientError(new ErrorType("wrapped failure", { cause: network })),
      ).toBe(true);
      expect(
        transientError(new ErrorType("wrapped failure", { cause: permanent })),
      ).toBe(false);
    }
  });

  test("PostgreSQL concurrency, availability and connection SQLSTATEs remain retryable", () => {
    for (const code of [
      "40001",
      "40P01",
      "55P03",
      "57014",
      "53300",
      "57P01",
      "57P02",
      "57P03",
      "08001",
      "08006",
    ])
      expect(
        transientError(
          Object.assign(new Error("PostgreSQL failure"), { code }),
        ),
      ).toBe(true);
  });

  test("network errors remain retryable through a wrapped cause", () => {
    for (const code of [
      "ECONNRESET",
      "ECONNREFUSED",
      "ETIMEDOUT",
      "EPIPE",
      "EAI_AGAIN",
      "ENOTFOUND",
      "EHOSTUNREACH",
      "ENETUNREACH",
    ]) {
      const error = Object.assign(new Error("network failure"), { code });
      expect(transientError(error)).toBe(true);
      expect(transientError(new Error("wrapper", { cause: error }))).toBe(true);
    }
    expect(
      transientError(
        Object.assign(new Error("disk failure"), { code: "ENOSPC" }),
      ),
    ).toBe(false);
  });

  test("known driver disconnections and timeout names do not require an error code", () => {
    for (const message of [
      "Connection terminated",
      "Connection terminated unexpectedly",
      "timeout expired",
      "select 1 - Connection terminated unexpectedly",
      "ROLLBACK - Client has encountered a connection error and is not queryable",
      "select 1 - Client was closed and is not queryable",
      "Knex: Timeout acquiring a connection. The pool is probably full. Are you missing a .transacting(trx) call?",
    ])
      expect(transientError(new Error(message))).toBe(true);
    for (const name of [
      "TimeoutError",
      "KnexTimeoutError",
      "RequestTimeout",
      "RequestTimeoutException",
    ])
      expect(
        transientError(Object.assign(new Error("timeout"), { name })),
      ).toBe(true);
    expect(transientError(new Error("unexpected timeout in domain rule"))).toBe(
      false,
    );
  });

  test("AWS throttling, server responses and retry traits differ from permanent SDK errors", () => {
    expect(
      transientError(
        new RequestThrottled({
          message: "throttled",
          $metadata: { httpStatusCode: 400 },
        }),
      ),
    ).toBe(true);
    for (const httpStatusCode of [429, 500, 502, 503, 504, 599])
      expect(
        transientError(
          new SQSServiceException({
            name: "ServiceFailure",
            $fault: "server",
            $metadata: { httpStatusCode },
          }),
        ),
      ).toBe(true);
    expect(
      transientError(
        Object.assign(
          new SQSServiceException({
            name: "RetryableFailure",
            $fault: "server",
            $metadata: { httpStatusCode: 400 },
          }),
          { $retryable: {} },
        ),
      ),
    ).toBe(true);
    expect(
      transientError(
        new QueueDoesNotExist({
          message: "missing queue",
          $metadata: { httpStatusCode: 400 },
        }),
      ),
    ).toBe(false);
    for (const httpStatusCode of [400, 401, 403, 404])
      expect(
        transientError(
          new SQSServiceException({
            name: "PermanentFailure",
            $fault: "client",
            $metadata: { httpStatusCode },
          }),
        ),
      ).toBe(false);
  });
});
