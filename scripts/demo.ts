import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import type { TransactionResult } from "../src/infrastructure/entities.js";

const base = process.env.API_URL ?? "http://localhost:3000";
async function call<T>(path: string, body?: unknown, key?: string) {
  const response = await fetch(`${base}${path}`, {
    method: body === undefined ? "GET" : "POST",
    headers: {
      "Content-Type": "application/json",
      ...(key ? { "Idempotency-Key": key } : {}),
    },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    signal: AbortSignal.timeout(20000),
  });
  const result = (await response.json()) as T;
  console.log(JSON.stringify({ path, httpStatus: response.status, result }));
  return { status: response.status, result };
}
const opened = await call<{ id: string; playerId: string }>("/wallets", {
  playerId: randomUUID(),
  initialBalance: { amount: "100.00", currency: "BRL" },
});
assert.equal(opened.status, 201);
assert.equal(typeof opened.result.id, "string");
const bets = Array.from({ length: 2 }, () => ({
  providerId: "provider-a",
  externalTransactionId: randomUUID(),
  walletId: opened.result.id,
  playerId: opened.result.playerId,
  roundId: "demo",
  gameId: "demo",
  kind: "BET",
  money: { amount: "80.00", currency: "BRL" },
}));
const responses = await Promise.all(
  bets.map((body) =>
    call<TransactionResult>(
      "/wagering/transactions",
      body,
      `${body.providerId}:${body.externalTransactionId}`,
    ),
  ),
);
assert.deepEqual(responses.map((r) => r.status).sort(), [200, 422]);
assert.deepEqual(responses.map((r) => r.result.status).sort(), [
  "PROCESSED",
  "REJECTED",
]);
assert(responses.every((r) => r.result.balance.amount === "20.00"));
assert.equal(
  responses.find((r) => r.status === 422)?.result.failureCode,
  "INSUFFICIENT_FUNDS",
);
const first = bets[0];
assert(first && responses[0]);
const replay = await call<TransactionResult>(
  "/wagering/transactions",
  first,
  `${first.providerId}:${first.externalTransactionId}`,
);
assert.equal(replay.status, responses[0].status);
assert.deepEqual(replay.result, {
  ...responses[0].result,
  idempotentReplay: true,
});
const ledger = await call<{
  entries: { direction: string }[];
  nextCursor: string | null;
}>(`/wallets/${opened.result.id}/ledger`);
assert.equal(ledger.status, 200);
assert.deepEqual(
  ledger.result.entries.map((entry) => entry.direction),
  ["CREDIT", "DEBIT"],
);
assert.equal(ledger.result.nextCursor, null);
const reconciliation = await call<{
  storedBalance: { amount: string; currency: string };
  calculatedBalance: { amount: string; currency: string };
  difference: { amount: string; currency: string };
  consistent: boolean;
  checkedEntries: number;
}>(`/wallets/${opened.result.id}/reconciliation`, {});
assert.equal(reconciliation.status, 200);
assert.equal(reconciliation.result.consistent, true);
assert.deepEqual(reconciliation.result.storedBalance, {
  amount: "20.00",
  currency: "BRL",
});
assert.deepEqual(
  reconciliation.result.calculatedBalance,
  reconciliation.result.storedBalance,
);
assert.deepEqual(reconciliation.result.difference, {
  amount: "0.00",
  currency: "BRL",
});
assert.equal(reconciliation.result.checkedEntries, 2);
console.log(JSON.stringify({ verified: true, walletId: opened.result.id }));
