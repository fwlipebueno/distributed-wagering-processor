import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { availableParallelism, totalmem } from "node:os";
import type { TransactionResult } from "../src/infrastructure/entities.js";

const origins = (
  process.env.API_URLS ??
  "http://localhost:3000,http://localhost:3001,http://localhost:3002"
)
  .split(",")
  .map((url) => new URL(url.trim()).origin);
const requests = positiveInteger(process.env.LOAD_REQUESTS ?? "60");
const concurrency = positiveInteger(process.env.LOAD_CONCURRENCY ?? "3");
const histories = [0, 100, 500];
const warmup = 10;
const runId = randomUUID();

function positiveInteger(value: string) {
  assert(/^\d+$/.test(value));
  const parsed = Number(value);
  assert(Number.isSafeInteger(parsed) && parsed > 0 && parsed <= 10000);
  return parsed;
}

async function call<T>(
  origin: string,
  path: string,
  body?: unknown,
  key?: string,
) {
  const response = await fetch(`${origin}${path}`, {
    method: body === undefined ? "GET" : "POST",
    headers: {
      "Content-Type": "application/json",
      ...(key ? { "Idempotency-Key": key } : {}),
    },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    signal: AbortSignal.timeout(20000),
  });
  return { status: response.status, body: (await response.json()) as T };
}

async function metrics() {
  const values = await Promise.all(
    origins.map(async (origin) => {
      const response = await fetch(`${origin}/metrics`, {
        signal: AbortSignal.timeout(10000),
      });
      assert.equal(response.status, 200);
      const text = await response.text();
      const sample = (name: string) => {
        const value = Number(
          text.match(new RegExp(`^${name} ([^\\n]+)$`, "m"))?.[1],
        );
        assert(Number.isFinite(value), `Missing ${name}`);
        return value;
      };
      return {
        locks: sample("wager_lock_conflicts_total"),
        lag: sample("wager_outbox_lag_seconds"),
      };
    }),
  );
  return {
    locks: values.reduce((sum, value) => sum + value.locks, 0),
    lag: Math.max(...values.map((value) => value.lag)),
  };
}

function percentile(sorted: number[], quantile: number) {
  const value = sorted[Math.ceil(sorted.length * quantile) - 1];
  assert(value !== undefined);
  return Math.round(value * 100) / 100;
}

const stages = [];
for (const history of histories) {
  const origin = origins[0];
  assert(origin);
  const opened = await call<{ id: string; playerId: string }>(
    origin,
    "/wallets",
    {
      playerId: randomUUID(),
      initialBalance: { amount: "100.00", currency: "BRL" },
    },
  );
  assert.equal(opened.status, 201);
  const wager = (i: number) => {
    const target = origins[i % origins.length];
    assert(target);
    const externalTransactionId = randomUUID();
    return call<TransactionResult>(
      target,
      "/wagering/transactions",
      {
        providerId: "load-test",
        externalTransactionId,
        walletId: opened.body.id,
        playerId: opened.body.playerId,
        roundId: runId,
        gameId: "load",
        kind: "WIN",
        money: { amount: "0.01", currency: "BRL" },
      },
      `load-test:${externalTransactionId}`,
    );
  };
  for (let i = 0; i < history + warmup; i += 1) {
    const response = await wager(i);
    assert.equal(response.status, 200);
    assert.equal(response.body.status, "PROCESSED");
  }
  const before = await metrics();
  const lags = [before.lag];
  let sampling = true;
  const samples = (async () => {
    while (sampling) {
      await Bun.sleep(250);
      if (sampling) lags.push((await metrics()).lag);
    }
  })();
  let next = 0;
  let succeeded = 0;
  const errors: Record<string, number> = {};
  const latencies: number[] = [];
  const started = performance.now();
  let elapsed: number;
  try {
    await Promise.all(
      Array.from({ length: Math.min(concurrency, requests) }, async () => {
        for (;;) {
          const index = next++;
          if (index >= requests) return;
          const sent = performance.now();
          let error: string | undefined;
          try {
            const response = await wager(index);
            if (
              response.status === 200 &&
              response.body.status === "PROCESSED" &&
              !response.body.idempotentReplay
            )
              succeeded += 1;
            else
              error = `HTTP_${response.status}:${response.body.failureCode ?? response.body.status ?? "INVALID_RESPONSE"}`;
          } catch {
            error = "NETWORK_OR_RESPONSE_ERROR";
          }
          latencies.push(performance.now() - sent);
          if (error) errors[error] = (errors[error] ?? 0) + 1;
        }
      }),
    );
    elapsed = (performance.now() - started) / 1000;
  } finally {
    sampling = false;
    await samples;
  }
  const after = await metrics();
  lags.push(after.lag);
  const reconciled = await call<{
    consistent: boolean;
    checkedEntries: number;
    storedBalance: { amount: string };
    difference: { amount: string };
  }>(origin, `/wallets/${opened.body.id}/reconciliation`, {});
  const wallet = await call<{ version: number }>(
    origin,
    `/wallets/${opened.body.id}`,
  );
  const movements = history + warmup + succeeded;
  const cents = 10000n + BigInt(movements);
  const expected = `${cents / 100n}.${(cents % 100n).toString().padStart(2, "0")}`;
  const consistent =
    reconciled.status === 200 &&
    wallet.status === 200 &&
    reconciled.body.consistent &&
    reconciled.body.difference.amount === "0.00" &&
    reconciled.body.storedBalance.amount === expected &&
    reconciled.body.checkedEntries === movements + 1 &&
    wallet.body.version === movements + 1;
  latencies.sort((a, b) => a - b);
  stages.push({
    walletId: opened.body.id,
    ledgerEntriesAtStart: history + warmup + 1,
    requests,
    concurrency,
    seconds: elapsed,
    throughputRps: requests / elapsed,
    successfulRps: succeeded / elapsed,
    p50Ms: percentile(latencies, 0.5),
    p95Ms: percentile(latencies, 0.95),
    p99Ms: percentile(latencies, 0.99),
    errors,
    errorRate: (requests - succeeded) / requests,
    lockConflicts: after.locks - before.locks,
    outboxLagSeconds: {
      start: before.lag,
      peak: Math.max(...lags),
      end: after.lag,
    },
    consistent,
  });
  if (succeeded !== requests || !consistent) process.exitCode = 1;
}
const drainStarted = performance.now();
let final = await metrics();
while (final.lag > 0 && performance.now() - drainStarted < 30000) {
  await Bun.sleep(250);
  final = await metrics();
}
console.log(
  JSON.stringify(
    {
      runId,
      occurredAt: new Date().toISOString(),
      environment: {
        runtime: `Bun ${Bun.version}`,
        platform: process.platform,
        arch: process.arch,
        visibleCpus: availableParallelism(),
        visibleMemoryBytes: totalmem(),
        origins,
        description:
          process.env.LOAD_ENVIRONMENT ??
          "Compose: PostgreSQL 17.6, LocalStack 4.10.0, three APIs and two workers",
      },
      methodology: {
        workload:
          "One hot wallet per stage; unique WIN 0.01 across API processes; sequential prefill and warmup excluded from timing",
        warmup,
        percentile:
          "nearest rank; end-to-end HTTP including body parsing; no client retries",
        metricsSamplingMs: 250,
        outboxLag:
          "Maximum across API scrapes; backlog includes prefill events; drain wait bounded at 30s",
      },
      stages,
      recovery: {
        outboxLagSeconds: final.lag,
        waitSeconds: (performance.now() - drainStarted) / 1000,
      },
    },
    null,
    2,
  ),
);
