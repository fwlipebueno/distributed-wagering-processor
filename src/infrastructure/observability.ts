import { Counter, Gauge, Histogram, Registry } from "prom-client";
import type { EventContext } from "../domain/events.js";

export interface LogContext extends Partial<EventContext> {
  messageId?: string;
  transactionId?: string;
  walletId?: string;
  providerId?: string;
}

export function log(
  level: "info" | "warn" | "error",
  event: string,
  context: LogContext = {},
  code?: string,
): void {
  console.log(
    JSON.stringify({
      time: new Date().toISOString(),
      level,
      event,
      pid: process.pid,
      correlationId: context.correlationId ?? null,
      messageId: context.messageId ?? null,
      transactionId: context.transactionId ?? null,
      walletId: context.walletId ?? null,
      providerId: context.providerId ?? null,
      ...(code ? { code } : {}),
    }),
  );
}

export class Metrics {
  readonly registry = new Registry();
  readonly transactions = new Counter({
    name: "wager_transactions_total",
    help: "Committed status transitions",
    labelNames: ["status"],
    registers: [this.registry],
  });
  readonly duplicates = new Counter({
    name: "wager_duplicates_total",
    help: "Persistent replay detections",
    labelNames: ["source"],
    registers: [this.registry],
  });
  readonly retries = new Counter({
    name: "wager_retries_total",
    help: "Retry attempts",
    labelNames: ["source"],
    registers: [this.registry],
  });
  readonly dlq = new Counter({
    name: "wager_dlq_total",
    help: "Messages forwarded to DLQ",
    registers: [this.registry],
  });
  readonly dlqDepth = new Gauge({
    name: "wager_dlq_messages",
    help: "Visible and in-flight DLQ messages, including broker redrives",
    registers: [this.registry],
  });
  readonly lockConflicts = new Counter({
    name: "wager_lock_conflicts_total",
    help: "Deadlocks, serialization conflicts and lock timeouts",
    registers: [this.registry],
  });
  readonly outboxLag = new Gauge({
    name: "wager_outbox_lag_seconds",
    help: "Age of oldest unpublished event",
    registers: [this.registry],
  });
  readonly latency = new Histogram({
    name: "wager_processing_seconds",
    help: "Use case latency including SQL commit",
    buckets: [0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 15],
    registers: [this.registry],
  });
  readonly inconsistencies = new Counter({
    name: "wager_reconciliation_mismatches_total",
    help: "Observed balance/ledger mismatches",
    registers: [this.registry],
  });
}
