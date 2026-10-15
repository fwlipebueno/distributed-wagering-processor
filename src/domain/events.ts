import { randomUUID } from "node:crypto";
import type { FailureCode } from "./errors.js";
import type { MoneyProps } from "./money.js";
import type { WagerTransaction } from "./transaction.js";
import type { LedgerDirection, Wallet, WalletLedgerEntry } from "./wallet.js";

export interface EventContext {
  correlationId: string;
  causationId?: string;
}

export interface IntegrationEventProps<T> extends EventContext {
  eventId: string;
  aggregateId: string;
  occurredAt: Date;
  data: T;
}

export abstract class IntegrationEvent<T> {
  abstract readonly eventType: string;
  abstract readonly version: number;
  readonly eventId: string;
  readonly aggregateId: string;
  readonly correlationId: string;
  readonly causationId?: string;
  private readonly _occurredAt: Date;
  readonly data: Readonly<T>;

  protected constructor(props: IntegrationEventProps<T>) {
    this.eventId = props.eventId;
    this.aggregateId = props.aggregateId;
    this.correlationId = props.correlationId;
    this.causationId = props.causationId;
    this._occurredAt = new Date(props.occurredAt);
    this.data = structuredClone(props.data);
  }

  get occurredAt(): Date {
    return new Date(this._occurredAt);
  }

  toJSON() {
    return {
      eventId: this.eventId,
      eventType: this.eventType,
      aggregateId: this.aggregateId,
      correlationId: this.correlationId,
      ...(this.causationId ? { causationId: this.causationId } : {}),
      occurredAt: this.occurredAt.toISOString(),
      version: this.version,
      data: structuredClone(this.data),
    };
  }
}

interface TransactionEventData {
  transactionId: string;
  walletId: string;
  providerId: string;
  externalTransactionId: string;
  kind: string;
  status: string;
  money: MoneyProps;
  failureCode?: FailureCode;
  referenceExternalTransactionId?: string;
}

function transactionProps(
  t: WagerTransaction,
  ctx: EventContext,
): IntegrationEventProps<TransactionEventData> {
  return {
    ...ctx,
    eventId: randomUUID(),
    aggregateId: t.walletId,
    occurredAt: new Date(),
    data: {
      transactionId: t.id,
      walletId: t.walletId,
      providerId: t.providerId,
      externalTransactionId: t.externalTransactionId,
      kind: t.kind,
      status: t.status,
      money: t.money.toJSON(),
      ...(t.failureCode ? { failureCode: t.failureCode } : {}),
      ...(t.referenceExternalTransactionId
        ? { referenceExternalTransactionId: t.referenceExternalTransactionId }
        : {}),
    },
  };
}

export class WagerTransactionProcessed extends IntegrationEvent<TransactionEventData> {
  readonly eventType = "WagerTransactionProcessed";
  readonly version = 1;
  private constructor(props: IntegrationEventProps<TransactionEventData>) {
    super(props);
  }
  static from(t: WagerTransaction, ctx: EventContext) {
    return new WagerTransactionProcessed(transactionProps(t, ctx));
  }
}

export class WagerTransactionRejected extends IntegrationEvent<TransactionEventData> {
  readonly eventType = "WagerTransactionRejected";
  readonly version = 1;
  private constructor(props: IntegrationEventProps<TransactionEventData>) {
    super(props);
  }
  static from(t: WagerTransaction, ctx: EventContext) {
    return new WagerTransactionRejected(transactionProps(t, ctx));
  }
}

export class WagerTransactionPendingReference extends IntegrationEvent<TransactionEventData> {
  readonly eventType = "WagerTransactionPendingReference";
  readonly version = 1;
  private constructor(props: IntegrationEventProps<TransactionEventData>) {
    super(props);
  }
  static from(t: WagerTransaction, ctx: EventContext) {
    return new WagerTransactionPendingReference(transactionProps(t, ctx));
  }
}

export class WagerTransactionFailed extends IntegrationEvent<TransactionEventData> {
  readonly eventType = "WagerTransactionFailed";
  readonly version = 1;
  private constructor(props: IntegrationEventProps<TransactionEventData>) {
    super(props);
  }
  static from(t: WagerTransaction, ctx: EventContext) {
    return new WagerTransactionFailed(transactionProps(t, ctx));
  }
}

export interface WalletBalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
}

export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = "WalletBalanceChanged";
  readonly version = 1;
  private constructor(props: IntegrationEventProps<WalletBalanceChangedData>) {
    super(props);
  }
  static from(wallet: Wallet, entry: WalletLedgerEntry, ctx: EventContext) {
    return new WalletBalanceChanged({
      ...ctx,
      eventId: randomUUID(),
      aggregateId: wallet.id,
      occurredAt: entry.createdAt,
      data: {
        walletId: wallet.id,
        transactionId: entry.transactionId,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        walletVersion: wallet.version,
      },
    });
  }
}
