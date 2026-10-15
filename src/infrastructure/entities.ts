import { BigIntType, EntitySchema, Type } from "@mikro-orm/core";
import type { FailureCode } from "../domain/errors.js";
import type { MoneyProps } from "../domain/money.js";
import type { WagerKind, WagerStatus } from "../domain/transaction.js";
import type { LedgerDirection } from "../domain/wallet.js";

class ExactAmountType extends Type<string, string> {
  override convertToDatabaseValue(value: string): string {
    if (typeof value !== "string")
      throw new TypeError("Money must remain a decimal string");
    return value;
  }
  override convertToJSValue(value: string): string {
    if (typeof value !== "string")
      throw new TypeError("Unexpected numeric conversion");
    return value;
  }
  override getColumnType(): string {
    return "numeric(20,2)";
  }
  override compareAsType(): string {
    return "string";
  }
}

export interface TransactionResult {
  transactionId: string;
  status: WagerStatus;
  balance: MoneyProps;
  failureCode?: FailureCode;
  idempotentReplay: boolean;
}

export class WalletRow {
  declare id: string;
  declare playerId: string;
  declare currency: string;
  declare balance: string;
  declare version: number;
  declare createdAt: Date;
  declare updatedAt: Date;
}

export class WagerRow {
  declare id: string;
  declare providerId: string;
  declare externalTransactionId: string;
  declare idempotencyKey: string;
  declare payloadHash: string;
  declare walletId: string;
  declare playerId: string;
  declare roundId: string;
  declare gameId: string;
  declare kind: WagerKind;
  declare amount: string;
  declare currency: string;
  declare referenceExternalTransactionId?: string;
  declare referenceTransactionId?: string;
  declare status: WagerStatus;
  declare failureCode?: FailureCode;
  declare createdAt: Date;
  declare processedAt?: Date;
  declare result?: TransactionResult;
  declare attempts: number;
  declare nextAttemptAt?: Date;
  declare correlationId: string;
  declare causationId?: string;
}

export class LedgerRow {
  declare id: string;
  declare position: string;
  declare walletId: string;
  declare transactionId: string;
  declare direction: LedgerDirection;
  declare amount: string;
  declare currency: string;
  declare balanceBefore: string;
  declare balanceAfter: string;
  declare createdAt: Date;
}

export class InboxRow {
  declare consumerName: string;
  declare messageId: string;
  declare payloadHash: string;
  declare receivedAt: Date;
  declare processedAt?: Date;
  declare transactionId?: string;
}

export class OutboxRow {
  declare id: string;
  declare aggregateId: string;
  declare eventType: string;
  declare payload: Record<string, unknown>;
  declare occurredAt: Date;
  declare attempts: number;
  declare nextAttemptAt?: Date;
  declare publishedAt?: Date;
}

const uuid = { type: "uuid" } as const;
const text = { type: "string" } as const;
const date = { type: "Date", columnType: "timestamptz" } as const;
const amount = { type: ExactAmountType } as const;

export const entities = [
  new EntitySchema<WalletRow>({
    class: WalletRow,
    tableName: "wallets",
    properties: {
      id: { ...uuid, primary: true },
      playerId: uuid,
      currency: text,
      balance: amount,
      version: { type: "integer" },
      createdAt: date,
      updatedAt: date,
    },
  }),
  new EntitySchema<WagerRow>({
    class: WagerRow,
    tableName: "wagers",
    properties: {
      id: { ...uuid, primary: true },
      providerId: text,
      externalTransactionId: text,
      idempotencyKey: text,
      payloadHash: text,
      walletId: uuid,
      playerId: uuid,
      roundId: text,
      gameId: text,
      kind: text,
      amount,
      currency: text,
      referenceExternalTransactionId: { ...text, nullable: true },
      referenceTransactionId: { ...uuid, nullable: true },
      status: text,
      failureCode: { ...text, nullable: true },
      createdAt: date,
      processedAt: { ...date, nullable: true },
      result: { type: "json", nullable: true },
      attempts: { type: "integer" },
      nextAttemptAt: { ...date, nullable: true },
      correlationId: text,
      causationId: { ...text, nullable: true },
    },
  }),
  new EntitySchema<LedgerRow>({
    class: LedgerRow,
    tableName: "ledger",
    properties: {
      id: { ...uuid, primary: true },
      position: {
        type: new BigIntType("string"),
        columnType: "bigint",
        autoincrement: true,
      },
      walletId: uuid,
      transactionId: uuid,
      direction: text,
      amount,
      currency: text,
      balanceBefore: amount,
      balanceAfter: amount,
      createdAt: date,
    },
  }),
  new EntitySchema<InboxRow>({
    class: InboxRow,
    tableName: "inbox",
    properties: {
      consumerName: { ...text, primary: true },
      messageId: { ...text, primary: true },
      payloadHash: text,
      receivedAt: date,
      processedAt: { ...date, nullable: true },
      transactionId: { ...uuid, nullable: true },
    },
  }),
  new EntitySchema<OutboxRow>({
    class: OutboxRow,
    tableName: "outbox",
    properties: {
      id: { ...uuid, primary: true },
      aggregateId: uuid,
      eventType: text,
      payload: { type: "json" },
      occurredAt: date,
      attempts: { type: "integer" },
      nextAttemptAt: { ...date, nullable: true },
      publishedAt: { ...date, nullable: true },
    },
  }),
];
