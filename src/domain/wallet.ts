import { randomUUID } from "node:crypto";
import { DomainError } from "./errors.js";
import type { Money } from "./money.js";

export enum LedgerDirection {
  Debit = "DEBIT",
  Credit = "CREDIT",
}

export interface LedgerState {
  id: string;
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  createdAt: Date;
}

export class WalletLedgerEntry {
  private readonly timestamp: number;
  private constructor(
    public readonly id: string,
    public readonly walletId: string,
    public readonly transactionId: string,
    public readonly direction: LedgerDirection,
    public readonly money: Money,
    public readonly balanceBefore: Money,
    public readonly balanceAfter: Money,
    createdAt: Date,
  ) {
    this.timestamp = createdAt.getTime();
    Object.freeze(this);
  }

  static rehydrate(state: LedgerState): WalletLedgerEntry {
    return new WalletLedgerEntry(
      state.id,
      state.walletId,
      state.transactionId,
      state.direction,
      state.money,
      state.balanceBefore,
      state.balanceAfter,
      state.createdAt,
    );
  }

  static create(state: LedgerState): WalletLedgerEntry {
    const entry = WalletLedgerEntry.rehydrate(state);
    if (
      !state.money.isPositive() ||
      state.balanceBefore.isNegative() ||
      state.balanceAfter.isNegative() ||
      !entry.isBalanced()
    ) {
      throw new DomainError("UNBALANCED_LEDGER");
    }
    return entry;
  }

  get createdAt(): Date {
    return new Date(this.timestamp);
  }

  isBalanced(): boolean {
    const expected =
      this.direction === LedgerDirection.Credit
        ? this.balanceBefore.add(this.money)
        : this.balanceBefore.subtract(this.money);
    return expected.equals(this.balanceAfter);
  }
}

export interface WalletState {
  id: string;
  playerId: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    private _balance: Money,
    private _version: number,
    private readonly _createdAt: Date,
    private _updatedAt: Date,
  ) {
    this._createdAt = new Date(_createdAt);
    this._updatedAt = new Date(_updatedAt);
  }

  static open(props: {
    id: string;
    playerId: string;
    initialBalance: Money;
  }): Wallet {
    if (props.initialBalance.isNegative()) {
      throw new DomainError("NEGATIVE_BALANCE");
    }
    const now = new Date();
    return new Wallet(
      props.id,
      props.playerId,
      props.initialBalance,
      1,
      now,
      now,
    );
  }

  static rehydrate(state: WalletState): Wallet {
    return new Wallet(
      state.id,
      state.playerId,
      state.balance,
      state.version,
      state.createdAt,
      state.updatedAt,
    );
  }

  get balance(): Money {
    return this._balance;
  }

  get currency(): string {
    return this._balance.currency;
  }

  get version(): number {
    return this._version;
  }

  get createdAt(): Date {
    return new Date(this._createdAt);
  }

  get updatedAt(): Date {
    return new Date(this._updatedAt);
  }

  debit(
    transactionId: string,
    money: Money,
    reversal = false,
  ): WalletLedgerEntry {
    if (this.balance.isLessThan(money)) {
      throw new DomainError(
        reversal ? "REVERSAL_INSUFFICIENT_FUNDS" : "INSUFFICIENT_FUNDS",
      );
    }
    return this.move(transactionId, money, LedgerDirection.Debit);
  }

  credit(transactionId: string, money: Money): WalletLedgerEntry {
    return this.move(transactionId, money, LedgerDirection.Credit);
  }

  private move(
    transactionId: string,
    money: Money,
    direction: LedgerDirection,
  ): WalletLedgerEntry {
    const after =
      direction === LedgerDirection.Credit
        ? this.balance.add(money)
        : this.balance.subtract(money);
    if (after.exceedsStorageLimit()) {
      throw new DomainError("BALANCE_LIMIT_EXCEEDED");
    }
    const entry = WalletLedgerEntry.create({
      id: randomUUID(),
      walletId: this.id,
      transactionId,
      direction,
      money,
      balanceBefore: this.balance,
      balanceAfter: after,
      createdAt: new Date(),
    });
    this._balance = after;
    this._version += 1;
    this._updatedAt = entry.createdAt;
    return entry;
  }
}
