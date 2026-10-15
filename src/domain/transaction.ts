import {
  DomainError,
  type FailureCode,
  InvalidTransactionStateError,
} from "./errors.js";
import type { Money } from "./money.js";
import { LedgerDirection, type Wallet } from "./wallet.js";

export enum WagerKind {
  Opening = "OPENING",
  Bet = "BET",
  Win = "WIN",
  Loss = "LOSS",
  Refund = "REFUND",
  Rollback = "ROLLBACK",
}

export enum WagerStatus {
  Pending = "PENDING",
  PendingReference = "PENDING_REFERENCE",
  Processed = "PROCESSED",
  Rejected = "REJECTED",
  Failed = "FAILED",
}

export interface TransactionProps {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerKind;
  money: Money;
  referenceExternalTransactionId?: string;
  createdAt: Date;
}

export interface TransactionState extends TransactionProps {
  status: WagerStatus;
  referenceTransactionId?: string;
  failureCode?: FailureCode;
  processedAt?: Date;
}

export class WagerTransaction {
  private constructor(
    private readonly props: Readonly<TransactionProps>,
    private _status: WagerStatus,
    private _referenceTransactionId?: string,
    private _failureCode?: FailureCode,
    private _processedAt?: Date,
  ) {
    this.props = Object.freeze({
      ...props,
      createdAt: new Date(props.createdAt),
    });
    this._processedAt = _processedAt && new Date(_processedAt);
  }

  static create(props: TransactionProps): WagerTransaction {
    if (
      [WagerKind.Refund, WagerKind.Rollback].includes(props.kind) &&
      !props.referenceExternalTransactionId
    ) {
      throw new DomainError("REFERENCE_REQUIRED");
    }
    if (props.kind !== WagerKind.Loss && !props.money.isPositive()) {
      throw new DomainError("AMOUNT_MUST_BE_POSITIVE");
    }
    if (props.kind === WagerKind.Loss && props.money.isNegative()) {
      throw new DomainError("INVALID_AMOUNT");
    }
    if (
      [WagerKind.Bet, WagerKind.Loss, WagerKind.Opening].includes(props.kind) &&
      props.referenceExternalTransactionId
    ) {
      throw new DomainError("REFERENCE_NOT_ALLOWED");
    }
    return new WagerTransaction(props, WagerStatus.Pending);
  }

  static rehydrate(state: TransactionState): WagerTransaction {
    return new WagerTransaction(
      state,
      state.status,
      state.referenceTransactionId,
      state.failureCode,
      state.processedAt,
    );
  }

  get id() {
    return this.props.id;
  }
  get providerId() {
    return this.props.providerId;
  }
  get externalTransactionId() {
    return this.props.externalTransactionId;
  }
  get idempotencyKey() {
    return this.props.idempotencyKey;
  }
  get payloadHash() {
    return this.props.payloadHash;
  }
  get walletId() {
    return this.props.walletId;
  }
  get playerId() {
    return this.props.playerId;
  }
  get roundId() {
    return this.props.roundId;
  }
  get gameId() {
    return this.props.gameId;
  }
  get kind() {
    return this.props.kind;
  }
  get money() {
    return this.props.money;
  }
  get referenceExternalTransactionId() {
    return this.props.referenceExternalTransactionId;
  }
  get createdAt() {
    return new Date(this.props.createdAt);
  }
  get status() {
    return this._status;
  }
  get referenceTransactionId() {
    return this._referenceTransactionId;
  }
  get failureCode() {
    return this._failureCode;
  }
  get processedAt() {
    return this._processedAt && new Date(this._processedAt);
  }

  markProcessed(referenceId: string | undefined, at: Date): void {
    this.assertMutable();
    this._status = WagerStatus.Processed;
    this._referenceTransactionId = referenceId;
    this._processedAt = new Date(at);
  }

  markPendingReference(): void {
    this.assertMutable();
    this._status = WagerStatus.PendingReference;
  }

  reject(code: FailureCode): void {
    this.assertMutable();
    this._status = WagerStatus.Rejected;
    this._failureCode = code;
    this._processedAt = new Date();
  }

  fail(code: FailureCode): void {
    this.assertMutable();
    this._status = WagerStatus.Failed;
    this._failureCode = code;
    this._processedAt = new Date();
  }

  isTerminal(): boolean {
    return [
      WagerStatus.Processed,
      WagerStatus.Rejected,
      WagerStatus.Failed,
    ].includes(this.status);
  }

  affectsBalance(): boolean {
    return this.kind !== WagerKind.Loss;
  }
  requiresReference(): boolean {
    return [WagerKind.Refund, WagerKind.Rollback].includes(this.kind);
  }

  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    if (this.kind === WagerKind.Loss)
      throw new DomainError("LOSS_HAS_NO_LEDGER");
    if (this.kind === WagerKind.Rollback) {
      if (!reference) throw new DomainError("REFERENCE_REQUIRED");
      return reference.ledgerDirectionFor() === LedgerDirection.Debit
        ? LedgerDirection.Credit
        : LedgerDirection.Debit;
    }
    return this.kind === WagerKind.Bet
      ? LedgerDirection.Debit
      : LedgerDirection.Credit;
  }

  private assertMutable(): void {
    if (this.isTerminal()) throw new InvalidTransactionStateError();
  }
}

export function validateWager(
  transaction: WagerTransaction,
  wallet: Wallet,
  reference: WagerTransaction | undefined,
  alreadyReversed: boolean,
): "READY" | "PENDING_REFERENCE" | FailureCode {
  if (transaction.playerId !== wallet.playerId) return "PLAYER_MISMATCH";
  if (transaction.money.currency !== wallet.currency)
    return "CURRENCY_MISMATCH";
  if (!transaction.referenceExternalTransactionId) return "READY";
  if (!reference) return "PENDING_REFERENCE";
  if (
    reference.providerId !== transaction.providerId ||
    reference.walletId !== transaction.walletId ||
    reference.playerId !== transaction.playerId ||
    reference.roundId !== transaction.roundId ||
    reference.money.currency !== transaction.money.currency
  )
    return "REFERENCE_CONTEXT_MISMATCH";
  if (!reference.isTerminal()) return "PENDING_REFERENCE";
  if (reference.status !== WagerStatus.Processed)
    return "REFERENCE_NOT_PROCESSED";
  const allowed =
    transaction.kind === WagerKind.Rollback
      ? [WagerKind.Bet, WagerKind.Win, WagerKind.Refund]
      : [WagerKind.Bet];
  if (!allowed.includes(reference.kind)) return "REFERENCE_KIND_INVALID";
  if (
    transaction.requiresReference() &&
    !transaction.money.equals(reference.money)
  ) {
    return "REFERENCE_AMOUNT_MISMATCH";
  }
  if (transaction.requiresReference() && alreadyReversed)
    return "REFERENCE_ALREADY_REVERSED";
  return "READY";
}
