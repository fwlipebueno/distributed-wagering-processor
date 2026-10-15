export type FailureCode =
  | "INSUFFICIENT_FUNDS"
  | "REVERSAL_INSUFFICIENT_FUNDS"
  | "CURRENCY_MISMATCH"
  | "PLAYER_MISMATCH"
  | "REFERENCE_CONTEXT_MISMATCH"
  | "REFERENCE_KIND_INVALID"
  | "REFERENCE_NOT_PROCESSED"
  | "REFERENCE_AMOUNT_MISMATCH"
  | "REFERENCE_ALREADY_REVERSED"
  | "REFERENCE_NOT_FOUND"
  | "BALANCE_LIMIT_EXCEEDED"
  | "PERMANENT_INFRASTRUCTURE_ERROR";

export class DomainError extends Error {
  constructor(public readonly code: string) {
    super(code);
    this.name = new.target.name;
  }
}

export class InvalidTransactionStateError extends DomainError {
  constructor() {
    super("INVALID_TRANSACTION_STATE");
  }
}

export class RequestError extends Error {
  constructor(
    public readonly code: string,
    public readonly httpStatus: number,
  ) {
    super(code);
  }
}
