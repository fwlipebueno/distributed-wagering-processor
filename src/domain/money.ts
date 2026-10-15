import { DomainError } from "./errors.js";

export interface MoneyProps {
  amount: string;
  currency: string;
}

export class Money {
  private constructor(
    private readonly cents: bigint,
    public readonly currency: string,
  ) {
    Object.freeze(this);
  }

  static from(props: MoneyProps): Money {
    if (
      typeof props.amount !== "string" ||
      !/^(0|[1-9]\d{0,17})\.\d{2}$/.test(props.amount)
    ) {
      throw new DomainError("INVALID_AMOUNT");
    }
    if (!["BRL", "USD", "EUR"].includes(props.currency)) {
      throw new DomainError("INVALID_CURRENCY");
    }
    return new Money(BigInt(props.amount.replace(".", "")), props.currency);
  }

  static rehydrate(props: MoneyProps): Money {
    return new Money(BigInt(props.amount.replace(".", "")), props.currency);
  }

  static zero(currency: string): Money {
    return Money.from({ amount: "0.00", currency });
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.cents + other.cents, this.currency);
  }

  subtract(other: Money): Money {
    return this.add(other.negate());
  }

  negate(): Money {
    return new Money(-this.cents, this.currency);
  }

  isZero(): boolean {
    return this.cents === 0n;
  }

  isPositive(): boolean {
    return this.cents > 0n;
  }

  isNegative(): boolean {
    return this.cents < 0n;
  }

  exceedsStorageLimit(): boolean {
    return this.cents > 99999999999999999999n;
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.cents < other.cents;
  }

  equals(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.cents === other.cents;
  }

  toString(): string {
    const absolute = this.cents < 0n ? -this.cents : this.cents;
    return `${this.isNegative() ? "-" : ""}${absolute / 100n}.${(absolute % 100n).toString().padStart(2, "0")}`;
  }

  toJSON(): MoneyProps {
    return { amount: this.toString(), currency: this.currency };
  }

  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) {
      throw new DomainError("CURRENCY_MISMATCH");
    }
  }
}
