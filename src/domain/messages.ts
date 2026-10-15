import { DomainError } from "./errors.js";
import type { IntegrationEvent } from "./events.js";

export class InboxMessage {
  private constructor(
    public readonly messageId: string,
    public readonly consumerName: string,
    public readonly payloadHash: string,
    private readonly _receivedAt: Date,
    private _processedAt?: Date,
  ) {
    this._receivedAt = new Date(_receivedAt);
    this._processedAt = _processedAt && new Date(_processedAt);
  }

  static receive(props: {
    messageId: string;
    consumerName: string;
    payloadHash: string;
  }): InboxMessage {
    return new InboxMessage(
      props.messageId,
      props.consumerName,
      props.payloadHash,
      new Date(),
    );
  }

  static rehydrate(state: {
    messageId: string;
    consumerName: string;
    payloadHash: string;
    receivedAt: Date;
    processedAt?: Date;
  }): InboxMessage {
    return new InboxMessage(
      state.messageId,
      state.consumerName,
      state.payloadHash,
      state.receivedAt,
      state.processedAt,
    );
  }

  get receivedAt() {
    return new Date(this._receivedAt);
  }
  get processedAt() {
    return this._processedAt && new Date(this._processedAt);
  }
  isProcessed() {
    return this._processedAt !== undefined;
  }
  markProcessed(at: Date): void {
    if (this.isProcessed()) throw new DomainError("INBOX_ALREADY_PROCESSED");
    this._processedAt = new Date(at);
  }
}

export interface OutboxState {
  id: string;
  aggregateId: string;
  eventType: string;
  payload: Readonly<Record<string, unknown>>;
  occurredAt: Date;
  attempts: number;
  nextAttemptAt?: Date;
  publishedAt?: Date;
}

export class OutboxMessage {
  private constructor(
    private readonly state: Readonly<
      Omit<OutboxState, "attempts" | "nextAttemptAt" | "publishedAt">
    >,
    private _attempts: number,
    private _nextAttemptAt?: Date,
    private _publishedAt?: Date,
  ) {}

  static enqueue(event: IntegrationEvent<unknown>): OutboxMessage {
    return OutboxMessage.rehydrate({
      id: event.eventId,
      aggregateId: event.aggregateId,
      eventType: event.eventType,
      payload: event.toJSON(),
      occurredAt: event.occurredAt,
      attempts: 0,
    });
  }

  static rehydrate(state: OutboxState): OutboxMessage {
    return new OutboxMessage(
      {
        ...state,
        payload: structuredClone(state.payload),
        occurredAt: new Date(state.occurredAt),
      },
      state.attempts,
      state.nextAttemptAt && new Date(state.nextAttemptAt),
      state.publishedAt && new Date(state.publishedAt),
    );
  }

  get id() {
    return this.state.id;
  }
  get aggregateId() {
    return this.state.aggregateId;
  }
  get eventType() {
    return this.state.eventType;
  }
  get payload() {
    return structuredClone(this.state.payload);
  }
  get occurredAt() {
    return new Date(this.state.occurredAt);
  }
  get attempts() {
    return this._attempts;
  }
  get nextAttemptAt() {
    return this._nextAttemptAt && new Date(this._nextAttemptAt);
  }
  get publishedAt() {
    return this._publishedAt && new Date(this._publishedAt);
  }
  isPending() {
    return this._publishedAt === undefined;
  }
  isDue(now: Date) {
    return (
      this.isPending() && (!this._nextAttemptAt || this._nextAttemptAt <= now)
    );
  }

  markPublished(at: Date): void {
    if (!this.isPending()) throw new DomainError("OUTBOX_ALREADY_PUBLISHED");
    this._publishedAt = new Date(at);
  }

  scheduleRetry(now: Date): void {
    if (!this.isPending()) throw new DomainError("OUTBOX_ALREADY_PUBLISHED");
    this._attempts += 1;
    this._nextAttemptAt = new Date(
      now.getTime() +
        Math.min(60_000, 1000 * 2 ** Math.min(this._attempts - 1, 6)),
    );
  }
}
