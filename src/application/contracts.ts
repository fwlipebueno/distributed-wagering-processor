import { createHash } from "node:crypto";
import { z } from "zod";
import { DomainError, RequestError } from "../domain/errors.js";
import { Money } from "../domain/money.js";
import { WagerKind, WagerTransaction } from "../domain/transaction.js";

const identifier = z
  .string()
  .min(1)
  .max(128)
  .regex(/^[A-Za-z0-9][A-Za-z0-9_.:-]*$/);
const moneySchema = z.strictObject({
  amount: z.string(),
  currency: z.string(),
});
export const walletSchema = z.strictObject({
  playerId: z.uuid(),
  initialBalance: moneySchema,
});
const wagerSchema = z.strictObject({
  providerId: identifier.refine((v) => v !== "internal", "Reserved provider"),
  externalTransactionId: identifier,
  playerId: z.uuid(),
  walletId: z.uuid(),
  roundId: identifier,
  gameId: identifier,
  kind: z.enum([
    WagerKind.Bet,
    WagerKind.Win,
    WagerKind.Loss,
    WagerKind.Refund,
    WagerKind.Rollback,
  ]),
  money: moneySchema,
  referenceExternalTransactionId: identifier.optional(),
});
export type WagerRequest = z.infer<typeof wagerSchema>;

export function canonicalJSON(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJSON).join(",")}]`;
  if (value && typeof value === "object") {
    const entries = Object.entries(value)
      .filter(([, v]) => v !== undefined)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${canonicalJSON(v)}`).join(",")}}`;
  }
  const serialized = JSON.stringify(value);
  if (serialized === undefined) throw new RequestError("INVALID_PAYLOAD", 400);
  return serialized;
}

export function payloadHash(value: unknown): string {
  return createHash("sha256").update(canonicalJSON(value)).digest("hex");
}

export function parseWager(
  input: unknown,
  key: unknown,
): { request: WagerRequest; key: string; hash: string } {
  if (
    typeof key !== "string" ||
    key.length === 0 ||
    key.length > 256 ||
    !/^[\x21-\x7E]+$/.test(key)
  ) {
    throw new RequestError("INVALID_IDEMPOTENCY_KEY", 400);
  }
  const parsed = wagerSchema.safeParse(input);
  if (!parsed.success) throw new RequestError("INVALID_PAYLOAD", 400);
  const request = parsed.data;
  try {
    const money = Money.from(request.money);
    WagerTransaction.create({
      ...request,
      id: "validation",
      idempotencyKey: key,
      payloadHash: "",
      money,
      createdAt: new Date(),
    });
  } catch (error) {
    if (error instanceof DomainError) throw new RequestError(error.code, 400);
    throw error;
  }
  return { request, key, hash: payloadHash(request) };
}

const messageSchema = z.strictObject({
  messageId: identifier,
  type: z.literal("WagerTransactionRequested"),
  occurredAt: z.iso.datetime(),
  data: wagerSchema.extend({ idempotencyKey: z.string() }),
});

export function parseMessage(body: string) {
  let input: unknown;
  try {
    input = JSON.parse(body);
  } catch {
    throw new RequestError("INVALID_JSON", 400);
  }
  const parsed = messageSchema.safeParse(input);
  if (!parsed.success) throw new RequestError("INVALID_MESSAGE", 400);
  const { idempotencyKey, ...business } = parsed.data.data;
  const wager = parseWager(business, idempotencyKey);
  return {
    ...wager,
    messageId: parsed.data.messageId,
    inboxHash: payloadHash({ type: parsed.data.type, data: parsed.data.data }),
  };
}

export function assertReplay(
  stored: { idempotencyKey: string; payloadHash: string },
  key: string,
  hash: string,
): void {
  if (stored.idempotencyKey !== key || stored.payloadHash !== hash) {
    throw new RequestError("IDEMPOTENCY_CONFLICT", 409);
  }
}
