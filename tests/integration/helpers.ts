import { expect } from "bun:test";
import { randomUUID } from "node:crypto";
import type { Subprocess } from "bun";
import type { WagerRequest } from "../../src/application/contracts.js";
import { WagerKind } from "../../src/domain/transaction.js";
import type { Config } from "../../src/infrastructure/config.js";
import type { TransactionResult } from "../../src/infrastructure/entities.js";

export interface WalletDTO {
  id: string;
  playerId: string;
  balance: { amount: string; currency: string };
  version: number;
}

export async function api<T>(
  path: string,
  options: {
    body?: unknown;
    method?: string;
    key?: string;
    origin?: string;
    correlation?: string;
  } = {},
) {
  const response = await fetch(
    `${options.origin ?? "http://127.0.0.1:4100"}${path}`,
    {
      method: options.method ?? (options.body === undefined ? "GET" : "POST"),
      headers: {
        "Content-Type": "application/json",
        ...(options.key ? { "Idempotency-Key": options.key } : {}),
        ...(options.correlation
          ? { "x-correlation-id": options.correlation }
          : {}),
      },
      ...(options.body === undefined
        ? {}
        : { body: JSON.stringify(options.body) }),
      signal: AbortSignal.timeout(20000),
    },
  );
  return { status: response.status, body: (await response.json()) as T };
}

export async function openWallet(amount = "100.00", currency = "BRL") {
  const response = await api<WalletDTO>("/wallets", {
    body: { playerId: randomUUID(), initialBalance: { amount, currency } },
  });
  expect(response.status).toBe(201);
  return response.body;
}

export function request(
  wallet: WalletDTO,
  props: Partial<WagerRequest> = {},
): WagerRequest {
  return {
    providerId: "provider-a",
    externalTransactionId: randomUUID(),
    walletId: wallet.id,
    playerId: wallet.playerId,
    roundId: "r",
    gameId: "g",
    kind: WagerKind.Bet,
    money: { amount: "10.00", currency: wallet.balance.currency },
    ...props,
  };
}

export async function submit(
  body: WagerRequest,
  origin?: string,
  key = `${body.providerId}:${body.externalTransactionId}`,
) {
  return api<TransactionResult>("/wagering/transactions", {
    body,
    origin,
    key,
  });
}

export async function waitUntil(
  check: () => Promise<boolean> | boolean,
  timeout = 15000,
  failure = "Condition did not become true",
) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (await check()) return;
    await Bun.sleep(30);
  }
  throw new Error(failure);
}

export class ChildProcess {
  readonly process: Subprocess<"ignore", "pipe", "pipe">;
  logs = "";
  private readonly output: Promise<unknown>;
  constructor(
    config: Config,
    port: number,
    role: "api" | "worker",
    mode = "normal",
  ) {
    const env = Object.fromEntries(
      Object.entries(config).map(([key, value]) => [key, String(value)]),
    );
    this.process = Bun.spawn(
      [process.execPath, "tests/integration/process.ts", mode],
      {
        env: { ...process.env, ...env, PORT: String(port), APP_ROLE: role },
        stdin: "ignore",
        stdout: "pipe",
        stderr: "pipe",
      },
    );
    const capture = async (stream: ReadableStream<Uint8Array>) => {
      for await (const chunk of stream)
        this.logs += new TextDecoder().decode(chunk);
    };
    this.output = Promise.all([
      capture(this.process.stdout),
      capture(this.process.stderr),
    ]);
  }
  async ready() {
    await waitUntil(
      () => this.logs.includes('"marker":"ready"'),
      20000,
      `Child did not start: ${this.logs}`,
    );
  }
  async stop(signal: "SIGTERM" | "SIGKILL" = "SIGTERM") {
    if (this.process.exitCode === null) this.process.kill(signal);
    await this.process.exited;
    await this.output;
  }
  async marker(name: string, timeout = 15000): Promise<void> {
    try {
      await waitUntil(() => this.logs.includes(`"marker":"${name}"`), timeout);
    } catch {
      throw new Error(
        `Missing ${name} in child ${this.process.pid}: ${this.logs}`,
      );
    }
  }
}
