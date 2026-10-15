import { z } from "zod";

const schema = z.object({
  DATABASE_URL: z
    .string()
    .default("postgresql://wager:wager@localhost:55432/wager"),
  SQS_ENDPOINT: z.url().default("http://localhost:4566"),
  SQS_QUEUE_PREFIX: z
    .string()
    .max(40)
    .regex(/^[A-Za-z0-9_-]*$/)
    .default(""),
  AWS_REGION: z.string().default("us-east-1"),
  AWS_ACCESS_KEY_ID: z.string().default("test"),
  AWS_SECRET_ACCESS_KEY: z.string().default("test"),
  PORT: z.coerce.number().int().min(1).max(65535).default(3000),
  APP_ROLE: z.enum(["api", "worker", "all"]).default("all"),
  SQL_LOCK_TIMEOUT_MS: z.coerce.number().int().min(1).max(30000).default(3000),
  REFERENCE_MAX_ATTEMPTS: z.coerce.number().int().min(1).default(10),
  REFERENCE_BACKOFF_MS: z.coerce.number().int().min(1).default(1000),
  REFERENCE_TTL_MS: z.coerce.number().int().min(1).default(86400000),
  SQS_MAX_ATTEMPTS: z.coerce.number().int().min(1).max(1000).default(5),
  SQS_VISIBILITY_SECONDS: z.coerce.number().int().min(2).max(43200).default(30),
});

export function readConfig(
  env: Record<string, string | undefined> = process.env,
) {
  return schema.parse(env);
}
export type Config = ReturnType<typeof readConfig>;
