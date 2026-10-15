import { Migrator } from "@mikro-orm/migrations";
import { type EntityManager, MikroORM } from "@mikro-orm/postgresql";
import { entities } from "./entities.js";
import { Migration202610140001 } from "./migrations/Migration202610140001.js";
import { Migration202610140002 } from "./migrations/Migration202610140002.js";
import { Migration202610140003 } from "./migrations/Migration202610140003.js";

export async function connectDatabase(url: string) {
  return MikroORM.init({
    clientUrl: url,
    entities,
    extensions: [Migrator],
    forceUndefined: true,
    migrations: {
      migrationsList: [
        Migration202610140001,
        Migration202610140002,
        Migration202610140003,
      ],
      transactional: true,
      allOrNothing: true,
      snapshot: false,
    },
    pool: { min: 0, max: 12 },
    driverOptions: {
      connection: { statement_timeout: 15000, connectionTimeoutMillis: 5000 },
    },
  });
}

export async function sql<T>(
  em: EntityManager,
  query: string,
  params: unknown[] = [],
): Promise<T[]> {
  return em
    .getConnection()
    .execute<T[]>(query, params, "all", em.getTransactionContext());
}

export function databaseErrorCode(error: unknown): string | undefined {
  if (error && typeof error === "object") {
    if ("code" in error && typeof error.code === "string") return error.code;
    if ("cause" in error) return databaseErrorCode(error.cause);
  }
  return undefined;
}

export function transientError(error: unknown): boolean {
  if (error instanceof TypeError || error instanceof RangeError)
    return error.cause !== undefined && transientError(error.cause);
  const code = databaseErrorCode(error);
  if (
    code !== undefined &&
    ([
      "40001",
      "40P01",
      "55P03",
      "57014",
      "53300",
      "57P01",
      "57P02",
      "57P03",
      "ECONNRESET",
      "ECONNREFUSED",
      "ETIMEDOUT",
      "EPIPE",
      "EAI_AGAIN",
      "ENOTFOUND",
      "EHOSTUNREACH",
      "ENETUNREACH",
    ].includes(code) ||
      code.startsWith("08"))
  )
    return true;
  if (!(error instanceof Error)) return false;
  if (
    [
      "TimeoutError",
      "KnexTimeoutError",
      "RequestTimeout",
      "RequestTimeoutException",
      "RequestThrottled",
    ].includes(error.name)
  )
    return true;
  // pg can report a lost connection without SQLSTATE; Knex may prefix its query.
  if (
    [
      "Connection terminated",
      "Connection terminated unexpectedly",
      "timeout expired",
      "Client has encountered a connection error and is not queryable",
      "Client was closed and is not queryable",
    ].includes(error.message.split(" - ").at(-1) ?? "") ||
    error.message.startsWith("Knex: Timeout acquiring a connection.")
  )
    return true;
  if (
    "$retryable" in error &&
    error.$retryable &&
    typeof error.$retryable === "object"
  )
    return true;
  const metadata = "$metadata" in error ? error.$metadata : undefined;
  if (
    metadata &&
    typeof metadata === "object" &&
    "httpStatusCode" in metadata
  ) {
    const status = metadata.httpStatusCode;
    if (
      typeof status === "number" &&
      (status === 429 || (status >= 500 && status < 600))
    )
      return true;
  }
  return error.cause !== undefined && transientError(error.cause);
}
