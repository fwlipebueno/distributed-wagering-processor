<p align="right">
  <kbd><a href="README.en.md">English</a></kbd>
  <kbd><a href="README.md">Português</a></kbd>
</p>

# Distributed Wagering Processor

NestJS service with an HTTP API and independent workers, PostgreSQL as the financial authority, and SQS through LocalStack. Compose starts **three APIs and two workers**.

[AUDIT.md](AUDIT.md) maps requirements to the checks performed.

## Running

Prerequisites: Docker with Compose v2 or later, a Linux engine, and available local ports 3000–3002, 4566, and 55432.

```sh
docker compose up --build -d --wait
docker compose ps
```

APIs: `http://localhost:3000`, `http://localhost:3001`, and `http://localhost:3002`. Health checks and metrics are available at `/health/live`, `/health/ready`, and `/metrics`. Workers expose these endpoints on port 3000 inside the Compose network.

The `migrate` service applies migrations before the APIs and workers start. Queues are provisioned idempotently at startup:

- `wager-transactions.fifo`: incoming requests;
- `wager-transactions-dlq.fifo`: invalid messages and exhausted retries;
- `wager-events.fifo`: integration events published by the outbox.

To follow logs or restart only the application, keeping PostgreSQL and LocalStack running:

```sh
docker compose logs -f api worker
docker compose restart api api-2 api-3 worker
```

`docker compose stop` stops the services. `docker compose down` removes containers and the network while preserving the PostgreSQL volume. LocalStack state is ephemeral in this Compose configuration; recovery tests restart the application while keeping the broker running. See the limits in [ARCHITECTURE.md](ARCHITECTURE.md).

## Local development

Bun **1.3.10**, strict TypeScript, NestJS 11.1.6, and MikroORM 6.6.8, with versions pinned in `bun.lock`.

```sh
bun install --frozen-lockfile
docker compose up -d postgres localstack
bun run migrate
bun run typecheck
bun run lint
bun run build
bun run start
```

`bun run start` runs compiled code; `bun run dev` runs TypeScript. The local default, `APP_ROLE=all`, starts the API and workers. `APP_ROLE=api` starts the API; `APP_ROLE=worker` exposes health/metrics and runs message consumption, outbox publication, and reference retries. Stop the corresponding Compose instances before using the same port locally.

`.env.example` documents local settings. Bun loads `.env` when present. Compose declares its own settings in `compose.yaml`. The supplied credentials are exclusively for local use and contain no real secrets.

`bun run migrate:down` rolls back **the latest** migration. Rolling back the initial migration removes the financial tables; use disposable databases. The suite checks a full rollback in a database created for the test.

## HTTP

| Method and route | Behavior |
| --- | --- |
| `POST /wallets` | Opens a wallet unique to a player/currency pair; returns 201 |
| `GET /wallets/:walletId` | Materialized balance and version |
| `GET /wallets/:walletId/ledger?limit=50&cursor=...` | Entries in stable order; opaque `nextCursor` or `null`; limit 1–100 |
| `POST /wagering/transactions` | Executes the use case; requires `Idempotency-Key` |
| `GET /wagering/transactions/:transactionId` | Looks up a transaction by internal ID |
| `GET /providers/:providerId/wagering/transactions/:externalTransactionId` | Looks up a transaction by provider identity |
| `POST /wallets/:walletId/reconciliation` | Compares the balance with the ledger sum without correcting differences |
| `GET /health/live` | Process liveness |
| `GET /health/ready` | PostgreSQL and all queues reachable; 503 when unavailable |
| `GET /metrics` | Prometheus, without high-cardinality labels |

Create a wallet:

```json
{
  "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
  "initialBalance": { "amount": "100.00", "currency": "BRL" }
}
```

Submit a bet using the wallet ID returned at creation and the header `Idempotency-Key: provider-a:bet-1`:

```json
{
  "providerId": "provider-a",
  "externalTransactionId": "bet-1",
  "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
  "walletId": "WALLET_ID",
  "roundId": "round-1",
  "gameId": "fortune-chimp",
  "kind": "BET",
  "money": { "amount": "80.00", "currency": "BRL" }
}
```

`REFUND` and `ROLLBACK` require `referenceExternalTransactionId`; `WIN` may optionally reference a `BET`. References are external IDs from the same provider. `OPENING` is reserved for wallet creation.

Input amounts must be strings with exactly two decimal places, without a sign, extra leading zeros, an exponent, or implicit rounding. BRL, USD, and EUR are supported, always with scale 2. Operations that move money require a positive amount; opening and `LOSS` allow zero. Maximum per value: `999999999999999999.99`.

### Responses and idempotency

| Submission outcome | HTTP | Identification |
| --- | --- | --- |
| Applied, or replay of an applied transaction | 200 | `status: PROCESSED` |
| Reference not yet resolved | 202 | `status: PENDING_REFERENCE` |
| Invalid contract | 400 | `error.code` |
| Wallet/transaction not found during a query | 404 | `error.code` |
| Duplicate wallet, conflicting key/payload, or external identity | 409 | `error.code` |
| Business rejection or its replay | 422 | `status: REJECTED`, `failureCode` |
| Persisted permanent failure | 500 | `status: FAILED`, `failureCode` |
| Unexpected internal failure | 500 | `error.code: INTERNAL_ERROR` |
| Temporarily unavailable infrastructure | 503 | `error.code`; resend with the same key |

GET returns 200 and the state of an existing resource, including rejected transactions. Submission against a nonexistent wallet returns 404 and is not accepted: the foreign key prevents orphan transactions. Contract errors and conflicts do not create a new operation.

A financial response contains `transactionId`, `status`, `balance`, `idempotentReplay`, and, for rejections, `failureCode`. Replay returns the **persisted historical balance**, even after other operations. Terminal states are immutable; adding funds does not turn a previous rejection into a success. Submitting the same external operation with another key is also a conflict. Replays of pending operations return the latest persisted state, which a worker may already have completed.

## Messaging

The SQS envelope is `WagerTransactionRequested`, with `messageId`, an ISO-8601 `occurredAt`, and `data` containing the HTTP body plus `idempotencyKey`. To publish using the CLI included in the emulator:

```sh
docker compose exec localstack awslocal sqs get-queue-url --queue-name wager-transactions.fifo
```

Use the returned URL with `awslocal sqs send-message`, with `--message-body` containing the official envelope, `--message-group-id` set to the wallet, and a unique `--message-deduplication-id`. Financial correctness does not depend on these broker attributes. The suite below exercises sending, redelivery, and DLQ handling with the real AWS SDK.

Outgoing events contain `eventId`, `eventType`, `aggregateId`, `correlationId`, `causationId` when applicable, `occurredAt`, `version`, and `data`. External consumers must persist deduplication by `eventId`: publication is at-least-once, including after a crash between sending and committing the publication marker.

## Tests and demonstration

```sh
bun run test
docker compose --profile tests build test
docker compose --profile tests run --rm test bun run test
docker compose --profile tests run --rm test
bun run demo
```

Without Bun on the host, the demonstration can also run in the test image with the full Compose stack active:

```sh
docker compose --profile tests run --rm -e API_URL=http://api:3000 test bun run demo
```

Unit tests use the Bun runner. Integration tests run on Linux with real PostgreSQL and LocalStack, creating a dedicated database and queue prefix that are removed afterward. They can run while the full Compose stack is active. Services are not replaced by mocks. Crash hooks exist only in the test fixture; the normal entrypoint does not enable them.

The suite includes duplicates across three processes, identity collisions between wallets, HTTP/SQS races, monetary boundaries, late references, reversals, atomicity, constraints, and failure recovery. Reconciliation of **all** wallets and their versions is checked after every scenario. Corruption and deadlock fixtures use triggers exclusively in the disposable database.

`bun run demo` verifies two bets of 80.00 against 100.00: one applied, one rejected, historical replay, and a final reconciliation of 20.00. Any discrepancy makes the command exit with an error.

### Load test

With the full Compose stack active:

```sh
bun run test:load
```

Without Bun on the host:

```sh
docker compose --profile tests run --rm -e API_URLS=http://api:3000,http://api-2:3000,http://api-3:3000 test bun run test:load
```

The experiment creates one wallet per stage, prepares 0, 100, and 500 movements, and sends 10 warmup requests. It measures 60 unique credits of 0.01 per stage, with concurrency 3 distributed across the APIs. Preparation and warmup are excluded from the measured time. `API_URLS`, `LOAD_REQUESTS`, `LOAD_CONCURRENCY`, and `LOAD_ENVIRONMENT` can record a different configuration.

The JSON output records the visible environment, throughput, HTTP p50/p95/p99, errors, lock conflicts, outbox lag, and reconciliation. Lag is sampled every 250 ms and includes preparation events. The final wait for draining is limited to 30 s. Financial records from the experiment remain in the database; do not use a production environment. This sample inspects growing costs, without an RPS target or extrapolation to production.

### Documentation

Architectural decisions, business rules, and limitations are documented in [ARCHITECTURE.md](ARCHITECTURE.md). Validated scenarios and verification results are available in [AUDIT.md](AUDIT.md). Both documents are in Portuguese.
