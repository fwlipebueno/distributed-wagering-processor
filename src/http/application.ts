import "reflect-metadata";
import { randomUUID } from "node:crypto";
import {
  type ArgumentsHost,
  Body,
  Catch,
  Controller,
  type ExceptionFilter,
  Get,
  Headers,
  HttpException,
  Inject,
  Module,
  Param,
  ParseUUIDPipe,
  Post,
  Query,
  Res,
} from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import type { NestExpressApplication } from "@nestjs/platform-express";
import type { Request, Response } from "express";
import { WagerService } from "../application/wager-service.js";
import { DomainError, RequestError } from "../domain/errors.js";
import type { EventContext } from "../domain/events.js";
import { WagerStatus } from "../domain/transaction.js";
import { type Config, readConfig } from "../infrastructure/config.js";
import {
  connectDatabase,
  databaseErrorCode,
  transientError,
} from "../infrastructure/database.js";
import { log, Metrics } from "../infrastructure/observability.js";
import { SqsGateway } from "../infrastructure/sqs.js";
import { type WorkerHooks, Workers } from "../infrastructure/workers.js";

function context(header?: string): EventContext {
  return {
    correlationId:
      header && /^[A-Za-z0-9_.:-]{1,128}$/.test(header) ? header : randomUUID(),
  };
}

@Controller()
class FinanceController {
  constructor(@Inject(WagerService) private readonly wagers: WagerService) {}

  @Post("wallets")
  open(
    @Body() body: unknown,
    @Headers("x-correlation-id") correlation?: string,
  ) {
    return this.wagers.openWallet(body, context(correlation));
  }

  @Get("wallets/:walletId")
  wallet(@Param("walletId", new ParseUUIDPipe()) id: string) {
    return this.wagers.getWallet(id);
  }

  @Get("wallets/:walletId/ledger")
  ledger(
    @Param("walletId", new ParseUUIDPipe()) id: string,
    @Query("cursor") cursor?: string,
    @Query("limit") input?: string,
  ) {
    if (
      input !== undefined &&
      (typeof input !== "string" || !/^\d{1,3}$/.test(input))
    )
      throw new RequestError("INVALID_LIMIT", 400);
    const limit = input === undefined ? 50 : Number(input);
    if (limit < 1 || limit > 100) throw new RequestError("INVALID_LIMIT", 400);
    if (cursor !== undefined && typeof cursor !== "string")
      throw new RequestError("INVALID_CURSOR", 400);
    return this.wagers.ledger(id, cursor, limit);
  }

  @Get("wagering/transactions/:transactionId")
  transaction(@Param("transactionId", new ParseUUIDPipe()) id: string) {
    return this.wagers.getTransaction({ id });
  }

  @Get("providers/:providerId/wagering/transactions/:externalTransactionId")
  external(
    @Param("providerId") providerId: string,
    @Param("externalTransactionId") externalTransactionId: string,
  ) {
    return this.wagers.getTransaction({ providerId, externalTransactionId });
  }

  @Post("wagering/transactions")
  async submit(
    @Body() body: unknown,
    @Headers("idempotency-key") key: unknown,
    @Headers("x-correlation-id") correlation: string | undefined,
    @Res() res: Response,
  ) {
    const result = await this.wagers.execute(body, key, context(correlation));
    const status =
      result.status === WagerStatus.Rejected
        ? 422
        : result.status === WagerStatus.Failed
          ? 500
          : result.status === WagerStatus.PendingReference ||
              result.status === WagerStatus.Pending
            ? 202
            : 200;
    res.status(status).json(result);
  }

  @Post("wallets/:walletId/reconciliation")
  async reconcile(
    @Param("walletId", new ParseUUIDPipe()) id: string,
    @Headers("x-correlation-id") correlation: string | undefined,
    @Res() res: Response,
  ) {
    res.status(200).json(await this.wagers.reconcile(id, context(correlation)));
  }
}

@Controller()
class HealthController {
  constructor(@Inject("runtime") private readonly runtime: Runtime) {}
  @Get("health/live")
  live() {
    return { status: "ok" };
  }
  @Get("health/ready")
  async ready() {
    try {
      await Promise.all([
        this.runtime.orm.em.fork().getConnection().execute("SELECT 1"),
        this.runtime.sqs.ready(),
      ]);
      return { status: "ready" };
    } catch {
      throw new RequestError("DEPENDENCY_UNAVAILABLE", 503);
    }
  }
  @Get("metrics")
  async metrics(@Res() res: Response) {
    await this.runtime.workers.updateLag().catch(() => {
      log(
        "warn",
        "metrics_dependency_unavailable",
        {},
        "DEPENDENCY_UNAVAILABLE",
      );
    });
    res.setHeader("Content-Type", this.runtime.metrics.registry.contentType);
    res.send(await this.runtime.metrics.registry.metrics());
  }
}

@Catch()
class ErrorFilter implements ExceptionFilter {
  catch(error: unknown, host: ArgumentsHost): void {
    const response = host.switchToHttp().getResponse<Response>();
    const request = host.switchToHttp().getRequest<Request>();
    let status = 500;
    let code = "INTERNAL_ERROR";
    if (error instanceof RequestError) {
      status = error.httpStatus;
      code = error.code;
    } else if (error instanceof DomainError) {
      status = 400;
      code = error.code;
    } else if (error instanceof HttpException) {
      status = error.getStatus();
      code = status === 400 ? "INVALID_PAYLOAD" : "HTTP_ERROR";
    } else if (transientError(error)) {
      status = 503;
      code = "INFRASTRUCTURE_UNAVAILABLE";
    }
    const ctx = context(request.get("x-correlation-id"));
    log(
      status >= 500 ? "error" : "warn",
      "http_request_failed",
      ctx,
      databaseErrorCode(error) ?? code,
    );
    response
      .status(status)
      .json({ error: { code, correlationId: ctx.correlationId } });
  }
}

export interface ProviderIdentityPort {
  authorize(request: {
    authorization?: string;
    providerId?: string;
    path: string;
    method: string;
  }): Promise<void>;
}

export async function createRuntime(config: Config, hooks?: WorkerHooks) {
  const orm = await connectDatabase(config.DATABASE_URL);
  const sqs = new SqsGateway(config);
  try {
    await sqs.initialize();
  } catch (error) {
    sqs.close();
    await orm.close(true);
    throw error;
  }
  const metrics = new Metrics();
  const wagers = new WagerService(orm, metrics, config);
  const workers = new Workers(orm, wagers, sqs, metrics, config, hooks);
  return { orm, sqs, metrics, wagers, workers };
}
export type Runtime = Awaited<ReturnType<typeof createRuntime>>;

@Module({})
class AppModule {}

export async function createApplication(
  options: {
    config?: Config;
    hooks?: WorkerHooks;
    providerIdentity?: ProviderIdentityPort;
  } = {},
) {
  const config = options.config ?? readConfig();
  const runtime = await createRuntime(config, options.hooks);
  try {
    const app = await NestFactory.create<NestExpressApplication>(
      {
        module: AppModule,
        controllers:
          config.APP_ROLE === "worker"
            ? [HealthController]
            : [FinanceController, HealthController],
        providers: [
          { provide: WagerService, useValue: runtime.wagers },
          { provide: "runtime", useValue: runtime },
        ],
      },
      { logger: false },
    );
    app.useGlobalFilters(new ErrorFilter());
    app.useGlobalGuards({
      async canActivate(ctx) {
        if (ctx.getClass() === HealthController || !options.providerIdentity)
          return true;
        const request = ctx.switchToHttp().getRequest<Request>();
        const body: unknown = request.body;
        const pathProvider = request.params.providerId;
        const providerId =
          body &&
          typeof body === "object" &&
          "providerId" in body &&
          typeof body.providerId === "string"
            ? body.providerId
            : typeof pathProvider === "string"
              ? pathProvider
              : undefined;
        await options.providerIdentity.authorize({
          authorization: request.get("authorization"),
          providerId,
          path: request.path,
          method: request.method,
        });
        return true;
      },
    });
    await app.listen(config.PORT, "0.0.0.0");
    if (config.APP_ROLE !== "api") runtime.workers.start();
    let closing: Promise<void> | undefined;
    const close = () => {
      closing ??= (async () => {
        await runtime.workers.stop();
        await app.close();
        runtime.sqs.close();
        await runtime.orm.close(true);
        log("info", "service_stopped");
      })();
      return closing;
    };
    log("info", "service_started");
    return { ...runtime, app, close };
  } catch (error) {
    runtime.sqs.close();
    await runtime.orm.close(true);
    throw error;
  }
}
