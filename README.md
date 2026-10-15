<p align="right">
  <kbd><a href="README.en.md">English</a></kbd>
  <kbd><a href="README.md">Português</a></kbd>
</p>

# Distributed Wagering Processor

Serviço NestJS com API HTTP e workers independentes, PostgreSQL como autoridade financeira e SQS via LocalStack. O Compose inicia **três APIs e dois workers**.

[AUDIT.md](AUDIT.md) relaciona os requisitos às verificações executadas.

## Executar

Pré-requisito: Docker com Compose v2 ou superior, engine Linux e portas locais 3000–3002, 4566 e 55432 disponíveis.

```sh
docker compose up --build -d --wait
docker compose ps
```

APIs: `http://localhost:3000`, `http://localhost:3001` e `http://localhost:3002`. Health checks e métricas usam `/health/live`, `/health/ready` e `/metrics`. Os workers expõem esses endpoints na porta 3000 dentro da rede do Compose.

O serviço `migrate` aplica migrations antes das APIs e dos workers. As filas são provisionadas idempotentemente na inicialização:

- `wager-transactions.fifo`: entrada;
- `wager-transactions-dlq.fifo`: mensagens inválidas e retries esgotados;
- `wager-events.fifo`: eventos de integração publicados pela outbox.

Para acompanhar logs ou reiniciar somente a aplicação, mantendo PostgreSQL e LocalStack:

```sh
docker compose logs -f api worker
docker compose restart api api-2 api-3 worker
```

`docker compose stop` para os serviços. `docker compose down` remove os containers e a rede, preservando o volume do PostgreSQL. O estado do LocalStack é efêmero neste Compose; os testes de recuperação reiniciam a aplicação mantendo o broker ativo. Veja os limites em [ARCHITECTURE.md](ARCHITECTURE.md).

## Desenvolvimento local

Bun **1.3.10**, TypeScript strict, NestJS 11.1.6 e MikroORM 6.6.8, com versões fixadas em `bun.lock`.

```sh
bun install --frozen-lockfile
docker compose up -d postgres localstack
bun run migrate
bun run typecheck
bun run lint
bun run build
bun run start
```

`bun run start` executa o código compilado; `bun run dev` executa TypeScript. O padrão local `APP_ROLE=all` inicia API e workers. `APP_ROLE=api` inicia a API; `APP_ROLE=worker` disponibiliza health/métricas e executa consumo, outbox e reprocessamento de referências. Pare as instâncias equivalentes do Compose antes de ocupar a mesma porta localmente.

`.env.example` documenta a configuração local. Bun carrega `.env` quando presente. O Compose declara sua própria configuração em `compose.yaml`. As credenciais apresentadas são exclusivamente locais, sem segredos reais.

`bun run migrate:down` reverte **a última** migration. A reversão da migration inicial remove as tabelas financeiras; use-a em bancos descartáveis. A suíte valida a reversão completa em um banco criado para o teste.

## HTTP

| Método e rota | Comportamento |
| --- | --- |
| `POST /wallets` | Abre wallet única por player/moeda; retorna 201 |
| `GET /wallets/:walletId` | Saldo materializado e versão |
| `GET /wallets/:walletId/ledger?limit=50&cursor=...` | Entradas em ordem estável; `nextCursor` opaco ou `null`; limite 1–100 |
| `POST /wagering/transactions` | Executa o caso de uso; exige `Idempotency-Key` |
| `GET /wagering/transactions/:transactionId` | Consulta por ID interno |
| `GET /providers/:providerId/wagering/transactions/:externalTransactionId` | Consulta por identidade do provedor |
| `POST /wallets/:walletId/reconciliation` | Compara saldo e soma do ledger sem corrigir diferenças |
| `GET /health/live` | Processo vivo |
| `GET /health/ready` | PostgreSQL e todas as filas alcançáveis; 503 se indisponíveis |
| `GET /metrics` | Prometheus, sem labels de alta cardinalidade |

Criar wallet:

```json
{
  "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
  "initialBalance": { "amount": "100.00", "currency": "BRL" }
}
```

Enviar aposta, usando o ID retornado pela criação e o header `Idempotency-Key: provider-a:bet-1`:

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

`REFUND` e `ROLLBACK` exigem `referenceExternalTransactionId`; `WIN` admite a referência opcional a uma `BET`. Referências são IDs externos do mesmo provedor. `OPENING` é reservado à criação da wallet.

Valores de entrada precisam ser strings com exatamente duas casas, sem sinal, zeros extras à esquerda, expoente ou arredondamento implícito. São suportadas BRL, USD e EUR, sempre com escala 2. Operações que movimentam dinheiro exigem valor positivo; abertura e `LOSS` aceitam zero. Limite por valor: `999999999999999999.99`.

### Respostas e idempotência

| Situação na submissão | HTTP | Identificação |
| --- | --- | --- |
| Aplicada ou replay de uma aplicada | 200 | `status: PROCESSED` |
| Referência ainda não resolvida | 202 | `status: PENDING_REFERENCE` |
| Contrato inválido | 400 | `error.code` |
| Wallet/transação inexistente em uma consulta | 404 | `error.code` |
| Wallet duplicada, key/payload ou identidade externa conflitante | 409 | `error.code` |
| Regra de negócio rejeitada ou seu replay | 422 | `status: REJECTED`, `failureCode` |
| Falha permanente persistida | 500 | `status: FAILED`, `failureCode` |
| Falha interna inesperada | 500 | `error.code: INTERNAL_ERROR` |
| Infraestrutura temporariamente indisponível | 503 | `error.code`, reenviar com a mesma key |

GET de um recurso existente retorna 200 e seu estado, inclusive se rejeitado. Uma wallet inexistente na submissão retorna 404 e não é aceita: a FK impede transações órfãs. Erros de contrato/conflito não criam uma nova operação.

Uma resposta financeira contém `transactionId`, `status`, `balance`, `idempotentReplay` e, em rejeições, `failureCode`. O replay devolve o **saldo histórico persistido**, mesmo após outras operações. Os estados terminais são imutáveis; adicionar saldo não transforma uma rejeição antiga em sucesso. A mesma operação externa com outra key também é conflito. Replays de pendências mostram o estado persistido mais recente, que pode ter sido concluído pelo worker.

## Mensagens

O envelope SQS é `WagerTransactionRequested`, com `messageId`, `occurredAt` ISO-8601 e `data` contendo o mesmo corpo HTTP mais `idempotencyKey`. Para publicar com a CLI incluída no emulador:

```sh
docker compose exec localstack awslocal sqs get-queue-url --queue-name wager-transactions.fifo
```

Use a URL retornada em `awslocal sqs send-message`, com `--message-body` contendo o envelope oficial, `--message-group-id` igual à wallet e um `--message-deduplication-id` único. A garantia financeira independe desses atributos do broker. A suíte abaixo demonstra envio, redelivery e DLQ com o SDK AWS real.

Os eventos de saída têm `eventId`, `eventType`, `aggregateId`, `correlationId`, `causationId` quando aplicável, `occurredAt`, `version` e `data`. Consumidores externos devem persistir a deduplicação por `eventId`: a publicação é at-least-once, inclusive após crash entre send e commit da marca de publicação.

## Testes e demonstração

```sh
bun run test
docker compose --profile tests build test
docker compose --profile tests run --rm test bun run test
docker compose --profile tests run --rm test
bun run demo
```

Sem Bun no host, a demonstração também pode ser executada na imagem de testes, com o Compose completo ativo:

```sh
docker compose --profile tests run --rm -e API_URL=http://api:3000 test bun run demo
```

Unidade usa o runner do Bun. Integração roda em Linux com PostgreSQL e LocalStack reais e cria um banco e um prefixo de filas exclusivos, removidos ao final. Pode rodar enquanto o Compose completo está ativo. Não substitui os serviços por mocks. Os hooks de crash existem somente na fixture de teste; o entrypoint normal não os ativa.

A suíte inclui duplicatas entre três processos, colisões de identidade entre wallets, disputa HTTP/SQS, limites monetários, referências tardias, reversões, atomicidade, constraints e recuperação de falhas. A reconciliação de **todas** as wallets e suas versões é verificada após cada cenário. As fixtures de corrupção e deadlock usam triggers exclusivamente no banco descartável.

`bun run demo` verifica duas apostas de 80.00 sobre 100.00: uma aplicada, outra rejeitada, replay histórico e reconciliação final de 20.00. Qualquer divergência encerra o comando com erro.

### Teste de carga

Com o Compose completo ativo:

```sh
bun run test:load
```

Sem Bun no host:

```sh
docker compose --profile tests run --rm -e API_URLS=http://api:3000,http://api-2:3000,http://api-3:3000 test bun run test:load
```

O experimento cria uma wallet por etapa, prepara 0, 100 e 500 movimentos e faz 10 requisições de aquecimento. Mede 60 créditos únicos de 0.01 por etapa, com concorrência 3 e distribuição entre APIs. Preparação e aquecimento ficam fora do tempo medido. `API_URLS`, `LOAD_REQUESTS`, `LOAD_CONCURRENCY` e `LOAD_ENVIRONMENT` permitem registrar outra configuração.

A saída JSON registra ambiente visível, throughput, p50/p95/p99 HTTP, erros, conflitos de lock, lag do outbox e reconciliação. O lag é amostrado a cada 250 ms e inclui eventos da preparação. A espera final por drenagem tem limite de 30 s. Os registros financeiros do experimento permanecem no banco; não use um ambiente de produção. A amostra serve para inspecionar custo crescente, sem meta de RPS ou extrapolação para produção.

### Documentação

As decisões arquiteturais, regras de negócio e limitações estão em [ARCHITECTURE.md](ARCHITECTURE.md). Os cenários validados e resultados das verificações estão em [AUDIT.md](AUDIT.md).
