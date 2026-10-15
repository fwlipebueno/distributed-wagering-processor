# Decisões de arquitetura

## Limite do sistema

Monólito modular: domínio independente de NestJS e ORM; um caso de uso financeiro compartilhado por HTTP e SQS; adaptadores de PostgreSQL, broker e HTTP. Os processos podem executar apenas API, apenas workers ou ambos. O Compose usa três APIs e dois workers, todos sem estado financeiro local.

MikroORM 6 foi escolhido pela Unit of Work, Identity Map por `em.fork()`, transação explícita e locks pessimistas. As linhas de persistência são separadas dos objetos de domínio: `Money` é reconstruído por factory, sem decorators ou tipos monetários do ORM. O caso de uso conhece o EntityManager para tornar o limite transacional visível; não há uma camada genérica de repositories sobre operações já expressas pelo ORM.

## Dinheiro e modelo

`Money` guarda centavos em `bigint`, com instâncias congeladas. Não há conversão monetária para `number`. Adição, subtração, comparação e serialização são exatas, inclusive acima do limite seguro de inteiros JavaScript. Usar inteiros de escala fixa evita uma dependência decimal e corresponde ao escopo de duas casas do desafio. Casas excedentes são rejeitadas, nunca arredondadas.

O banco usa `numeric(20,2)` e o custom type exige strings em ambas as direções. A escala e a precisão são explícitas; moedas suportadas: BRL, USD e EUR. Um crédito além do limite de armazenamento é rejeitado com `BALANCE_LIMIT_EXCEEDED`.

As classes de domínio usam construtores privados/protegidos e factories. `rehydrate` reconstrói estado persistido sem executar transições. Datas são copiadas na entrada e na leitura para impedir alterações externas do estado. Wallet encapsula saldo e versão; cada movimento devolve um ledger entry validado. Ledger não tem métodos mutadores e congela seus campos.

Abertura positiva gera `OPENING` e crédito de zero até o saldo inicial, preservando versão 1. Abertura zero não produz transação/ledger. Todo movimento posterior incrementa a versão uma vez; `LOSS`, rejeições, pendências e replay não a incrementam.

## Transação e concorrência

Cada execução financeira usa `em.fork().transactional()`, com isolamento padrão READ COMMITTED e `PESSIMISTIC_WRITE` na **wallet**. O lock permanece até o commit. Não há lock de aplicação global, nem dependência de ordenação FIFO. Wallets distintas continuam independentes.

Ordem de acesso: inbox quando aplicável → wallet → wager. O worker de referência e a terminalização de falhas também adquirem wallet antes do wager. Referências são lidas sem adquirir locks em outra wallet: referências com contexto diferente são rejeitadas, evitando ciclos entre wallets.

Na mesma transação ficam wager, snapshot da resposta, saldo/versão, ledger, inbox processada e eventos da outbox. Flushes intermediários ordenam a persistência para FKs/triggers, mas **não são commits**. Falhas de gravação ou de validação das constraints causam rollback. O SDK SQS não é chamado nesse limite financeiro.

Lock timeout: 3 segundos por espera; statement timeout: 15 segundos. Conflitos de lock, deadlock, serialização e falhas transitórias recebem até três tentativas SQL com nova Unit of Work e backoff curto com jitter. Unicidade disputada recebe retry para reler a identidade vencedora. Esgotamento retorna 503 na API ou agenda redelivery no broker. Esses retries não transformam rejeições de negócio em novas operações.

A classificação transitória exige um sinal conhecido de PostgreSQL, rede, timeout ou SDK AWS (throttling, HTTP 429/5xx ou atributo de retry). Desconexões sem SQLSTATE reconhecem as mensagens específicas do driver. Um erro inesperado sem esses sinais não recebe retry SQL, retorna HTTP 500 com `INTERNAL_ERROR` e segue para DLQ no consumidor. A outbox preserva seu agendamento persistente quando a publicação falha.

## Garantias no PostgreSQL

- `UNIQUE(player_id,currency)`, key global única e identidade externa única por provedor;
- índice parcial de uma reversão `PROCESSED` por `(reference_transaction_id,kind)` e de uma abertura por wallet;
- FKs de wallet, referência, ledger e inbox; CHECKs de espécie/estado, failureCode, referência exigida, valor, aritmética e não negatividade; `NaN` é proibido explicitamente;
- ledger bloqueia UPDATE, DELETE e TRUNCATE; wagers bloqueiam DELETE e qualquer UPDATE após estado terminal; identidade e moeda da wallet são imutáveis;
- inserção de ledger trava a wallet e valida encadeamento, espécie, direção, valor, moeda e vínculo com uma transação processada;
- constraint triggers deferred verificam no commit a igualdade entre saldo e ledger, a versão `1 + movimentos posteriores à abertura`, a quantidade correta de lançamentos por wager e o contexto/tipo/valor de referências processadas.

A validação da soma no commit percorre o histórico da wallet no UPDATE da wallet e no INSERT do ledger: são duas somas por movimento normal, com custo O(n) cada. Acumular n movimentos tem custo cumulativo O(n²). `bun run test:load` mede requisições com históricos crescentes; sua latência inclui HTTP, ORM, locks e publicação concorrente, sem isolar o custo da trigger. Alterar essa proteção exige demonstrar equivalência das invariantes e medir o ganho.

A contagem de entradas por wager usa um índice em `ledger(transaction_id)`, evitando a varredura global observada quando havia apenas o índice composto iniciado por wallet. Esse índice tem custo de armazenamento e manutenção por inserção; os planos medidos constam em [AUDIT.md](AUDIT.md).

Superusuários podem desabilitar triggers. A aplicação não faz isso; a fixture descartável de corrupção usa esse privilégio. Separar papéis de migrations e runtime é uma etapa de implantação real, não uma garantia contra um administrador do banco.

As três migrations são reversíveis. A segunda proíbe NaN, duplicidade de abertura e alteração da identidade da wallet, e valida o contexto das operações sem ledger. A terceira adiciona o índice da contagem de ledger por transação. O Compose executa o migrator dedicado antes das réplicas.

## Idempotência

`Idempotency-Key` é obrigatório em HTTP e não recebe default silencioso. Na fila, a key vem de `data.idempotencyKey`. Ela fica vinculada ao hash e à identidade `(providerId,externalTransactionId)`; trocar a key da mesma operação também produz conflito.

O SHA-256 usa UTF-8 do JSON canônico do DTO validado: ordenação lexicográfica recursiva de chaves, sem espaços, ordem de arrays preservada e campos undefined omitidos. Os campos são provider, identidade externa, player, wallet, round, game, kind, money e referência opcional. Header, key e metadados de transporte não entram nesse hash. Escala monetária fixa evita aliases como `1` versus `1.00`; campos desconhecidos são rejeitados.

Identificadores de provedor, operação, rodada e jogo usam ASCII alfanumérico e `_.:-`, até 128 caracteres, começando por letra ou dígito. `internal` é reservado à abertura. A key aceita até 256 caracteres ASCII visíveis. Esses limites e a rejeição de zeros extras à esquerda são interpretações de contrato explícitas para eliminar representações ambíguas.

A inbox usa `(consumerName,messageId)` único e um hash de `{type,data}`; esse hash inclui a key para detectar reaproveitamento inconsistente de uma mensagem. Um conflito de inbox não altera a operação original. O insert com ON CONFLICT e o lock da inbox são persistentes e funcionam entre processos.

O resultado, com saldo observado e failureCode, é gravado em JSONB. Replays terminais leem esse snapshot, sem recalcular a partir do saldo atual. Pendências podem avançar para um resultado terminal, que então fica congelado. Perda da conexão durante commit é tratada como resultado incerto: reenviar a mesma key resolve se houve commit, sem duplicar efeitos.

## Estados e referências

```text
PENDING ────────────────> PROCESSED | REJECTED | FAILED
    └─> PENDING_REFERENCE ────────> PROCESSED | REJECTED | FAILED
                  └────────────> PENDING_REFERENCE (nova tentativa)
```

Transições saindo de estados terminais lançam `InvalidTransactionStateError` e são bloqueadas também pelo banco. `FAILED` registra falha permanente ao retomar uma operação já aceita, sem gerar ledger; inclui evento `WagerTransactionFailed`. Uma falha antes da aceitação/commit não inventa uma transação confirmada: a mensagem é preservada em retry/DLQ.

Referências são resolvidas por provider + ID externo. Precisam ser da mesma wallet, player, moeda e rodada. `REFUND` aceita apenas `BET`; `ROLLBACK` aceita `BET`, `WIN` e `REFUND`; `WIN` com referência exige `BET`. Reversões exigem o valor integral. `BET`, `LOSS` e `OPENING` não aceitam referência. `gameId` participa da identidade do payload, mas não da comparação de contexto da referência, conforme a lista expressa do enunciado.

O limite de reversão é **por tipo**. A regra adicional 4 da seção 7 diz: “Uma referência não pode ser revertida duas vezes pelo mesmo tipo de operação.” O índice único `(reference_transaction_id,kind)` permite um REFUND e um ROLLBACK sobre a mesma BET, impedindo a repetição de cada tipo. Exemplo verificado pelo teste concorrente: 100.00 → BET 10.00 → REFUND 10.00 → ROLLBACK 10.00 resulta em 110.00. Não há exclusividade entre os dois tipos na regra adotada. Reverter WIN ou REFUND debita; saldo insuficiente produz o código específico de reversão.

Referência ausente ou ainda não terminal gera `PENDING_REFERENCE`. Referência terminal rejeitada/falha causa `REFERENCE_NOT_PROCESSED`. O worker consulta vencimentos a cada 200 ms, verifica novamente após adquirir o lock da wallet e grava tentativas e o próximo vencimento. Defaults: 10 tentativas, backoff 1s × 2^tentativa limitado a 60s e TTL de 24h para referência não resolvida. O limite de tentativas encerra uma ausência contínua em cerca de cinco minutos; o TTL também cobre longa indisponibilidade do worker. Esgotamento rejeita com `REFERENCE_NOT_FOUND` e gera um evento final. Só a primeira pendência gera `WagerTransactionPendingReference`.

Uma pendência aceita pela fila pode ser acked: a responsabilidade por retomá-la já está duravelmente transferida ao worker de referências.

### Códigos de falha de negócio

| failureCode | Significado |
| --- | --- |
| `INSUFFICIENT_FUNDS` | Aposta sem saldo |
| `REVERSAL_INSUFFICIENT_FUNDS` | Reversão de crédito produziria saldo negativo |
| `CURRENCY_MISMATCH` | Operação em moeda diferente da wallet |
| `PLAYER_MISMATCH` | Player não é o titular da wallet |
| `REFERENCE_CONTEXT_MISMATCH` | Provider/player/wallet/moeda/rodada da referência divergente |
| `REFERENCE_KIND_INVALID` | Espécie de referência não permitida |
| `REFERENCE_NOT_PROCESSED` | Referência terminal não processada |
| `REFERENCE_AMOUNT_MISMATCH` | Tentativa de reversão parcial/divergente |
| `REFERENCE_ALREADY_REVERSED` | Esse tipo de reversão já foi aplicado |
| `REFERENCE_NOT_FOUND` | Referência não resolvida no prazo/tentativas |
| `BALANCE_LIMIT_EXCEEDED` | Crédito excede numeric(20,2) |
| `PERMANENT_INFRASTRUCTURE_ERROR` | Operação aceita terminou em FAILED durante retomada |

Esses resultados terminais permanecem auditáveis; replay não os reexecuta. Corrigir payload/identidade após 400 ou 409 exige resolver o conflito; não se deve mudar a key para mascarar uma intenção já identificada. Para 503, reenviar **a mesma** key é o procedimento seguro. Os códigos de contrato incluem `INVALID_PAYLOAD`, `INVALID_AMOUNT`, `INVALID_CURRENCY`, `INVALID_IDEMPOTENCY_KEY`, `REFERENCE_REQUIRED` e `REFERENCE_NOT_ALLOWED`; não são rejeições financeiras persistidas.

## Consumo SQS e shutdown

Inbox e financeiro compartilham o caso de uso HTTP. `DeleteMessage` só ocorre depois do retorno da transação SQL. Resultados de negócio terminais e pendências aceitas recebem ack; payload inválido, identidade conflitante e erros permanentes vão à DLQ. Erros transitórios usam visibilidade com backoff 2^receiveCount segundos, limitado a 60s, e limite padrão de cinco tentativas. A fila também tem RedrivePolicy, protegendo contra falhas repetidas antes de o código conseguir encaminhar à DLQ.

A visibilidade padrão é 30s e é renovada durante processamento. Em SIGTERM, param novas iterações; o long polling em andamento **conclui pela conexão original** e devolve a visibilidade de mensagens que chegaram durante a parada. Não se cancela o socket, evitando ocultar mensagens recebidas pelo broker depois do cancelamento local. Mensagens já em processamento têm até 25s para terminar; depois, o worker interrompe heartbeats, aguarda renovações já em andamento e devolve receipts conhecidos com visibilidade zero. Compose concede 40s. SIGKILL usa a expiração normal da visibilidade e redelivery; a inbox evita repetir o financeiro. Os dois caminhos de SIGTERM, inclusive o limite real de 25s, são testados em processos Linux.

O envio à DLQ antecede o ack da origem. Crash entre os dois pode duplicar diagnóstico, mas preserva a mensagem. Nenhum payload ou saldo completo é escrito em logs da aplicação.

## Outbox

Os quatro eventos mínimos têm subclasses concretas e versionadas de `IntegrationEvent`; abertura também gera evento processado e mudança de saldo. `LOSS` gera apenas processado, rejeições apenas rejeitado, pendências apenas pendência inicial. `WalletBalanceChanged` é criado quando um lançamento altera o saldo, seja débito ou crédito.

Cada publisher seleciona um evento já confirmado por `FOR UPDATE SKIP LOCKED`, envia ao SQS e grava `publishedAt` antes de liberar o lock. A transação do publisher é posterior e separada da financeira. Manter o lock durante o send simplifica exclusão entre publishers, ao custo de uma conexão ocupada durante I/O; o SDK tem timeout de 5s e uma tentativa por chamada. Falhas incrementam tentativas e persistem backoff exponencial de 1s a 60s. Eventos confirmados não são descartados por um limite arbitrário.

Crash depois do commit financeiro deixa a linha publicável por outra instância. Crash depois do send e antes de marcar publicação pode reenviar o **mesmo eventId**. O dedup ID do FIFO reduz duplicatas durante a janela do broker; consumidores externos precisam de inbox persistente por eventId. Não se promete exactly-once de transporte. Publishers podem enviar eventos fora de ordem; `walletVersion` permite detectar lacunas nos eventos de saldo.

## Observabilidade e reconciliação

Logs JSON incluem sempre as chaves correlationId, messageId, transactionId, walletId e providerId, nulas quando não se aplicam, além de PID. Valores, tokens e payloads não são logados. O correlationId HTTP pode vir de `X-Correlation-Id`; fila usa messageId e preserva causationId nos eventos e nas retomadas.

Prometheus cobre transições confirmadas por status, replays, retries por origem, encaminhamentos à DLQ, profundidade da DLQ incluindo redrive do broker, conflitos de lock, idade do evento não publicado mais antigo, histograma de latência e divergências de reconciliação. Contadores são por processo e reiniciam com ele; o banco é a auditoria durável. Gauges são atualizados periodicamente e durante scrape; falha de dependência mantém a última amostra sem impedir o scrape dos demais indicadores.

Reconciliação segura um lock compartilhado na wallet e soma o ledger na mesma transação, impedindo comparar snapshots inconsistentes durante um movimento. Diferença é `stored - calculated`, podendo ser negativa. Divergência gera resposta `consistent:false`, log e métrica. O endpoint nunca altera saldo ou ledger.

## Autenticação e limites

Autenticação é opcional na seção 2 e não está ativada. `ProviderIdentityPort`, recebido por `createApplication`, permite integrar um IdP externo: validar JWT OIDC por issuer/audience/JWKS e autorizar provider e recursos antes do caso de uso. Health permanece público; a fila é um canal interno com validação de contrato.

O Compose usa LocalStack 4.10.0 sem persistência do broker e PostgreSQL com volume persistente. Os testes comprovam recuperação de processos da aplicação e do outbox diante de falhas de comunicação, mantendo o broker ativo. Recriar o emulador pode perder mensagens já confirmadas; a aplicação não recupera esse estado. Réplicas usam restart policy e readiness. A validação local não depende de conta AWS.

Double-entry, OpenTelemetry e dashboard não estão implementados. Os resultados da carga local estão em `AUDIT.md`; não há medição em AWS real nem estimativa de capacidade de produção.
