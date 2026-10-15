# Auditoria técnica

Referência: [Desafio técnico da Jungle Gaming](https://github.com/junglegaming/backend-challenge).

## Requisitos eliminatórios

| Requisito | Evidência verificada |
| --- | --- |
| Dinheiro exato | `Money` usa bigint; contratos e persistência usam strings.<br>Unidade verifica entradas inválidas, escala, moedas e valores acima do inteiro seguro.<br>Integração alcança o máximo de numeric(20,2), rejeita crédito excedente sem movimento e preserva o replay da rejeição. |
| Saldo não negativo e sem lost update | Duas apostas de 80.00 sobre 100.00 em processos distintos produzem um sucesso, uma rejeição e 20.00 finais. Reconciliação de saldo e versão ocorre após cada cenário. |
| Efeito financeiro único | 50 requisições HTTP e 50 entregas SQS concorrentes; colisões de key/identidade externa entre wallets; corrida HTTP/SQS com inbox persistente e transporte distinto. |
| Idempotência persistente | Unicidades no PostgreSQL, hash canônico e snapshot JSONB. Replay após outros movimentos devolve o saldo histórico; payload divergente é conflito. |
| Múltiplas instâncias | Três processos Linux com PIDs distintos; APIs e workers adicionais participam dos testes de corrida e recuperação. |
| Eventos somente após commit | Publisher não enxerga eventos de uma transação financeira ainda aberta. Falha de outbox reverte wallet, wager, ledger e inbox. |
| Ledger auditável | Constraints de vínculo, direção, valor, encadeamento, saldo e versão; UPDATE, DELETE e TRUNCATE recusados no PostgreSQL real. LOSS e rejeições não geram ledger. |
| Infraestrutura real nos testes | PostgreSQL 17.6 e LocalStack 4.10.0 em containers; SDK AWS, transações, locks, redelivery e processos reais. |

## Demais requisitos

| Seções | Implementação e verificação |
| --- | --- |
| 2, 4–6 | Autenticação opcional com porta de integração; Bun fixado, TypeScript strict, NestJS, MikroORM, Compose;<br>factories e estados encapsulados, incluindo cópias defensivas de datas; abertura positiva/zero; dinheiro, versão e moedas. |
| 7 | BET, WIN, LOSS, REFUND e ROLLBACK; referência por provedor/ID externo, contexto completo, valor integral, espécie e estado válidos;<br>uma reversão de cada tipo por referência; rejeição específica para reversão sem saldo.<br>A interpretação e o exemplo estão em [ARCHITECTURE.md](ARCHITECTURE.md#estados-e-referências). |
| 7.1–7.2 | Tentativas e backoff persistidos; worker resolve referência fora de ordem; esgotamento rejeita uma vez;<br>referência tardia não ressuscita a rejeição, inclusive em replay HTTP/SQS; falha permanente aceita termina em FAILED imutável. |
| 8–9 | Disputa de saldo, wallets independentes e identidades colidentes; todas as rotas HTTP, contratos de erro, cursor opaco, consultas, reconciliação e health. |
| 10 | Inbox por consumer/message, conflito de payload preserva registro original e produz DLQ; ack pós-commit; retry, limite, DLQ, SIGKILL e dois caminhos de SIGTERM. |
| 11 | Eventos tipados e versionados; dois publishers; crash após send e antes da marca de publicação; eventId estável e recuperação do outbox. |
| 12 | Logs JSON com identificadores, sem payload financeiro; métricas de status, duplicatas, retries, DLQ, locks, lag e latência; readiness falha quando uma fila real fica indisponível. |
| 13–14 | Unidade e integração reproduzíveis, migrations reversíveis, recuperação após reinício, documentação de execução e experimento pequeno de carga. |

## Execuções verificadas

Reexecutados neste fechamento de 15/10/2026: instalação frozen em Linux, typecheck, lint, build, verificações de imports, unidade e integração. README em português/inglês e ARCHITECTURE também foram conferidos. Compose, demonstração e carga mantêm as evidências históricas de 14–15/10/2026.

Ambiente: Docker Desktop, engine Linux 29.3.0, 12 CPUs visíveis e 8.263.618.560 bytes de memória; Bun 1.3.10, PostgreSQL 17.6 e LocalStack 4.10.0. APIs e workers usam as configurações do Compose. Os testes criam banco e prefixo de filas exclusivos e removem esses recursos ao concluir.

A integração foi repetida três vezes consecutivas, com containers novos de PostgreSQL e LocalStack e rede isolada por execução, sem volumes preexistentes. Os recursos de teste foram removidos ao final de cada rodada.

| Verificação | Resultado |
| --- | --- |
| Instalação frozen | Concluída em Linux com dependências inicialmente vazias, sem alteração do lockfile |
| Typecheck, lint e build | Aprovados no host e em Linux |
| Imports e declarações locais | tsc com noUnusedLocals e noUnusedParameters aprovado |
| Unidade em Linux | 43 testes aprovados, 0 falhas, 214 assertions;<br>TypeError/RangeError genéricos e com causa permanente não recebem retry, enquanto a causa ECONNRESET permanece transitória |
| Datas no domínio | Cinco testes de regressão verificam isolamento dos objetos Date fornecidos às factories e devolvidos pelos getters;<br>estados, versões e agendamento preservados |
| Classificação de falhas | Erro inesperado: uma tentativa SQL, HTTP 500, DLQ na primeira recepção e rollback sem efeito financeiro;<br>TypeError com causa ECONNRESET: três tentativas SQL, HTTP 503 e redelivery com um único débito;<br>throttling do ACK após commit também recupera sem duplicação |
| LOSS interno | Factory rejeita valores negativos produzidos por negate, subtract e rehydrate; zero e valores positivos continuam aceitos sem ledger |
| Integração em Linux | Três execuções consecutivas: 29 testes aprovados, 0 falhas e 2.841 assertions por execução;<br>95,56 s, 93,46 s e 93,84 s.<br>Cada rodada confirmou três processos com PIDs distintos |
| Migrations | Três aplicadas, reversão até zero e reaplicação no PostgreSQL real; existência do índice da terceira verificada |
| Deadlock real | Um conflito e uma tentativa SQL adicional; um wager, um ledger e dois eventos para a operação |
| Compose recompilado | Três APIs, dois workers, PostgreSQL e LocalStack saudáveis; migrator concluído |
| Reinício da aplicação | As três APIs e os dois workers reiniciados;<br>mesma wallet com 20.00, versão 2 e reconciliação consistente nas três portas; replay histórico preservado |
| Demonstração após restart | Assertions aprovadas: uma aposta aplicada, outra rejeitada, um débito, duas entradas totais e saldo reconstruído de 20.00 |
| Banco após carga e reinício | 20 wallets consistentes, nenhum elo quebrado no ledger, nenhum vínculo wager/ledger divergente,<br>nenhum evento não publicado e nenhum banco de teste restante |
| Documentação bilíngue | READMEs em português e inglês, com navegação recíproca e links internos válidos |

Os comandos de instalação, validação, integração, demonstração e carga estão disponíveis no [README.md](README.md).

## Custo das constraints

Evidência histórica de 14/10/2026, anterior a este fechamento; as migrations não foram alteradas. A contagem de ledger por transaction_id usa o índice adicionado na migration 003. Os planos EXPLAIN ANALYZE registrados no mesmo conjunto de 817 entradas e para a mesma transação mostram:

| Plano | Entradas selecionadas | Entradas descartadas pelo filtro | Buffers |
| --- | --- | --- | --- |
| Antes: Seq Scan | 1 | 816 | 15 hits |
| Após migration 003: Index Only Scan | 1 | 0 | 1 hit + 2 reads |

A migration adiciona somente esse índice, mantendo as constraints de soma, versão e referência. Há custo adicional de armazenamento e manutenção nas inserções. Os tempos pontuais, 0,139 ms e 0,092 ms, são amostras com cache diferente; não sustentam uma estimativa de ganho de throughput.

A soma por wallet continua proporcional ao seu histórico e ocorre duas vezes por movimento normal. Nos planos observados, a execução da reconstrução com 71 e 571 entradas consumiu 0,404 ms e 0,535 ms, incluindo joins. O planejamento, separado desses tempos, consumiu 1,143 ms e 0,170 ms. Essa amostra pequena não estabelece uma curva de capacidade.

## Carga local reproduzível

Execução: 15/10/2026, 00:00, com três APIs e dois workers. O banco tinha 2.455 entradas de ledger antes do experimento; não houve limpeza de caches. Uma wallet nova por etapa, preparação de 0/100/500 créditos e 10 requisições de aquecimento; depois 60 WINs únicos de 0.01 por etapa, concorrência 3 entre APIs, sem retry do cliente. Preparação e aquecimento ficam fora do tempo medido. Percentis por nearest rank, incluindo HTTP e leitura do JSON; lag amostrado a cada 250 ms, inclusive backlog da preparação.

| Ledger no início | RPS | p50 / p95 / p99 (ms) | Erros | Conflitos contabilizados | Pico de lag s |
| --- | --- | --- | --- | --- | --- |
| 11 | 34,45 | 81,04 / 133,63 / 162,11 | 0/60 | 0 | 0,237 |
| 111 | 58,37 | 50,09 / 64,12 / 73,09 | 0/60 | 0 | 0,196 |
| 511 | 77,10 | 37,01 / 50,17 / 51,76 | 0/60 | 0 | 0,218 |

As três wallets reconciliaram saldo, quantidade de lançamentos e versão. O outbox drenou após 0,276 s de espera final, com lag zero. Contenção que apenas espera e conclui não incrementa o contador de conflitos, que registra timeout/deadlock/serialização.

O experimento inclui HTTP, ORM, locks, cache e publicação concorrente; não isola o custo das constraints. A variação entre as etapas não demonstra ganho de RPS pelo índice nem crescimento causal da latência pelo histórico. O ganho comprovado do índice é o plano seletivo da contagem por transação. Não há meta de RPS nem extrapolação para produção.

## Limites

A soma deferred percorre o histórico duas vezes por movimento normal. Seu custo e os limites da medição estão descritos acima. O broker do Compose é efêmero: a recuperação testada mantém PostgreSQL e LocalStack e reinicia processos da aplicação. SQS gerenciado não foi utilizado.

A publicação é at-least-once; consumidores externos precisam deduplicar eventId de modo persistente. O deadlock da fixture introduz uma ordem inversa para exercitar rollback e retry; não demonstra um ciclo espontâneo da aplicação. Nenhum teste prova ausência universal de deadlocks ou de falhas, e não foi calculada cobertura percentual. Autenticação, double-entry, tracing e dashboard permanecem opcionais não implementados.
