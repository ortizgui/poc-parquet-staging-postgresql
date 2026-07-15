# Analytics da Captura de Carteira

## 1. Objetivo

Este documento descreve a estrutura analítica planejada para o fluxo de Captura de Carteira.

A solução deve permitir:

- acompanhar execuções de Landing, Harmonização e Enriquecimento;
- identificar qual produto e qual partição diária foram processados;
- medir volumes de entrada, saída, rejeição, inserção, atualização e registros sem alteração;
- consultar tempo de processamento por fluxo e por etapa;
- rastrear contratos processados;
- consultar contratos rejeitados e seus motivos;
- apoiar dashboards, war rooms, auditoria e análises operacionais;
- democratizar os dados por meio do Data Mesh;
- disponibilizar consulta via Athena e visualização via QuickSight.

A unidade de processamento diário será:

```plaintext
produto + ano + mês + dia de referência
```

Exemplo:

```plaintext
product = CARTAO
reference_date = 2026-07-14
```

O processamento pode acontecer em outra data. Por isso, devem ser mantidos separadamente:

```plaintext
reference_date
```

Data de negócio dos dados processados.

```plaintext
processing_date
```

Data em que a execução ocorreu.

---

# 2. Visão geral da arquitetura

```plaintext
Orquestrador
    |
    |-- gera execution_id
    |-- define product
    |-- define reference_date
    |-- define source partition
    |
    +--> Glue Landing
    |       |
    |       |-- processa a partição diária
    |       |-- grava saída funcional
    |       |-- grava rastreabilidade detalhada
    |       |-- grava rejeições, quando existirem
    |       +-- publica evento sintético no EventBridge
    |
    +--> Glue Harmonização
    |       |
    |       |-- valida e normaliza os dados
    |       |-- grava saída funcional
    |       |-- grava métricas por produto
    |       |-- grava rastreabilidade detalhada
    |       |-- grava rejeições
    |       +-- publica evento sintético no EventBridge
    |
    +--> Glue Enriquecimento
            |
            |-- enriquece os dados
            |-- grava saída funcional
            |-- grava métricas por produto
            |-- grava rastreabilidade detalhada
            |-- grava rejeições
            +-- publica evento sintético no EventBridge

Eventos sintéticos dos Glues
    |
    v
EventBridge
    |
    v
Lambda de observabilidade e consolidação
    |
    |-- atualiza execução geral
    |-- atualiza execução por etapa
    |-- consolida status
    |-- registra falhas técnicas
    |-- envia métricas e alertas
    +-- trata eventos duplicados de forma idempotente

Dados analíticos detalhados
    |
    v
S3 em Parquet ou Iceberg
    |
    v
Glue Data Catalog
    |
    v
Lake Formation
    |
    +--> Athena
    |
    +--> QuickSight
```

---

# 3. Princípios da solução

## 3.1 Separação de responsabilidades

O Glue será responsável por:

- processar os dados;
- calcular métricas de negócio;
- produzir datasets detalhados;
- produzir rastreabilidade;
- produzir rejeições;
- publicar eventos sintéticos.

A Lambda será responsável por:

- consolidar a execução geral;
- consolidar a execução de cada etapa;
- tratar eventos nativos do Glue;
- registrar falhas inesperadas;
- evitar duplicidade;
- emitir alertas e métricas operacionais.

O EventBridge será responsável por:

- transportar eventos sintéticos;
- transportar eventos nativos de mudança de estado dos Glue Jobs.

O S3 será responsável por:

- armazenar os data products democratizados;
- armazenar histórico analítico;
- armazenar rastreabilidade detalhada;
- armazenar rejeições.

O Athena e o QuickSight serão responsáveis por:

- consulta analítica;
- dashboards;
- investigação;
- suporte a war rooms.

---

## 3.2 Evento sintético

O evento enviado ao EventBridge deve conter apenas informações resumidas.

Ele não deve transportar:

- lista de contratos processados;
- lista de rejeições;
- payload completo de registros;
- arquivos Parquet;
- logs extensos;
- stack traces completos.

O detalhamento deve ficar nas tabelas democratizadas.

Exemplo de evento:

```json
{
  "eventVersion": "1.0",
  "eventId": "ad794041-bbd4-4854-9be2-b85ce25c3451",
  "executionId": "exec-20260714-cartao-001",
  "jobRunId": "jr_abc123",
  "jobName": "captura-carteira-harmonizacao",
  "stage": "HARMONIZATION",
  "status": "SUCCESS",
  "product": "CARTAO",
  "referenceDate": "2026-07-14",
  "processingDate": "2026-07-15",
  "startedAt": "2026-07-15T02:00:00Z",
  "finishedAt": "2026-07-15T02:08:21Z",
  "durationSeconds": 501,
  "inputRecords": 5000000,
  "outputRecords": 4920000,
  "validRecords": 4920000,
  "invalidRecords": 80000,
  "rejectedRecords": 80000,
  "duplicatedRecords": 0,
  "source": {
    "table": "carteira_origem",
    "path": "s3://bucket/source/year=2026/month=07/day=14/",
    "year": "2026",
    "month": "07",
    "day": "14"
  },
  "targets": {
    "processedContractsTable": "tb_rc8_processed_contract_event",
    "rejectedContractsTable": "tb_rc8_rejected_contract",
    "processingByProductTable": "tb_rc8_processing_by_product"
  }
}
```

---

## 3.3 Eventos nativos do Glue

Os eventos customizados não substituem os eventos nativos do Glue.

Devem ser monitorados os estados:

```plaintext
SUCCEEDED
FAILED
STOPPED
TIMEOUT
```

O evento nativo é necessário para capturar situações em que o Glue falha antes de publicar o evento analítico.

Exemplos:

- timeout;
- falta de memória;
- erro de permissão;
- falha de infraestrutura;
- interrupção manual;
- falha durante inicialização do Spark.

---

## 3.4 Idempotência

O EventBridge pode entregar o mesmo evento mais de uma vez.

A Lambda deve usar chaves lógicas e operações idempotentes.

Chaves recomendadas:

```plaintext
Execução geral:
execution_id
```

```plaintext
Execução da etapa:
execution_id + stage + attempt
```

```plaintext
Processamento por produto:
execution_id + stage + product
```

```plaintext
Rastreabilidade:
execution_id + contract_id + stage + processing_event
```

```plaintext
Rejeição:
execution_id + contract_id + stage + rejection_code
```

---

# 4. Particionamento

Todas as tabelas democratizadas devem usar:

```plaintext
year
month
day
```

Essas partições representam a data de processamento.

Exemplo:

```plaintext
s3://mesh-captura-carteira/analytics/pipeline-execution/
    year=2026/
        month=07/
            day=15/
```

A data de referência do dado continuará disponível na coluna:

```plaintext
reference_date
```

Isso permite representar reprocessamentos.

Exemplo:

```plaintext
reference_date = 2026-07-14
processing_date = 2026-07-15
```

---

# 5. Tabelas analíticas

A solução deverá criar cinco tabelas analíticas.

```plaintext
tb_rc8_pipeline_execution
tb_rc8_stage_execution
tb_rc8_processing_by_product
tb_rc8_processed_contract_event
tb_rc8_rejected_contract
```

---

# 6. Tabela `tb_rc8_pipeline_execution`

## 6.1 Objetivo

Representar a execução completa de uma partição diária de um produto, desde o Landing até o Enriquecimento.

Essa tabela será usada para responder:

- qual produto foi processado;
- qual data de referência foi processada;
- quando o processamento ocorreu;
- quanto tempo o fluxo completo demorou;
- quantos registros entraram e saíram;
- quantos foram rejeitados;
- qual foi o status final;
- se houve reprocessamento;
- qual foi a última etapa concluída.

## 6.2 Granularidade

Uma linha por:

```plaintext
execution_id
```

A unidade de negócio da execução será:

```plaintext
product + reference_date + execution_id
```

## 6.3 Campos

| Campo | Tipo sugerido | Obrigatório | Descrição |
|---|---|---:|---|
| `execution_id` | string | Sim | Identificador único da execução completa |
| `correlation_id` | string | Não | Identificador de correlação entre sistemas |
| `product` | string | Sim | Produto processado |
| `reference_date` | date | Sim | Data de negócio da partição processada |
| `processing_date` | date | Sim | Data em que o processamento ocorreu |
| `source_system` | string | Sim | Sistema ou domínio de origem |
| `source_table` | string | Não | Tabela ou data product de origem |
| `source_path` | string | Não | Path técnico de origem |
| `source_partition_year` | string | Sim | Ano da partição de origem |
| `source_partition_month` | string | Sim | Mês da partição de origem |
| `source_partition_day` | string | Sim | Dia da partição de origem |
| `status` | string | Sim | Estado geral da execução |
| `started_at` | timestamp | Sim | Início da execução completa |
| `finished_at` | timestamp | Não | Fim da execução completa |
| `duration_seconds` | bigint | Não | Duração total |
| `input_records` | bigint | Não | Registros recebidos no início |
| `output_records` | bigint | Não | Registros produzidos ao final |
| `processed_records` | bigint | Não | Registros processados com sucesso |
| `rejected_records` | bigint | Não | Registros rejeitados |
| `inserted_records` | bigint | Não | Registros inseridos |
| `updated_records` | bigint | Não | Registros atualizados |
| `unchanged_records` | bigint | Não | Registros sem alteração |
| `duplicated_records` | bigint | Não | Registros duplicados |
| `last_completed_stage` | string | Não | Última etapa concluída |
| `schema_version` | string | Não | Versão do contrato de dados |
| `final_target_table` | string | Não | Data product final publicado |
| `final_target_path` | string | Não | Path físico da saída final |
| `attempt` | int | Sim | Número do processamento ou reprocessamento |
| `original_execution_id` | string | Não | Execução original quando houver reprocessamento |
| `error_code` | string | Não | Código resumido do erro geral |
| `error_message` | string | Não | Mensagem resumida do erro |
| `created_at` | timestamp | Sim | Data de criação |
| `updated_at` | timestamp | Sim | Última atualização |
| `year` | string | Sim | Partição por ano de processamento |
| `month` | string | Sim | Partição por mês de processamento |
| `day` | string | Sim | Partição por dia de processamento |

## 6.4 Status recomendados

```plaintext
RUNNING
SUCCESS
PARTIAL_SUCCESS
FAILED
STOPPED
TIMEOUT
```

## 6.5 Alimentação

A tabela será alimentada pela Lambda de observabilidade.

Fluxo:

```plaintext
Landing concluído
    |
    +--> cria ou atualiza a execução como RUNNING

Harmonização concluída
    |
    +--> atualiza volumes e last_completed_stage

Enriquecimento concluído
    |
    +--> fecha a execução
         status = SUCCESS ou PARTIAL_SUCCESS
         finished_at = horário final
         duration_seconds = finished_at - started_at

Evento nativo de falha
    |
    +--> status = FAILED, STOPPED ou TIMEOUT
```

---

# 7. Tabela `tb_rc8_stage_execution`

## 7.1 Objetivo

Registrar o resultado de cada etapa individual do pipeline.

Etapas previstas:

```plaintext
LANDING
HARMONIZATION
ENRICHMENT
```

Essa tabela deve unir:

- informações técnicas do Glue;
- métricas de volume;
- tempo de execução;
- status;
- tentativas;
- paths de entrada e saída;
- erros técnicos e funcionais.

## 7.2 Granularidade

Uma linha por:

```plaintext
execution_id + stage + attempt
```

## 7.3 Campos

| Campo | Tipo sugerido | Obrigatório | Descrição |
|---|---|---:|---|
| `stage_execution_id` | string | Sim | Identificador único da etapa |
| `execution_id` | string | Sim | Referência à execução geral |
| `product` | string | Sim | Produto processado |
| `reference_date` | date | Sim | Data dos dados |
| `processing_date` | date | Sim | Data do processamento |
| `stage` | string | Sim | Nome da etapa |
| `attempt` | int | Sim | Número da tentativa |
| `job_name` | string | Sim | Nome do Glue Job |
| `job_run_id` | string | Sim | Identificador da execução do Glue |
| `status` | string | Sim | Status da etapa |
| `started_at` | timestamp | Sim | Início da etapa |
| `finished_at` | timestamp | Não | Fim da etapa |
| `duration_seconds` | bigint | Não | Duração da etapa |
| `input_records` | bigint | Não | Quantidade lida |
| `output_records` | bigint | Não | Quantidade produzida |
| `valid_records` | bigint | Não | Quantidade válida |
| `invalid_records` | bigint | Não | Quantidade inválida |
| `rejected_records` | bigint | Não | Quantidade rejeitada |
| `duplicated_records` | bigint | Não | Quantidade duplicada |
| `input_files` | bigint | Não | Quantidade de arquivos lidos |
| `output_files` | bigint | Não | Quantidade de arquivos gerados |
| `input_bytes` | bigint | Não | Volume lido em bytes |
| `output_bytes` | bigint | Não | Volume escrito em bytes |
| `source_table` | string | Não | Data product de origem |
| `source_path` | string | Não | Path de entrada |
| `target_table` | string | Não | Data product de saída |
| `target_path` | string | Não | Path de saída |
| `rejection_table` | string | Não | Tabela democratizada de rejeições |
| `rejection_path` | string | Não | Path de rejeições |
| `worker_type` | string | Não | Tipo de worker do Glue |
| `number_of_workers` | int | Não | Quantidade de workers |
| `glue_version` | string | Não | Versão do Glue |
| `dpu_seconds` | double | Não | Consumo aproximado de DPU |
| `timeout_seconds` | int | Não | Timeout configurado |
| `error_type` | string | Não | Tipo técnico ou funcional do erro |
| `error_code` | string | Não | Código padronizado |
| `error_message` | string | Não | Mensagem resumida |
| `created_at` | timestamp | Sim | Data de criação |
| `updated_at` | timestamp | Sim | Última atualização |
| `year` | string | Sim | Partição por ano de processamento |
| `month` | string | Sim | Partição por mês de processamento |
| `day` | string | Sim | Partição por dia de processamento |

## 7.4 Alimentação

A tabela será alimentada pela Lambda.

Fontes:

```plaintext
Evento customizado do Glue
    |
    +--> volumes
    +--> produto
    +--> data de referência
    +--> paths
    +--> contagens
    +--> status funcional

Evento nativo Glue Job State Change
    |
    +--> status técnico
    +--> falha
    +--> timeout
    +--> interrupção
```

A Lambda deverá consolidar os dois eventos usando upsert lógico.

---

# 8. Tabela `tb_rc8_processing_by_product`

## 8.1 Objetivo

Disponibilizar uma visão analítica agregada por produto e por etapa.

Essa tabela deverá ser a principal fonte de dashboards de negócio.

Ela responderá:

- quantos registros chegaram por produto;
- quantos foram processados;
- quantos foram rejeitados;
- quantos foram inseridos;
- quantos foram atualizados;
- quantos não sofreram alteração;
- qual foi a taxa de sucesso;
- qual foi a taxa de rejeição;
- quanto tempo cada produto levou para ser processado.

## 8.2 Granularidade

Uma linha por:

```plaintext
execution_id + stage + product
```

## 8.3 Campos

| Campo | Tipo sugerido | Obrigatório | Descrição |
|---|---|---:|---|
| `execution_id` | string | Sim | Identificador da execução |
| `stage` | string | Sim | Etapa que gerou a métrica |
| `product` | string | Sim | Produto |
| `reference_date` | date | Sim | Data de negócio |
| `processing_date` | date | Sim | Data do processamento |
| `received_records` | bigint | Não | Quantidade recebida |
| `processed_records` | bigint | Não | Quantidade processada |
| `valid_records` | bigint | Não | Quantidade válida |
| `invalid_records` | bigint | Não | Quantidade inválida |
| `rejected_records` | bigint | Não | Quantidade rejeitada |
| `duplicated_records` | bigint | Não | Quantidade duplicada |
| `inserted_records` | bigint | Não | Quantidade inserida |
| `updated_records` | bigint | Não | Quantidade atualizada |
| `unchanged_records` | bigint | Não | Quantidade sem alteração |
| `processing_duration_seconds` | bigint | Não | Tempo do processamento |
| `success_rate` | double | Não | Percentual de sucesso |
| `rejection_rate` | double | Não | Percentual de rejeição |
| `source_table` | string | Não | Data product de origem |
| `target_table` | string | Não | Data product gerado |
| `schema_version` | string | Não | Versão do contrato |
| `processed_at` | timestamp | Sim | Momento da geração |
| `year` | string | Sim | Partição |
| `month` | string | Sim | Partição |
| `day` | string | Sim | Partição |

## 8.4 Fórmulas recomendadas

```plaintext
success_rate =
processed_records / received_records
```

```plaintext
rejection_rate =
rejected_records / received_records
```

Deve ser tratado o caso em que `received_records = 0`.

## 8.5 Alimentação

A tabela deverá ser escrita diretamente pelo Glue.

O Glue já terá os dados distribuídos em memória e poderá realizar:

```plaintext
groupBy(product)
    |
    +--> received_records
    +--> processed_records
    +--> valid_records
    +--> invalid_records
    +--> rejected_records
    +--> inserted_records
    +--> updated_records
    +--> unchanged_records
```

A Lambda não deve recalcular essas métricas.

---

# 9. Tabela `tb_rc8_processed_contract_event`

## 9.1 Objetivo

Fornecer rastreabilidade detalhada de cada contrato processado.

Essa tabela será usada para:

- pesquisa por contrato;
- war rooms;
- auditoria;
- reconstrução do histórico;
- identificação da etapa em que o contrato passou;
- identificação do resultado da carga;
- análise de reprocessamento.

A modelagem será baseada em eventos imutáveis.

Cada nova etapa adiciona uma linha.

Não será atualizado o registro anterior.

## 9.2 Granularidade

Uma linha por:

```plaintext
execution_id + contract_id + stage + processing_event
```

## 9.3 Campos

| Campo | Tipo sugerido | Obrigatório | Descrição |
|---|---|---:|---|
| `execution_id` | string | Sim | Execução do pipeline |
| `contract_id` | string | Sim | Identificador do contrato |
| `customer_id` | string | Não | Identificador do cliente, quando permitido |
| `product` | string | Sim | Produto |
| `reference_date` | date | Sim | Data de negócio |
| `processing_date` | date | Sim | Data de processamento |
| `stage` | string | Sim | Etapa |
| `processing_event` | string | Sim | Evento ocorrido |
| `processing_status` | string | Sim | Resultado do evento |
| `processed_at` | timestamp | Sim | Momento do processamento |
| `source_system` | string | Não | Sistema de origem |
| `source_table` | string | Não | Data product de origem |
| `source_file` | string | Não | Arquivo de origem |
| `source_partition_year` | string | Não | Ano da partição de origem |
| `source_partition_month` | string | Não | Mês da partição de origem |
| `source_partition_day` | string | Não | Dia da partição de origem |
| `record_hash` | string | Não | Hash da versão processada |
| `schema_version` | string | Não | Versão do contrato |
| `target_table` | string | Não | Data product produzido |
| `target_path` | string | Não | Path de saída |
| `attempt` | int | Não | Número da tentativa |
| `year` | string | Sim | Partição por ano de processamento |
| `month` | string | Sim | Partição por mês de processamento |
| `day` | string | Sim | Partição por dia de processamento |

## 9.4 Eventos recomendados

```plaintext
RECEIVED
VALIDATED
HARMONIZED
ENRICHED
INSERTED
UPDATED
UNCHANGED
DUPLICATED
```

## 9.5 Alimentação

Cada Glue grava apenas os eventos relacionados à sua etapa.

Exemplo:

```plaintext
Glue Landing
    |
    +--> RECEIVED

Glue Harmonização
    |
    +--> VALIDATED
    +--> HARMONIZED

Glue Enriquecimento
    |
    +--> ENRICHED

Carga final
    |
    +--> INSERTED
    +--> UPDATED
    +--> UNCHANGED
```

A escrita deverá ser feita diretamente em Parquet ou Iceberg.

A Lambda não deve receber nem gravar registros individuais.

---

# 10. Tabela `tb_rc8_rejected_contract`

## 10.1 Objetivo

Registrar contratos ou registros rejeitados durante o processamento.

Essa tabela deverá permitir:

- descobrir por que um contrato foi rejeitado;
- identificar a etapa da rejeição;
- identificar a regra de qualidade;
- consultar os maiores motivos de rejeição;
- analisar rejeições por produto;
- apoiar war rooms;
- apoiar correções e reprocessamentos.

## 10.2 Granularidade

Uma linha por:

```plaintext
execution_id + contract_id + stage + rejection_code
```

Um contrato pode ter mais de uma linha caso viole mais de uma regra.

## 10.3 Campos

| Campo | Tipo sugerido | Obrigatório | Descrição |
|---|---|---:|---|
| `execution_id` | string | Sim | Execução do pipeline |
| `contract_id` | string | Sim | Identificador do contrato |
| `product` | string | Sim | Produto |
| `reference_date` | date | Sim | Data de negócio |
| `processing_date` | date | Sim | Data do processamento |
| `stage` | string | Sim | Etapa em que ocorreu a rejeição |
| `rejection_code` | string | Sim | Código padronizado |
| `rejection_category` | string | Sim | Categoria da rejeição |
| `rejection_reason` | string | Sim | Motivo resumido |
| `quality_rule` | string | Não | Regra de qualidade aplicada |
| `column_name` | string | Não | Coluna relacionada |
| `invalid_value` | string | Não | Valor inválido, somente quando permitido |
| `source_system` | string | Não | Sistema de origem |
| `source_table` | string | Não | Data product de origem |
| `source_file` | string | Não | Arquivo de origem |
| `source_partition_year` | string | Não | Ano da partição de origem |
| `source_partition_month` | string | Não | Mês da partição de origem |
| `source_partition_day` | string | Não | Dia da partição de origem |
| `rejected_at` | timestamp | Sim | Momento da rejeição |
| `schema_version` | string | Não | Versão do schema |
| `rejected_data_path` | string | Não | Path para detalhes adicionais |
| `attempt` | int | Não | Número da tentativa |
| `year` | string | Sim | Partição |
| `month` | string | Sim | Partição |
| `day` | string | Sim | Partição |

## 10.4 Categorias sugeridas

```plaintext
DATA_QUALITY
SCHEMA_VALIDATION
BUSINESS_RULE
MISSING_REQUIRED_FIELD
INVALID_FORMAT
DUPLICATE
TECHNICAL_FAILURE
```

## 10.5 Alimentação

Cada Glue separa os dados válidos dos rejeitados.

```plaintext
DataFrame de entrada
    |
    +--> registros válidos
    |
    +--> registros rejeitados
            |
            +--> adiciona rejection_code
            +--> adiciona rejection_category
            +--> adiciona quality_rule
            +--> adiciona rejected_at
            +--> grava tb_rc8_rejected_contract
```

A Lambda recebe apenas o total de rejeições.

---

# 11. Organização dos paths no S3

```plaintext
s3://mesh-captura-carteira/analytics/

    pipeline-execution/
        year=2026/
            month=07/
                day=15/

    stage-execution/
        year=2026/
            month=07/
                day=15/

    processing-by-product/
        year=2026/
            month=07/
                day=15/

    processed-contract-event/
        year=2026/
            month=07/
                day=15/

    rejected-contract/
        year=2026/
            month=07/
                day=15/
```

O path é um detalhe físico.

O contrato principal deve ser a tabela registrada no Glue Data Catalog.

Consumidores devem consultar:

```sql
SELECT *
FROM mesh_captura_carteira.tb_rc8_processed_contract_event
WHERE year = '2026'
  AND month = '07'
  AND day = '15'
  AND product = 'CARTAO';
```

Consumidores não devem depender do nome físico dos arquivos.

---

# 12. Parquet ou Iceberg

## 12.1 Parquet

Usar Parquet para tabelas append-only.

Adequado para:

```plaintext
tb_rc8_processing_by_product
tb_rc8_processed_contract_event
tb_rc8_rejected_contract
```

Vantagens:

- simples;
- performático;
- compressão eficiente;
- integração nativa com Athena;
- fácil particionamento.

## 12.2 Iceberg

Considerar Iceberg quando for necessário:

- atualizar registros existentes;
- realizar merge;
- corrigir partições;
- consultar versões anteriores;
- realizar time travel;
- manter estado atual em vez de eventos imutáveis.

Pode ser adequado para:

```plaintext
tb_rc8_pipeline_execution
tb_rc8_stage_execution
```

Caso a Lambda atualize progressivamente o mesmo registro.

Alternativa:

- manter eventos imutáveis em Parquet;
- criar views para obter o último estado.

A decisão deve considerar a padronização já adotada pela plataforma Mesh.

---

# 13. Fluxo de alimentação por componente

## 13.1 Orquestrador

Responsável por gerar e propagar:

```plaintext
execution_id
product
reference_date
processing_date
source_table
source_partition_year
source_partition_month
source_partition_day
attempt
original_execution_id
```

## 13.2 Glue Landing

Responsável por:

- ler a partição de origem;
- contar registros recebidos;
- gravar o dado de Landing;
- gerar eventos `RECEIVED`;
- gerar rejeições iniciais;
- publicar evento sintético;
- informar paths e métricas.

Tabelas alimentadas:

```plaintext
tb_rc8_processed_contract_event
tb_rc8_rejected_contract
tb_rc8_processing_by_product
```

## 13.3 Glue Harmonização

Responsável por:

- validar schema;
- normalizar dados;
- aplicar regras de qualidade;
- separar válidos e inválidos;
- gerar eventos `VALIDATED` e `HARMONIZED`;
- gerar métricas por produto;
- publicar evento sintético.

Tabelas alimentadas:

```plaintext
tb_rc8_processed_contract_event
tb_rc8_rejected_contract
tb_rc8_processing_by_product
```

## 13.4 Glue Enriquecimento

Responsável por:

- enriquecer os dados;
- aplicar regras de produto;
- gerar eventos `ENRICHED`;
- contabilizar saída final;
- gerar métricas por produto;
- publicar evento sintético.

Tabelas alimentadas:

```plaintext
tb_rc8_processed_contract_event
tb_rc8_rejected_contract
tb_rc8_processing_by_product
```

## 13.5 Lambda de observabilidade

Responsável por:

- receber eventos customizados;
- receber eventos nativos do Glue;
- validar versão do evento;
- garantir idempotência;
- consultar `GetJobRun` quando necessário;
- atualizar execução geral;
- atualizar execução por etapa;
- consolidar status;
- registrar falhas técnicas;
- publicar métricas no CloudWatch;
- enviar alertas.

Tabelas alimentadas:

```plaintext
tb_rc8_pipeline_execution
tb_rc8_stage_execution
```

---

# 14. Views sugeridas para Athena e QuickSight

## 14.1 `vw_rc8_daily_processing`

Objetivo:

- mostrar volumes processados por dia;
- consolidar produto e status;
- acompanhar tendência.

## 14.2 `vw_rc8_processing_by_product`

Objetivo:

- mostrar recebidos;
- processados;
- rejeitados;
- inseridos;
- atualizados;
- sem alteração;
- taxa de sucesso;
- taxa de rejeição.

## 14.3 `vw_rc8_processing_sla`

Objetivo:

- duração média;
- duração máxima;
- duração por etapa;
- comparação com SLA.

## 14.4 `vw_rc8_rejection_analysis`

Objetivo:

- rejeições por produto;
- rejeições por regra;
- rejeições por categoria;
- maiores causas;
- evolução diária.

## 14.5 `vw_rc8_contract_history`

Objetivo:

- histórico completo de um contrato;
- etapas percorridas;
- eventos;
- horários;
- tentativas.

## 14.6 `vw_rc8_contract_latest_status`

Objetivo:

- obter o último estado conhecido de cada contrato;
- facilitar pesquisa em war rooms.

## 14.7 `vw_rc8_latest_product_execution`

Objetivo:

- mostrar a última execução por produto e data de referência;
- identificar partições não processadas.

---

# 15. Dashboards sugeridos

## 15.1 Dashboard executivo

Indicadores:

- quantidade processada por produto;
- quantidade rejeitada;
- taxa de sucesso;
- tempo total;
- status da última execução;
- comparação diária e semanal.

## 15.2 Dashboard operacional

Indicadores:

- duração por etapa;
- jobs com falha;
- tentativas;
- volume de entrada e saída;
- perda de registros entre etapas;
- consumo de DPU;
- tempo desde o último sucesso.

## 15.3 Dashboard de qualidade

Indicadores:

- top regras de rejeição;
- produtos com maior taxa de rejeição;
- colunas com maior quantidade de erro;
- evolução de rejeições;
- falhas de schema.

## 15.4 Pesquisa de contrato

Filtros:

```plaintext
contract_id
product
reference_date
execution_id
```

Retorno:

- entrada no Landing;
- validação;
- harmonização;
- enriquecimento;
- resultado final;
- rejeição, quando houver.

---

# 16. Governança e segurança

Deve ser utilizado Lake Formation para controlar:

- acesso por tabela;
- acesso por coluna;
- acesso por domínio;
- acesso por perfil;
- acesso a dados sensíveis.

Recomendações:

- evitar dados pessoais quando não forem necessários;
- mascarar ou hashear identificadores sensíveis;
- não armazenar payload completo em tabelas de observabilidade;
- restringir `invalid_value`;
- definir política de retenção;
- definir owner de cada data product;
- versionar os contratos;
- manter documentação no catálogo.

---

# 17. Retenção

Sugestão inicial:

| Tipo de dado | Retenção sugerida |
|---|---:|
| Execução geral | 3 a 5 anos |
| Execução por etapa | 1 a 3 anos |
| Métricas por produto | 3 a 5 anos |
| Rastreabilidade por contrato | Conforme auditoria e negócio |
| Rejeições | 90 dias a 1 ano |
| Logs técnicos | 30 a 90 dias |

A retenção final deve ser definida com:

- negócio;
- governança;
- segurança;
- jurídico;
- compliance.

---

# 18. Observabilidade e alertas

Alertas recomendados:

```plaintext
Execução FAILED
Execução TIMEOUT
Execução STOPPED
Ausência de processamento esperado
Duração acima do SLA
Taxa de rejeição acima do limite
Diferença anormal entre entrada e saída
Evento customizado não recebido
Partição democratizada não publicada
Falha na Lambda de consolidação
Falha na escrita do data product
```

A Lambda deverá publicar métricas customizadas no CloudWatch.

Exemplos:

```plaintext
PipelineExecutionSuccess
PipelineExecutionFailure
PipelineDurationSeconds
StageDurationSeconds
InputRecords
OutputRecords
RejectedRecords
RejectionRate
MissingAnalyticsEvent
```

---

# 19. Reconciliação

Deve existir uma rotina de reconciliação.

Objetivo:

- localizar Glue Jobs finalizados sem registro analítico;
- localizar eventos customizados ausentes;
- localizar partições não catalogadas;
- localizar divergência entre dados detalhados e métricas;
- corrigir falhas de processamento do EventBridge ou Lambda.

Fluxo sugerido:

```plaintext
Job de reconciliação
    |
    +--> consulta Glue Job Runs
    +--> consulta tb_rc8_stage_execution
    +--> compara execuções
    +--> identifica ausências
    +--> reprocessa eventos ou atualiza registros
```

---

# 20. Decisões finais

## Tabelas que devem ser criadas

```plaintext
tb_rc8_pipeline_execution
tb_rc8_stage_execution
tb_rc8_processing_by_product
tb_rc8_processed_contract_event
tb_rc8_rejected_contract
```

## Alimentação pela Lambda

```plaintext
tb_rc8_pipeline_execution
tb_rc8_stage_execution
```

## Alimentação pelos Glues

```plaintext
tb_rc8_processing_by_product
tb_rc8_processed_contract_event
tb_rc8_rejected_contract
```

## Consulta

```plaintext
S3
    |
Glue Data Catalog
    |
Lake Formation
    |
Athena
    |
QuickSight
```

## Particionamento

```plaintext
year
month
day
```

## Unidade lógica de processamento

```plaintext
product + reference_date + execution_id
```

## Separação de datas

```plaintext
reference_date
    Data dos dados processados

processing_date
    Data da execução
```

## Regra principal do evento

```plaintext
Evento = resumo
Tabela democratizada = detalhe
```

## Regra principal da Lambda

```plaintext
Lambda consolida
Lambda não processa milhões de registros
```

## Regra principal do Glue

```plaintext
Glue calcula e grava os dados analíticos detalhados
```
