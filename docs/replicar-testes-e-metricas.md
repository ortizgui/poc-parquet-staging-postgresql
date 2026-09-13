# Replicar os testes e medir — guia único

Este documento fecha o ciclo: **subir → gerar o arquivo → rodar o teste → acompanhar a memória →
ler as métricas → exportar a evidência → repetir em outra máquina.**

Ele existe porque a prova desta POC não é uma afirmação, é um número. E número que não pode ser
reproduzido por outra pessoa não é prova.

> Para o desenho da solução em produção, veja [`aws-producao-ecs-fargate.md`](aws-producao-ecs-fargate.md).
> Aqui é só o **como rodar e medir**.

---

## 1. Pré-requisitos

| Item | Por quê |
|---|---|
| Docker + Docker Compose v2 | sobe a stack inteira (PostgreSQL, S3 emulado, worker, observabilidade) |
| Python 3.10+ com `boto3`, `pyarrow`, `numpy` | scripts de infra, geração de arquivo e coleta |
| `curl` | consultas ao Prometheus (o `collect_metrics.py` é stdlib-only e não precisa) |
| ~4 GB de disco livre | o arquivo de teste de 1 GB + o volume do PostgreSQL |

```bash
pip install -r requirements.txt
```

---

## 2. Passo a passo — o ciclo completo

```bash
# 1. Sobe a stack (PostgreSQL + S3 emulado + worker + Prometheus + Grafana + cadvisor)
docker compose up -d

# 2. Cria bucket, fila, DLQ e a notificação de evento S3 -> SQS
python3 scripts/setup_infra.py

# 3. Gera o arquivo de teste e sobe para o S3
#    ~1 GB, schema largo (40 colunas), row groups de 20k linhas
python3 scripts/generate_large_parquet.py \
    --target-size-mb 1024 --row-group-rows 20000 --columns 40 \
    --output data/large_1gb.parquet --upload

# 4. Roda o teste: dispara a notificação e acompanha memória + contagens
./scripts/run_memory_test.sh --key input/large_1gb.parquet --limit-mb 512

# 5. Lê as métricas da mesma fonte que o Grafana usa
./scripts/collect_metrics.py --minutes 5 --out reports/metrics.json

# 6. Exporta a evidência (gráfico da curva)
python3 scripts/plot_memory_test.py --csv reports/memory_test_<stamp>.csv
```

Os artefatos saem em `reports/`: o `.log`, o `.csv` (memória × tempo) e o `.worker.log`.

### Os arquivos de teste — gerados, não versionados

**Não existe parquet de teste no git, de propósito.** O cenário precisa de ~1 GB, e binário desse
tamanho no repositório é inviável. O que é versionado é o **gerador** + o script do A/B, que
reproduzem os arquivos de forma determinística (`--seed 42`) em qualquer máquina.

O controle é a flag **`--row-group-rows`** — é ela, não o tamanho do arquivo, que define o piso de
memória do leitor:

```bash
# N row groups (row groups de 20k linhas) — o cenário que deve CONCLUIR
python3 scripts/generate_large_parquet.py --rows 1160000 --row-group-rows 20000 \
    --columns 40 --output data/ab_muitos_rg.parquet --upload

# 1 row group único com todas as linhas — o cenário que deve ESTOURAR
# (--row-group-rows == --rows faz o writer emitir um único row group)
python3 scripts/generate_large_parquet.py --rows 1160000 --row-group-rows 1160000 \
    --columns 40 --output data/ab_um_rg.parquet --upload
```

Verificado (mesmo total de linhas, 40k, 40 colunas — o mecanismo em miniatura):

- `--row-group-rows 20000` → **2 row groups**, maior descomprimido **32,5 MB**, 35,89 MB em disco
- `--row-group-rows 40000` → **1 row group**, maior descomprimido **63,9 MB**, 34,90 MB em disco

O arquivo é o mesmo; o piso de memória do leitor dobra. É exatamente isso que o A/B mede.

Para rodar o A/B inteiro (gera os dois, sobe e testa, com asserção):

```bash
./scripts/test_rowgroup_ab.sh --limit-mb 192

# smoke rápido, valida a mecânica em ~5s sem gerar 1 GB
./scripts/test_rowgroup_ab.sh --rows 40000 --limit-mb 64
```

> Os arquivos gerados caem em `data/`, que está no `.gitignore` (`data/input/*.parquet`) — confirme
> que o seu padrão cobre o caminho que você usar.

---

## 3. Escolhendo o limite de memória

O limite é o eixo do teste e é **env var — não precisa editar arquivo nenhum**:

```bash
CONSUMER_MEM_LIMIT=512m docker compose up -d consumer
CONSUMER_MEM_LIMIT=128m docker compose up -d consumer   # demonstra o OOMKilled
CONSUMER_MEM_LIMIT=1g   docker compose up -d consumer
```

O compose também expõe os knobs do worker, todos com default seguro:

| Env var | Default | Para que serve |
|---|---|---|
| `CONSUMER_MEM_LIMIT` | `512m` | limite de memória do pod (memswap igual, de propósito — ver §5) |
| `CONSUMER_READ_MODE` | `S3Range` | `S3Range` (lê o footer e busca só o necessário) ou `LocalFile` |
| `CONSUMER_PIN_OBJECT_VERSION` | `true` | envia `If-Match` com o ETag em toda requisição Range |
| `CONSUMER_RANGE_TRACE_LOG` | `false` | liga o log de cada Range GET (offset/tamanho) |

### Valores medidos (arquivo de 1.040,8 MB = 2,03× o limite de 512 MB)

| Limite | Modo | Pico | Resultado |
|---|---|---|---|
| 512 MB | `S3Range` | 108–132 MiB (21–26%) | ok · 0 erros · 29,6 MB transferidos (2,8%) em 120 requisições |
| 512 MB | `LocalFile` | ~100 MiB (19%) | ok · 0 erros · 1.040,8 MB (100%) em 1 requisição |
| 128 MB | `S3Range` | 108,5 MiB | ok · 0 erros · 1 entrega |
| 96 MB | `S3Range` | 93,6 MiB | ok · 0 erros · 1 entrega |
| 80 MB | `S3Range` | 76,1 MiB | **OOMKilled** (exit 137) após 247.779 linhas — **sem exceção no log** |

**O piso não é uma constante do runtime — é o row group.** O worker *inicia* até com 32 MB (ocioso em
~30 MiB); o que não cabe é o processamento. Para este arquivo (20k linhas por row group, 32,5 MB
descomprimidos) a fronteira fica entre 80 e 96 MB. Note que o pico **se adapta ao limite**: 108 MiB
com 128 MB e 93 MiB com 96 MB — o GC é ciente do cgroup e aperta a coleta sob pressão. Para baixar o
piso, reduza `--row-group-rows` no gerador; apertar o limite do pod não faz um row group grande caber.

Com o arquivo de 358,9 MB (20 row groups, 5 de 40 colunas): `S3Range` 95 MiB / 10,4 MB (2,9%) /
44 requisições — `LocalFile` 91 MiB / 358,9 MB (100%) / 1.

---

## 4. Acompanhando o consumo de memória

São **três fontes**, e elas respondem perguntas diferentes. Use as três.

### 4.1 `docker stats` — o que decide o `OOMKilled`

É o que o `run_memory_test.sh` já faz, a cada 2 s, gravando no CSV:

```bash
docker stats --no-stream --format '{{.MemUsage}}' poc-consumer
```

É a **mesma contagem do cgroup** que o kernel usa para matar o container. Se você só puder olhar
uma coisa, olhe esta.

### 4.2 Grafana — a curva e os 4 golden signals

```bash
open http://localhost:3000     # admin/admin
```

O dashboard **está provisionado como código** (`observability/grafana/dashboards/ingestion-memory.json`):
`docker compose up` já o deixa pronto, sem nenhum clique. Datasource também provisionado.

O que olhar, em ordem:

1. **Memória do pod vs. limite** — a linha tem de ficar **chapada**. Se ela acompanha o tamanho do
   arquivo em vez de ficar plana, a paginação não está funcionando.
2. **Traffic** — `poc_parquet_bytes_downloaded_total` contra `poc_ingest_last_file_bytes`. Numa
   leitura parcial isso fica em ~3% do objeto. Se subir para ~100%, a projeção quebrou ou o
   `ReadMode` virou `LocalFile`.
3. **Latency** — `poc_parquet_rowgroup_read_seconds` e `poc_db_upsert_seconds`.
4. **Errors** — `poc_ingest_invalid_records_total` e `poc_sqs_dlq_depth`. DLQ maior que zero é
   alarme.

### 4.3 Prometheus direto — números para levar para fora

O `collect_metrics.py` consulta o Prometheus e imprime um resumo + JSON:

```bash
./scripts/collect_metrics.py --minutes 5 --out reports/metrics.json
```

Ele lê **as mesmas séries do Grafana**, então o número que ele imprime é o número do painel — útil
para colar em PR, issue ou relatório sem screenshot. Também dá o veredito de tráfego
("2,8% do objeto — leitura parcial funcionando").

### Por que o cadvisor importa

`process_working_set_bytes` é o que o **processo** enxerga. Quem decide o `OOMKilled` é o
`container_memory_working_set_bytes`, do **pod**. Medir só o heap gerenciado do .NET dá
**falso negativo**: você vê 30 MB no heap enquanto o container é morto por 512 MB de uso real
(buffer nativo, arena do GC, sockets).

> **Limitação conhecida em Docker-in-Docker:** o cadvisor pode não enumerar os cgroups aninhados do
> dind, e aí a série de pod fica ausente. O `collect_metrics.py` detecta isso e avisa, orientando a
> usar o `docker stats` como fonte do pico. Em Docker normal (host) ele funciona.

---

## 5. Lendo a curva de memória sem falso negativo

Três armadilhas que já geraram conclusão errada neste projeto:

1. **`memswap_limit` igual ao `mem_limit`.** Com swap disponível o container sobrevive à pressão de
   memória e o teste "passa" sem provar nada. Está igual de propósito no compose — não mexa.
2. **Amostrar o processo em vez do container.** Ver §4 — heap gerenciado não é o número que mata o pod.
3. **Olhar o pico antes do fim.** O pico real costuma ser no **último** row group, não no primeiro.
   Aguarde o `Result:` no log; o runner já espera.

---

## 6. A/B: `S3Range` vs `LocalFile`

O mesmo arquivo, os dois modos, sem editar nada:

```bash
CONSUMER_READ_MODE=S3Range   ./scripts/run_memory_test.sh --key input/large_1gb.parquet
./scripts/collect_metrics.py --out reports/metrics_s3range.json

CONSUMER_READ_MODE=LocalFile ./scripts/run_memory_test.sh --key input/large_1gb.parquet
./scripts/collect_metrics.py --out reports/metrics_localfile.json
```

Compare o que importa: **pico de memória** (deve ficar na mesma faixa) e **`poc_parquet_bytes_downloaded_total`**
(deve cair para a fração das colunas projetadas no modo `S3Range`).

> Um aviso que já custou tempo: a env var fica no ambiente do shell. Se você exportar
> `CONSUMER_READ_MODE=LocalFile` num terminal e depois rodar em outro, pode pegar um teste no modo
> errado sem perceber. Confira no log: a linha `Origem S3Range para …` (ou `Origem LocalFile para …`)
> diz qual modo rodou de verdade.

---

## 7. Exportando a evidência

```bash
python3 scripts/plot_memory_test.py --csv reports/memory_test_<stamp>.csv
```

Gera o gráfico da curva a partir das amostras. Para um relatório mais completo (tabelas + gráfico),
`scripts/generate_report.py`. As evidências versionadas desta POC estão em `docs/assets/` e o resumo
numérico em `docs/memory-test-results.json`.

---

## 8. Troubleshooting

| Sintoma | Causa provável | O que fazer |
|---|---|---|
| A contagem final veio **maior** que o esperado | Fila contaminada: mensagem residual entregue junto com o disparo | O runner já drena e espera a propagação do `purge`. Se usou `--skip-purge`, não use para evidência. |
| Resultado **`timeout`** com o container morto | `OOMKilled` **sem exceção no log** — medido: 0 ocorrências de `OutOfMemoryException` em todos os limites | O runner detecta via `State.OOMKilled` (não pela exceção). Confira `docker inspect poc-consumer --format '{{.State.OOMKilled}}'` — deve dar `true` com `exit 137`. |
| Mensagem foi para a **DLQ** sem erro aparente | Ingestão mais longa que o visibility timeout | Confira a regra do heartbeat no README; o worker deriva o valor e nunca aceita heartbeat ≥ timeout. |
| `docker compose up` falha no **localstack** | A tag `latest` passou a exigir license token (sai com código 55) | A POC usa `ministack` (MIT, drop-in na mesma porta). Ver comentário no `docker-compose.yml`. |
| Painel do Grafana vazio | Prometheus sem alvo, ou a stack subiu antes do worker | `docker compose logs prometheus`; alvo é `consumer:9464`. |
| Pico do pod aparece **`n/d`** | cadvisor sem cgroups aninhados (dind) | Use `docker stats`. Ver §4. |

---

## 9. Replicar em outra máquina — checklist

- [ ] `docker compose up -d` sobe **7 serviços** e o Grafana responde em `:3000`
- [ ] `python3 scripts/setup_infra.py` cria bucket + fila + DLQ + notificação S3
- [ ] `pip install -r requirements.txt` (boto3, pyarrow, numpy)
- [ ] O arquivo de teste é gerado **ou** baixado — não precisa ser criado do zero
- [ ] `./scripts/run_memory_test.sh` termina com `resultado: ok` e `entregas: 1`
- [ ] `./scripts/collect_metrics.py` mostra o veredito de tráfego em ~3% (leitura parcial ativa)
- [ ] O dashboard do Grafana abre preenchido, sem configuração manual
- [ ] Liberar portas: `3000` (Grafana), `9090` (Prometheus), `9464` (métricas do worker), `4566` (S3/SQS emulado), `5432` (PostgreSQL)

Se algo disso falhar, a seção §8 tem a causa mais comum de cada sintoma.
