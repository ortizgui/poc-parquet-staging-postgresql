# Prompt — Produtor Parquet: `row_group_size` configurável e validado

> **Para um agente de IA.** Este documento é auto-contido: descreve o problema, o contexto, os
> requisitos, as restrições, a implementação de referência, os critérios de aceite, os anti-padrões
> e a receita de teste. Execute a tarefa **no produtor de Parquet** (o serviço/job que escreve os
> arquivos que a POC consome). Não mexa no leitor (isso é o outro prompt:
> [`consumer-leitura-row-groups-dotnet.md`](consumer-leitura-row-groups-dotnet.md)).

---

## 1. Problema

Arquivos Parquet gerados sem controle de `row_group_size` podem sair com **um único row group
gigante**. O consumidor então não consegue paginar: o piso de memória vira o row group inteiro e o
pod estoura — **mesmo que o arquivo não seja maior que a RAM**.

Medido nesta POC (limite de 192 MB, 1.160.000 linhas, 40 colunas):

| Arquivo | Row groups | Maior RG (descomprimido) | Resultado no consumidor |
|---|---|---|---|
| `ab_1160000_20000rg.parquet` | **58** | **32,5 MiB** | `ok`, pico **109,4 MiB**, +1.073.909 ins, 0 err |
| `ab_1160000_1rg.parquet` | **1** | **1.803,6 MiB** | falha por memória, 0 linhas |

Evidência: [`../evidencias/`](../evidencias/README.md) ·
[`../memory-test-results.json`](../memory-test-results.json).

## 2. Contexto

- Parquet **não tem índice de linha**. A menor unidade endereçável é o **row group**, e quem define o
  tamanho dele é o **writer** — não o leitor.
- O leitor (`Parquet.Net`) consegue projetar colunas, mas **não consegue fatiar abaixo de um row
  group**: `ReadColumnAsync` devolve a coluna inteira do row group.
- Consequência: a memória do consumidor é `O(maior row group × colunas lidas) + O(lote de flush)`,
  **não** `O(arquivo)`.
- Portanto, `row_group_size` é uma **decisão de quem escreve**, e precisa ser um parâmetro
  configurável com default seguro e validação pós-escrita.

Referências internas: [`../../scripts/generate_large_parquet.py`](../../scripts/generate_large_parquet.py)
(gerador com `--row-group-rows`) e [`../aws-producao-ecs-fargate.md`](../aws-producao-ecs-fargate.md) §5.

## 3. Requisitos

1. **Parametrizar o tamanho do row group** por:
   - `row_group_size` em **linhas** (ex.: `20_000`), e/ou
   - **bytes descomprimidos alvo** por row group (quando o produtor permitir).
2. Expor o parâmetro via **config/env/CLI**, com **default seguro** (`20_000` linhas é o default da
   POC) e **documentação** clara do knob.
3. **Dimensionar** para folga:
   ```
   largest_rg_uncompressed_bytes × multiplicador(.NET 2–4×) + baseline_runtime < pod_limit_bytes
   ```
   com **~50% de headroom**. Metas práticas:
   - **10–20 row groups** para ~200 mil linhas (schema largo);
   - row group de **32–128 MB descomprimidos** (ou ~**20 mil–1 milhão** de linhas) é o ponto doce;
   - para um arquivo de ~1 GB, ficar entre **~30 e ~120 row groups**.
4. **Validar pós-escrita (obrigatório)** e **falhar/avisar o pipeline** se o orçamento for violado.
5. **Não super-dividir** (ver §6).
6. Escolher compressão/dicionário conscientemente (§7).

## 4. Exemplos concretos por engine

### 4.1 pyarrow (batch único)

```python
import pyarrow.parquet as pq

pq.write_table(
    table,
    "saida.parquet",
    row_group_size=20_000,   # linhas por row group (knob principal)
    compression="snappy",
    use_dictionary=True,
)
```

### 4.2 pyarrow (escrita incremental por chunk)

```python
import pyarrow as pa
import pyarrow.parquet as pq

writer = pq.ParquetWriter("saida.parquet", schema, compression="snappy")
for chunk in chunks:                      # cada chunk com row_group_size linhas
    writer.write_table(chunk, row_group_size=20_000)
writer.close()                            # fecha o footer
```

### 4.3 parquet-mr / Spark

```properties
# ATENÇÃO: em parquet-mr/Spark o parâmetro é em BYTES (não linhas).
parquet.block.size=134217728          # default ~128 MB
```

```python
# Spark
spark.conf.set("spark.sql.parquet.block.size", str(64 * 1024 * 1024))  # 64 MB
```

> Regra de bolso: `rows_per_rg ≈ target_bytes / bytes_por_linha_descomprimidos`.
> Meça `bytes_por_linha = total_byte_size / num_rows` no metadata e ajuste.

## 5. Validação pós-escrita (obrigatória)

Depois de escrever, **leia o metadata de volta** e valide:

```python
import sys
import pyarrow.parquet as pq

MULTIPLIER   = 3.0          # .NET materializa ~2–4× o RG descomprimido
POD_LIMIT_MB = 512.0        # limite de memória do pod consumidor
HEADROOM     = 0.50         # exigimos 50% de folga
MIN_RGS      = 10           # mínimo de row groups para permitir paginação


def validar(path: str) -> None:
    md = pq.ParquetFile(path).metadata
    sizes = [md.row_group(i).total_byte_size for i in range(md.num_row_groups)]
    largest_mb = max(sizes) / 1024 / 1024
    estimativa = largest_mb * MULTIPLIER
    teto = POD_LIMIT_MB * (1 - HEADROOM)

    print(f"arquivo          : {path}")
    print(f"linhas           : {md.num_rows:,}")
    print(f"row groups       : {md.num_row_groups}")
    print(f"min/avg/max RG   : {min(sizes)/1048576:.1f} / "
          f"{sum(sizes)/len(sizes)/1048576:.1f} / {largest_mb:.1f} MiB")
    print(f"estimativa .NET  : {estimativa:.1f} MiB (teto {teto:.1f} MiB)")

    problemas = []
    if md.num_row_groups < MIN_RGS:
        problemas.append(f"num_row_groups={md.num_row_groups} < {MIN_RGS}")
    if estimativa >= teto:
        problemas.append(f"maior RG {largest_mb:.1f} MiB × {MULTIPLIER} >= teto {teto:.1f} MiB")
    if problemas:
        raise SystemExit("BUDGET DE ROW GROUP VIOLADO: " + "; ".join(problemas))


validar(sys.argv[1])
```

No pipeline, isso deve rodar **antes** de publicar o arquivo e **quebrar o job** (ou ao menos emitir
alerta bloqueante) quando o orçamento estourar. Logar o resumo (rows, RG count, min/avg/max) é parte
do contrato.

Exemplo de wrapper CLI (`--row-group-rows`):

```python
import argparse
p = argparse.ArgumentParser()
p.add_argument("--row-group-rows", type=int, default=20_000,
               help="linhas por row group (default: 20000)")
p.add_argument("--validate-budget", action="store_true",
               help="valida metadata e falha se o maior RG exceder o orçamento")
# ... gera com row_group_size=args.row_group_rows ...
```

Referência real: [`../../scripts/generate_large_parquet.py`](../../scripts/generate_large_parquet.py) expõe
`--row-group-rows` e, ao final, lê os metadados e imprime o **maior row group descomprimido** e a
estimativa `×3`.

## 6. Não super-dividir (o outro extremo)

Row groups pequenos demais são tão ruins quanto um gigante:

| Efeito | Por quê |
|---|---|
| Footer gigante | Cada RG carrega metadata própria (header de column chunk **por coluna**, dicionário, páginas); o footer vira um índice enorme. |
| Compressão pior | Dicionário e RLE operam **dentro** do row group; menos linhas = pior taxa. |
| Mais requisições ao S3 | S3 Range GET é **uma requisição por faixa** (não suporta múltiplos ranges num GET); mais RGs = mais `Seek`/`Read` = mais GETs e overhead. |

> **Nunca** use row groups de ~100 linhas num arquivo de 1 GB. O ponto doce é dezenas de row groups
> com dezenas de MB cada, não milhares.

## 7. Casos de borda

- **Arquivo pequeno que cabe inteiro:** 1 row group é aceitável — desde que
  `largest_rg × multiplicador + baseline < limite do pod`. A validação não deve falhar só por ter 1 RG
  se o orçamento couber; ajuste `MIN_RGS` ao caso (ou condicione a `num_rows`).
- **Appends / múltiplos lotes de escrita:** o writer emite **um row group por `write_table`** (ou
  agrupa conforme a engine). Se o job escreve em lotes, garanta que o tamanho do lote ≈ `row_group_size`;
  valide no fim (não confie no meio).
- **Compressão:** `snappy` é um default equilibrado; `zstd` comprime mais e descomprime bem; `gzip`
  costuma ser mais lento. A escolha muda o tamanho **em disco**, não o descomprimido (que é o que
  pesa na memória do leitor).
- **Dicionário:** `use_dictionary=True` ajuda colunas de baixa cardinalidade; colunas de texto
  aleatório/alta cardinalidade ficam maiores. O consumidor projeta colunas — as não lidas não pesam.
- **Multipart upload / ETag:** o `ETag` de objetos multipart tem sufixo `-N` (nº de partes). Isso é
  irrelevante para o tamanho do row group, mas o leitor usa `If-Match` com esse ETag; **não**
  sobrescreva o objeto durante a ingestão.

## 8. Critérios de aceite

- [ ] Um arquivo de **200k linhas × 40 colunas** gera **≥ 10 row groups**, com
      `maior_RG × 3 < limite_do_pod`.
- [ ] O parâmetro (linhas e/ou bytes) existe, tem default documentado e é ajustável sem editar código.
- [ ] A validação pós-escrita roda no pipeline e **falha** quando o orçamento é violado.
- [ ] O log/resumo mostra `num_rows`, `num_row_groups`, `min/avg/max RG bytes` e a estimativa.
- [ ] Testes cobrem: arquivo grande (muitos RGs), arquivo pequeno (1 RG que cabe) e um caso
      deliberadamente grande (1 RG estourando → validação falha).

## 9. Anti-padrões

| Anti-padrão | Por que é ruim | Faça |
|---|---|---|
| Deixar o default do engine (ex.: 128 MB) sem medir o pod | Pode estourar um pod pequeno | Parametrize e valide contra o limite real |
| 1 row group gigante | Leitor não pagina; OOM garantido | ≥ 10–20 RGs por arquivo grande |
| Milhares de RGs minúsculos | Footer enorme, compressão ruim, muitos GETs | Dezenas de RGs de 32–128 MB |
| `row_group_size` hard-coded | Não acompanha mudança de schema/pod | Config/env/CLI com default |
| Validar só "o arquivo abriu" | Não detecta o orçamento estourado | Ler metadata e comparar com o teto |
| Confundir bytes comprimidos com descomprimidos | O pico é sobre o **descomprimido** | Use `total_byte_size` do metadata |

## 10. Troubleshooting

| Sintoma | Causa provável | Ação |
|---|---|---|
| Consumidor OOM com arquivo "pequeno" | 1 row group grande / colunas lidas dominantes | Reduza `row_group_size`; confira o maior RG no metadata |
| Footer grande / arquivo maior que o dado | RGs pequenos demais + dicionário | Aumente o RG |
| Muitos Range GETs no S3 | RGs pequenos demais | Aumente o RG (menos seeks) |
| `row_group_size` ignorado | Engine usa bytes (`parquet.block.size`) | Use o parâmetro correto da engine |
| Validação passou mas consumiu demais | Multiplicador subestimado | Meça o pico real e calibre (2–4×) |

## 11. Como usar este prompt

1. Localize o **writer** de Parquet do seu produtor (pyarrow / Spark / parquet-mr / outra lib).
2. Adicione o knob `row_group_size` (e/ou bytes) com default `20_000` linhas, configurável.
3. Adicione a **validação pós-escrita** do §5 ao pipeline, com falha bloqueante.
4. Rode a receita de teste abaixo e anexe o resumo de metadata ao PR.

## 12. Receita de teste

```bash
# Gera o cenário positivo (muitos RGs) e confere o maior row group
.venv/bin/python3 scripts/generate_large_parquet.py --rows 1160000 \
    --row-group-rows 20000 --columns 40 --output data/ab_1160000_20000rg.parquet

# Lê o metadata de volta (validação do §5)
.venv/bin/python3 -c "
import pyarrow.parquet as pq
md = pq.ParquetFile('data/ab_1160000_20000rg.parquet').metadata
sz = [md.row_group(i).total_byte_size for i in range(md.num_row_groups)]
print('RGs:', md.num_row_groups, '| maior MiB:', round(max(sz)/1048576, 1))
"
```

Confirmação esperada (medida nesta POC): **58 row groups**, maior RG **32,5 MiB**, estimativa ×3
**97,4 MiB** — detalhe em [`../evidencias/ab-parquet-metadata.txt`](../evidencias/ab-parquet-metadata.txt).

O consumidor deve então ingerir com pico **109,4 MiB** sob limite de 192 MB. Para o A/B completo
(muitos RGs vs 1 RG gigante) use [`../../scripts/test_rowgroup_ab.sh`](../../scripts/test_rowgroup_ab.sh).

## 13. Referências

- [`../aws-producao-ecs-fargate.md`](../aws-producao-ecs-fargate.md) — dimensionamento, modos de leitura, ECS.
- [`../replicar-testes-e-metricas.md`](../replicar-testes-e-metricas.md) — como medir e reproduzir.
- [`../memory-test-results.json`](../memory-test-results.json) — resultados medidos.
- [`../evidencias/README.md`](../evidencias/README.md) — evidência bruta versionada do A/B.
- [`consumer-leitura-row-groups-dotnet.md`](consumer-leitura-row-groups-dotnet.md) — o prompt do consumidor.
