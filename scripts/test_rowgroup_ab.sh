#!/usr/bin/env bash
# =============================================================================
# A/B DECISIVO: paginacao por row group
#
# Prova o ponto central da POC: com o MESMO limite de memoria e dois arquivos do
# MESMO tamanho, o que decide o pico NAO e o tamanho do arquivo — e o maior row group.
#
#   Cenario MUITOS_RG : N row groups   -> deve concluir, com pico chapado
#   Cenario UM_RG     : 1 row group    -> deve estourar o mesmo limite (OOMKilled)
#
# Este script FALHA (exit != 0) se o comportamento nao for esse. Um teste que so
# imprime numeros nao garante nada; este tem asercao.
#
# -----------------------------------------------------------------------------
# OS ARQUIVOS DE TESTE NAO SAO VERSIONADOS — SÃO GERADOS
#
# Nao existe parquet de teste no git, de proposito: o cenario precisa de ~1 GB, e
# binario desse tamanho no repositorio e inviavel. O que e versionado e o GERADOR
# (scripts/generate_large_parquet.py) + este script, que reproduz os dois arquivos
# de forma deterministica (--seed 42) em qualquer maquina.
#
# Para gerar SO os arquivos, sem rodar o teste:
#
#   # N row groups (row groups de 20k linhas) — o cenario que deve CONCLUIR
#   python3 scripts/generate_large_parquet.py --rows 1160000 --row-group-rows 20000 \
#       --columns 40 --output data/ab_muitos_rg.parquet --upload
#
#   # 1 row group unico com todas as linhas — o cenario que deve ESTOURAR
#   python3 scripts/generate_large_parquet.py --rows 1160000 --row-group-rows 1160000 \
#       --columns 40 --output data/ab_um_rg.parquet --upload
#
# Smoke test rapido (valida a mecanica em ~5s, sem 1 GB):
#   ./scripts/test_rowgroup_ab.sh --rows 40000 --limit-mb 64
#
# -----------------------------------------------------------------------------
# HIGIENE DA FILA (a parte que mais da errado — leia antes de mexer)
#
# 1. PurgeQueue e ASSINCRONO (pode levar ate 60s). Confirmar "0 mensagens" uma vez
#    nao basta.
# 2. A notificacao de evento do S3 tambem e ASSINCRONA: um upload feito segundos
#    antes pode ENTREGAR UMA MENSAGEM DEPOIS do purge ter reportado zero. Foi
#    exatamente assim que uma execucao anterior processou dois arquivos no lugar
#    de um, e a contagem de memoria saiu contaminada.
#    Por isso: purga -> espera -> CONFERE DE NOVO -> so entao dispara.
# 3. Mensagem em voo quando o consumer morre (OOMKilled) fica invisivel por ate
#    VisibilityTimeoutSeconds e reaparece depois. Para o cenario UM_RG, rode o
#    MUITOS_RG PRIMEIRO: a mensagem que sobrar do estouro e a ultima.
#
# Uso:
#   ./scripts/test_rowgroup_ab.sh --limit-mb 192
#   ./scripts/test_rowgroup_ab.sh --rows 40000 --limit-mb 64     # smoke
#   ./scripts/test_rowgroup_ab.sh --reuse                        # nao regera os arquivos
# =============================================================================

set -uo pipefail

LIMIT_MB=192
ROWS=1160000
MANY_RG_ROWS=20000
COLUMNS=40
DATA_DIR="${DATA_DIR:-data}"
BUCKET="${S3_BUCKET:-poc-bucket}"
OUT_DIR="reports"
REUSE=0
FALHAS=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --limit-mb)     LIMIT_MB="$2"; shift 2 ;;
    --rows)         ROWS="$2"; shift 2 ;;
    --many-rg-rows) MANY_RG_ROWS="$2"; shift 2 ;;
    --columns)      COLUMNS="$2"; shift 2 ;;
    --out-dir)      OUT_DIR="$2"; shift 2 ;;
    --reuse)        REUSE=1; shift ;;
    -h|--help)      sed -n '2,52p' "$0"; exit 0 ;;
    *) echo "opcao desconhecida: $1"; exit 1 ;;
  esac
done

mkdir -p "$OUT_DIR" "$DATA_DIR"
STAMP="$(date +%Y%m%d_%H%M%S)"
LOG="$OUT_DIR/rowgroup_ab_${STAMP}.log"
log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }

F_MUITOS="$DATA_DIR/ab_${ROWS}_${MANY_RG_ROWS}rg.parquet"
F_UM="$DATA_DIR/ab_${ROWS}_1rg.parquet"

# --- 1. gera (e sobe) os dois arquivos: mesmo total de linhas, row group diferente
if [[ "$REUSE" == "1" && -f "$F_MUITOS" && -f "$F_UM" ]]; then
  log "reusando os arquivos existentes (--reuse)"
else
  log "gerando: $ROWS linhas, $COLUMNS colunas"
  log "  MUITOS_RG: row groups de $MANY_RG_ROWS linhas"
  python3 scripts/generate_large_parquet.py --rows "$ROWS" --row-group-rows "$MANY_RG_ROWS" \
      --columns "$COLUMNS" --output "$F_MUITOS" --upload >>"$LOG" 2>&1 \
      || { echo "falha ao gerar/subir $F_MUITOS"; exit 1; }
  log "  UM_RG: 1 unico row group com as $ROWS linhas"
  # --row-group-rows == --rows faz o writer emitir UM unico row group.
  python3 scripts/generate_large_parquet.py --rows "$ROWS" --row-group-rows "$ROWS" \
      --columns "$COLUMNS" --output "$F_UM" --upload >>"$LOG" 2>&1 \
      || { echo "falha ao gerar/subir $F_UM"; exit 1; }
fi

# Confere o que foi realmente escrito (o teste depende disso, nao do que pedimos).
for f in "$F_MUITOS" "$F_UM"; do
  python3 - "$f" <<'PY'
import sys, pyarrow.parquet as pq
md = pq.ParquetFile(sys.argv[1]).metadata
sz = [md.row_group(i).total_byte_size for i in range(md.num_row_groups)]
print(f"  {sys.argv[1]}: {md.num_row_groups} row group(s), "
      f"maior descomprimido = {max(sz)/1024/1024:.1f} MB, {md.num_rows:,} linhas")
PY
done

# --- 2. garante o bucket antes de subir (idempotente) ------------------------
if [[ "$REUSE" == "1" ]]; then
  log "subindo os arquivos existentes para o S3"
  BUCKET="$BUCKET" python3 - "$F_MUITOS" "$F_UM" <<'PY'
import os, sys, boto3
s3 = boto3.client('s3', endpoint_url=os.getenv('AWS_ENDPOINT_URL', 'http://localhost:4566'),
                  region_name='us-east-1', aws_access_key_id='test', aws_secret_access_key='test')
b = os.getenv('BUCKET', 'poc-bucket')
try: s3.create_bucket(Bucket=b)
except Exception: pass
for f in sys.argv[1:]:
    s3.upload_file(f, b, f"input/{os.path.basename(f)}")
    print(f"   input/{os.path.basename(f)}")
PY
fi

drena_fila() {
  QUEUE="${SQS_QUEUE:-poc-notification-queue}" DLQ="${SQS_DLQ:-poc-notification-dlq}" python3 - <<'PY'
import os, time, boto3
kw = dict(endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566"),
          region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
          aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
          aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"))
sqs = boto3.client("sqs", **kw)
url = sqs.get_queue_url(QueueName=os.getenv("QUEUE"))["QueueUrl"]
durl = sqs.get_queue_url(QueueName=os.getenv("DLQ"))["QueueUrl"]
for u in (url, durl): sqs.purge_queue(QueueUrl=u)
for _ in range(20):
    time.sleep(3)
    a = sqs.get_queue_attributes(QueueUrl=url,
        AttributeNames=["ApproximateNumberOfMessages","ApproximateNumberOfMessagesNotVisible"])["Attributes"]
    if int(a["ApproximateNumberOfMessages"]) == 0 and int(a["ApproximateNumberOfMessagesNotVisible"]) == 0:
        time.sleep(10)   # a notificacao do upload pode chegar atrasada
        a = sqs.get_queue_attributes(QueueUrl=url,
            AttributeNames=["ApproximateNumberOfMessages","ApproximateNumberOfMessagesNotVisible"])["Attributes"]
        if int(a["ApproximateNumberOfMessages"]) == 0 and int(a["ApproximateNumberOfMessagesNotVisible"]) == 0:
            print("   fila zerada e CONFIRMADA"); return
        print("   residual pos-purge: repetindo")
        sqs.purge_queue(QueueUrl=url)
print("   AVISO: fila nao zerou")
PY
}

roda_cenario() {   # $1=rotulo  $2=key  -> ecoa "resultado|pico|linhas"
  local TAG="$1" KEY="$2" BASE="$OUT_DIR/ab_${1}_${STAMP}"
  log "cenario $TAG ($KEY) @ ${LIMIT_MB}m"
  docker compose rm -sf consumer >/dev/null 2>&1
  CONSUMER_MEM_LIMIT="${LIMIT_MB}m" docker compose up -d consumer >/dev/null 2>&1
  sleep 6
  drena_fila
  docker compose exec -T postgres psql -U pocuser -d pocdb -q \
    -c "TRUNCATE custody_position, custody_position_error, custody_position_staging;" >>"$LOG" 2>&1
  python3 scripts/simulate_s3_notification.py --bucket "$BUCKET" --key "$KEY" --mode sqs >>"$LOG" 2>&1

  echo "t_s,mem_mib,linhas" > "$BASE.csv"
  local PEAK=0 START=$(date +%s) RESULT=timeout
  while :; do
    local E=$(( $(date +%s) - START )); [ "$E" -gt 600 ] && break
    local MEM=$(docker stats --no-stream --format '{{.MemUsage}}' poc-consumer 2>/dev/null | awk '{print $1}' | tr -d 'MiB'); MEM=${MEM:-0}
    local ROWS=$(docker compose exec -T postgres psql -U pocuser -d pocdb -t -A -c 'SELECT count(*) FROM custody_position;' 2>/dev/null | tr -d '[:space:]')
    echo "$E,$MEM,$ROWS" >> "$BASE.csv"
    awk -v a="$MEM" -v b="$PEAK" 'BEGIN{exit !(a>b)}' && PEAK=$MEM
    local LOGS=$(docker logs poc-consumer 2>&1)
    if [ "$(docker inspect poc-consumer --format '{{.State.OOMKilled}}' 2>/dev/null)" = "true" ]; then RESULT=oomkilled; break; fi
    if grep -q "OutOfMemoryException" <<<"$LOGS"; then RESULT=out_of_memory; break; fi
    if grep -q "Result: .* ${KEY##*/}" <<<"$LOGS"; then RESULT=ok; break; fi
    sleep 2
  done
  docker logs poc-consumer > "$BASE.worker.log" 2>&1
  local LIN=$(docker compose exec -T postgres psql -U pocuser -d pocdb -t -A -c 'SELECT count(*) FROM custody_position;' 2>/dev/null | tr -d '[:space:]')
  log "  -> $RESULT | pico ${PEAK} MiB | ${LIN} linhas | OutOfMemoryException no log: $(grep -c 'OutOfMemoryException' "$BASE.worker.log")"
  echo "${RESULT}|${PEAK}|${LIN}"
}

K_MUITOS="input/$(basename "$F_MUITOS")"
K_UM="input/$(basename "$F_UM")"
R_MUITOS=$(roda_cenario MUITOS_RG "$K_MUITOS" | tail -1)
R_UM=$(roda_cenario UM_RG "$K_UM" | tail -1)

RES_MUITOS="${R_MUITOS%%|*}"; PICO_MUITOS=$(echo "$R_MUITOS" | cut -d'|' -f2)
RES_UM="${R_UM%%|*}";         PICO_UM=$(echo "$R_UM" | cut -d'|' -f2)

# --- 3. veredito com asercao ---------------------------------------------------
log "==================== VEREDITO (limite ${LIMIT_MB}m) ===================="
log "  MUITOS_RG : $RES_MUITOS (pico ${PICO_MUITOS} MiB)"
log "  UM_RG     : $RES_UM (pico ${PICO_UM} MiB)"

if [[ "$RES_MUITOS" != "ok" ]]; then
  log "  FALHA: o arquivo com VARIOS row groups nao concluiu. O teste nao prova nada assim."
  FALHAS=$((FALHAS+1))
fi
if [[ "$RES_UM" == "ok" ]]; then
  log "  FALHA: o arquivo de UM row group passou. A tese 'o row group e o piso' nao se sustenta"
  log "         NESTE limite — a projecao de colunas pode ter salvado. Abaixe --limit-mb e repita."
  FALHAS=$((FALHAS+1))
fi
if awk -v a="$PICO_UM" -v b="$PICO_MUITOS" 'BEGIN{exit !(a <= b)}'; then
  log "  FALHA: o pico do UM_RG (${PICO_UM}) nao superou o do MUITOS_RG (${PICO_MUITOS})."
  log "         Sem essa diferenca o teste nao demonstra que o row group define o pico."
  FALHAS=$((FALHAS+1))
fi

if [[ "$FALHAS" -eq 0 ]]; then
  log "  PASS: mesmo limite e mesmo tamanho de arquivo — varios row groups concluem,"
  log "        um row group unico estoura. A paginacao por row group esta demonstrada."
  log "log : $LOG"
  exit 0
fi
log "  $FALHAS asercao(oes) falharam."
log "log : $LOG"
exit 1
