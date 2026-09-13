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
#   ./scripts/test_rowgroup_ab.sh --limit-mb 512 --rows 1160000 --columns 40
# =============================================================================

set -uo pipefail

LIMIT_MB=192
ROWS=1160000
COLUMNS=40
DATA_DIR="${DATA_DIR:-data}"
BUCKET="${S3_BUCKET:-poc-bucket}"
OUT_DIR="reports"
FALHAS=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --limit-mb) LIMIT_MB="$2"; shift 2 ;;
    --rows)     ROWS="$2"; shift 2 ;;
    --columns)  COLUMNS="$2"; shift 2 ;;
    --out-dir)  OUT_DIR="$2"; shift 2 ;;
    -h|--help)  sed -n '2,40p' "$0"; exit 0 ;;
    *) echo "opcao desconhecida: $1"; exit 1 ;;
  esac
done

mkdir -p "$OUT_DIR" "$DATA_DIR"
STAMP="$(date +%Y%m%d_%H%M%S)"
LOG="$OUT_DIR/rowgroup_ab_${STAMP}.log"
log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }

F_MUITOS="$DATA_DIR/ab_${ROWS}_muitos_rg.parquet"
F_UM="$DATA_DIR/ab_${ROWS}_um_rg.parquet"

# --- 1. gera os dois arquivos: mesmo total de linhas, row group diferente -----
log "gerando os dois arquivos ($ROWS linhas, $COLUMNS colunas)"
python3 scripts/generate_large_parquet.py --rows "$ROWS" --row-group-rows 20000 \
    --columns "$COLUMNS" --output "$F_MUITOS" >>"$LOG" 2>&1 || { echo "falha ao gerar $F_MUITOS"; exit 1; }
python3 scripts/generate_large_parquet.py --rows "$ROWS" --row-group-rows "$ROWS" \
    --columns "$COLUMNS" --output "$F_UM" >>"$LOG" 2>&1 || { echo "falha ao gerar $F_UM"; exit 1; }

for f in "$F_MUITOS" "$F_UM"; do
  python3 - "$f" <<'PY'
import sys, pyarrow.parquet as pq
md = pq.ParquetFile(sys.argv[1]).metadata
sz = [md.row_group(i).total_byte_size for i in range(md.num_row_groups)]
print(f"  {sys.argv[1]}: {md.num_row_groups} row group(s), maior descomprimido = {max(sz)/1024/1024:.1f} MB")
PY
done

# --- 2. sobe os dois para o S3 ------------------------------------------------
log "subindo para o S3"
python3 scripts/upload_to_s3.py --file "$F_MUITOS" --key "input/$(basename "$F_MUITOS")" >>"$LOG" 2>&1
python3 scripts/upload_to_s3.py --file "$F_UM" --key "input/$(basename "$F_UM")" >>"$LOG" 2>&1

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
  log "  -> $RESULT | pico ${PEAK} MiB | ${LIN} linhas"
  echo "${RESULT}|${PEAK}|${LIN}"
}

R_MUITOS=$(roda_cenario MUITOS_RG "input/$(basename "$F_MUITOS")" | tail -1)
R_UM=$(roda_cenario UM_RG "input/$(basename "$F_UM")" | tail -1)

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
