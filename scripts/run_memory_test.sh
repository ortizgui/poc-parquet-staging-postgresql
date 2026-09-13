#!/usr/bin/env bash
# =============================================================================
# Teste de memoria e paginacao: Parquet -> PostgreSQL com limite de RAM no pod
#
# Dispara um arquivo que JA esta no S3 e acompanha, segundo a segundo, o consumo
# de memoria do consumer (limitado por `mem_limit` no docker-compose.yml) contra
# as contagens no banco.
#
# E o teste que prova o ponto: a paginacao por ROW GROUP mantem a memoria chapada
# mesmo com o arquivo sendo maior que a RAM do pod. O que define o pico de memoria
# e o tamanho do MAIOR ROW GROUP — nao o tamanho do arquivo.
#
# Pre-requisitos:
#   docker compose up -d
#   python3 scripts/setup_infra.py                 # bucket + fila + DLQ
#   python3 scripts/generate_large_parquet.py ...  # o arquivo de teste no S3
#
# Uso:
#   ./scripts/run_memory_test.sh --key input/prod_400k_20rg.parquet
#   ./scripts/run_memory_test.sh --key input/prod_200k_1rg.parquet --limit-mb 512
#
# Saida: pico de memoria, linhas lidas, registros gravados e veredito
#        (sucesso / OutOfMemoryException / timeout).
# =============================================================================

set -uo pipefail

KEY=""
LIMIT_MB=512
TIMEOUT=1200
CONTAINER="poc-consumer"
BUCKET="${S3_BUCKET:-poc-bucket}"
OUT_DIR="reports"
INTERVAL=2

while [[ $# -gt 0 ]]; do
  case "$1" in
    --key)       KEY="$2"; shift 2 ;;
    --limit-mb)  LIMIT_MB="$2"; shift 2 ;;
    --timeout)   TIMEOUT="$2"; shift 2 ;;
    --container) CONTAINER="$2"; shift 2 ;;
    --out-dir)   OUT_DIR="$2"; shift 2 ;;
    -h|--help)   sed -n '2,24p' "$0"; exit 0 ;;
    *) echo "opcao desconhecida: $1"; exit 1 ;;
  esac
done

if [[ -z "$KEY" ]]; then
  echo "erro: --key e obrigatorio (ex: --key input/prod_400k_20rg.parquet)"
  exit 1
fi

mkdir -p "$OUT_DIR"
STAMP="$(date +%Y%m%d_%H%M%S)"
LOG="$OUT_DIR/memory_test_${STAMP}.log"
MON="$OUT_DIR/memory_test_${STAMP}.csv"

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }

log "teste de memoria | arquivo=s3://$BUCKET/$KEY | limite=${LIMIT_MB}MB | container=$CONTAINER"

# --- baseline do banco -------------------------------------------------------
log "resetando tabelas"
docker compose exec -T postgres psql -U pocuser -d pocdb -q \
  -c "TRUNCATE custody_position, custody_position_error, custody_position_staging;" >>"$LOG" 2>&1

log "linhas antes: $(docker compose exec -T postgres psql -U pocuser -d pocdb -t -A \
  -c 'SELECT count(*) FROM custody_position;' 2>/dev/null | tr -d '[:space:]')"

# --- dispara o evento S3 -----------------------------------------------------
log "disparando notificacao S3 -> SQS para $KEY"
python3 scripts/simulate_s3_notification.py --bucket "$BUCKET" --key "$KEY" --mode sqs >>"$LOG" 2>&1

# --- monitor -----------------------------------------------------------------
echo "t_s,mem_mib,pct_limite,linhas" > "$MON"
PEAK=0
START=$(date +%s)
RESULT="timeout"

while :; do
  ELAPSED=$(( $(date +%s) - START ))
  [[ $ELAPSED -gt $TIMEOUT ]] && { RESULT="timeout"; break; }

  MEM=$(docker stats --no-stream --format '{{.MemUsage}}' "$CONTAINER" 2>/dev/null | awk '{print $1}' | tr -d 'MiB')
  ROWS=$(docker compose exec -T postgres psql -U pocuser -d pocdb -t -A \
          -c 'SELECT count(*) FROM custody_position;' 2>/dev/null | tr -d '[:space:]')
  MEM=${MEM:-0}; ROWS=${ROWS:-0}

  PCT=$(awk -v m="$MEM" -v l="$LIMIT_MB" 'BEGIN{printf "%.1f", (l>0? m*100/l : 0)}')
  echo "${ELAPSED},${MEM},${PCT},${ROWS}" >> "$MON"
  awk -v a="$MEM" -v b="$PEAK" 'BEGIN{exit !(a>b)}' && PEAK=$MEM

  log "t=${ELAPSED}s mem=${MEM}MiB ($PCT% do limite) linhas=$ROWS"

  # concluiu: a mensagem sumiu da fila e o worker voltou a ficar ocioso
  if docker logs "$CONTAINER" 2>&1 | grep -q "Result: .* ${KEY##*/}"; then
    RESULT="ok"; break
  fi
  if docker logs "$CONTAINER" 2>&1 | grep -q "OutOfMemoryException"; then
    RESULT="oom"; break
  fi

  sleep "$INTERVAL"
done

# --- veredito ----------------------------------------------------------------
TOTAL=$(docker compose exec -T postgres psql -U pocuser -d pocdb -t -A \
        -c 'SELECT count(*) FROM custody_position;' 2>/dev/null | tr -d '[:space:]')
ERRORS=$(docker compose exec -T postgres psql -U pocuser -d pocdb -t -A \
        -c 'SELECT count(*) FROM custody_position_error;' 2>/dev/null | tr -d '[:space:]')

log "---------------------------------------------"
log "pico de memoria : ${PEAK}MiB de ${LIMIT_MB}MB ($(awk -v p="$PEAK" -v l="$LIMIT_MB" 'BEGIN{printf "%.0f", p*100/l}')%)"
log "linhas          : ${TOTAL:-?}"
log "erros           : ${ERRORS:-?}"
log "resultado       : $RESULT"
log "---------------------------------------------"

case "$RESULT" in
  ok)  log "OK — ingestao completa sem estourar o limite." ;;
  oom) log "FALHOU — OutOfMemoryException. O pico e proporcional ao MAIOR ROW GROUP:"
       log "       reduza --row-group-rows no gerador do parquet (o writer define o row group)." ;;
  *)   log "INCONCLUSIVO — timeout. Veja o log do worker: docker logs $CONTAINER --tail 50" ;;
esac

log "log : $LOG"
log "csv : $MON"
exit 0
