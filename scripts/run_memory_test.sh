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
# -----------------------------------------------------------------------------
# HIGIENE DO TESTE (importa mais do que parece — sem isso o resultado mente)
#
# 1. A FILA E DRENADA ANTES DO DISPARO, COM ESPERA DE PROPAGACAO.
#    O PurgeQueue do SQS e ASSINCRONO: pode levar ate 60s para fazer efeito.
#    Sem esperar a profundidade zerar, uma mensagem residual — inclusive a
#    notificacao do proprio upload do arquivo para o S3 — e entregue junto com o
#    disparo. O MESMO arquivo e processado duas vezes e a contagem final sai
#    inflada, sem que nada esteja errado no worker.
#
# 2. A CONCLUSAO EXIGE QUE TODA ENTREGA TENHA TERMINADO (entregas == conclusoes).
#    Um "Result:" no log nao significa que o teste acabou: pode haver outra
#    entrega em andamento.
#
# 3. O OOM E DETECTADO POR `State.OOMKilled`, NAO SO PELA EXCECAO.
#    Quando o kernel mata o processo (OOMKilled, exit 137) NAO existe excecao no
#    log — o container simplesmente morre. Procurar so "OutOfMemoryException"
#    faz o teste reportar "timeout" justamente no cenario de pouca memoria, que e
#    o que ele existe para detectar.
# -----------------------------------------------------------------------------
#
# Pre-requisitos:
#   docker compose up -d
#   python3 scripts/setup_infra.py                 # bucket + fila + DLQ
#   python3 scripts/generate_large_parquet.py ...  # o arquivo de teste no S3
#
# Uso:
#   ./scripts/run_memory_test.sh --key input/prod_400k_20rg.parquet
#   ./scripts/run_memory_test.sh --key input/large_1gb.parquet --limit-mb 512
#
# Saida: pico de memoria, linhas lidas, registros gravados e veredito
#        (ok / out_of_memory / oomkilled / dlq / timeout).
# =============================================================================

set -uo pipefail

KEY=""
LIMIT_MB=512
TIMEOUT=1200
CONTAINER="poc-consumer"
BUCKET="${S3_BUCKET:-poc-bucket}"
QUEUE="${SQS_QUEUE:-poc-notification-queue}"
DLQ="${SQS_DLQ:-poc-notification-dlq}"
OUT_DIR="reports"
INTERVAL=2
SKIP_PURGE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --key)        KEY="$2"; shift 2 ;;
    --limit-mb)   LIMIT_MB="$2"; shift 2 ;;
    --timeout)    TIMEOUT="$2"; shift 2 ;;
    --container)  CONTAINER="$2"; shift 2 ;;
    --out-dir)    OUT_DIR="$2"; shift 2 ;;
    --skip-purge) SKIP_PURGE=1; shift ;;
    -h|--help)    sed -n '2,45p' "$0"; exit 0 ;;
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

# --- 1. drena a fila (purge + espera de propagacao) --------------------------
if [[ "$SKIP_PURGE" == "1" ]]; then
  log "AVISO: --skip-purge informado — a contagem final pode vir contaminada por mensagem residual"
else
  log "drenando fila (purge e assincrono: aguardando profundidade zerar)"
  AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}" \
  AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}" \
  AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}" \
  AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost:4566}" \
  QUEUE="$QUEUE" DLQ="$DLQ" python3 - >>"$LOG" 2>&1 <<'PY'
import os, time, boto3
kw = dict(endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
          region_name=os.getenv("AWS_DEFAULT_REGION"),
          aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
          aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))
sqs = boto3.client("sqs", **kw)
names = [os.getenv("QUEUE"), os.getenv("DLQ")]
urls = {}
for n in names:
    try:
        urls[n] = sqs.get_queue_url(QueueName=n)["QueueUrl"]
    except Exception as e:
        print(f"fila {n} indisponivel: {e}")
if names[0] not in urls:
    raise SystemExit("fila principal nao encontrada — rode scripts/setup_infra.py")
for u in urls.values():
    sqs.purge_queue(QueueUrl=u)
for i in range(30):
    time.sleep(3)
    a = sqs.get_queue_attributes(QueueUrl=urls[names[0]],
        AttributeNames=["ApproximateNumberOfMessages",
                        "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
    vis, novis = int(a["ApproximateNumberOfMessages"]), int(a["ApproximateNumberOfMessagesNotVisible"])
    dlq = 0
    if names[1] in urls:
        dlq = int(sqs.get_queue_attributes(QueueUrl=urls[names[1]],
              AttributeNames=["ApproximateNumberOfMessages"])["Attributes"]["ApproximateNumberOfMessages"])
    if vis == 0 and novis == 0 and dlq == 0:
        print(f"fila zerada apos {(i + 1) * 3}s (visiveis=0, em voo=0, dlq=0)")
        break
    if vis > 0 or novis > 0:
        sqs.purge_queue(QueueUrl=urls[names[0]])   # residuo reapareceu: purga de novo
    print(f"aguardando purge: visiveis={vis} em_voo={novis} dlq={dlq}")
else:
    print("AVISO: fila nao zerou em 90s — o resultado pode vir contaminado")
PY
fi

# --- 2. baseline do banco ----------------------------------------------------
log "resetando tabelas"
docker compose exec -T postgres psql -U pocuser -d pocdb -q \
  -c "TRUNCATE custody_position, custody_position_error, custody_position_staging;" >>"$LOG" 2>&1

log "linhas antes: $(docker compose exec -T postgres psql -U pocuser -d pocdb -t -A \
  -c 'SELECT count(*) FROM custody_position;' 2>/dev/null | tr -d '[:space:]')"

# --- 3. dispara o evento S3 --------------------------------------------------
log "disparando notificacao S3 -> SQS para $KEY"
python3 scripts/simulate_s3_notification.py --bucket "$BUCKET" --key "$KEY" --mode sqs >>"$LOG" 2>&1

# --- 4. monitor --------------------------------------------------------------
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

  LOGS=$(docker logs "$CONTAINER" 2>&1)

  # OOMKilled: o kernel matou o container — NAO ha excecao no log neste caso.
  if [[ "$(docker inspect "$CONTAINER" --format '{{.State.OOMKilled}}' 2>/dev/null)" == "true" ]]; then
    RESULT="oomkilled"; break
  fi
  if grep -q "OutOfMemoryException" <<<"$LOGS"; then
    RESULT="out_of_memory"; break
  fi
  if grep -q "movida para a DLQ" <<<"$LOGS"; then
    RESULT="dlq"; break
  fi

  # Concluido somente quando toda entrega terminou (entregas == conclusoes).
  if grep -q "Result: .* ${KEY##*/}" <<<"$LOGS"; then
    P=$(grep -c "Processing: " <<<"$LOGS"); R=$(grep -c "Result: " <<<"$LOGS")
    if [[ "$P" == "$R" ]]; then
      RESULT="ok"; break
    fi
    log "aguardando: ${P} entrega(s) / ${R} concluida(s)"
  fi

  sleep "$INTERVAL"
done

# --- 5. veredito -------------------------------------------------------------
docker logs "$CONTAINER" > "${LOG%.log}.worker.log" 2>&1
TOTAL=$(docker compose exec -T postgres psql -U pocuser -d pocdb -t -A \
        -c 'SELECT count(*) FROM custody_position;' 2>/dev/null | tr -d '[:space:]')
ERRORS=$(docker compose exec -T postgres psql -U pocuser -d pocdb -t -A \
        -c 'SELECT count(*) FROM custody_position_error;' 2>/dev/null | tr -d '[:space:]')
DELIVERIES=$(grep -c "Processing: " "${LOG%.log}.worker.log" 2>/dev/null || echo "?")

log "---------------------------------------------"
log "arquivo         : $KEY"
log "pico de memoria : ${PEAK}MiB de ${LIMIT_MB}MB ($(awk -v p="$PEAK" -v l="$LIMIT_MB" 'BEGIN{printf "%.0f", p*100/l}')%)"
log "entregas        : $DELIVERIES (esperado: 1)"
log "linhas          : ${TOTAL:-?}"
log "erros           : ${ERRORS:-?}"
log "resultado       : $RESULT"
log "---------------------------------------------"

case "$RESULT" in
  ok) log "OK — ingestao completa sem estourar o limite."
      [[ "$DELIVERIES" != "1" ]] && log "ATENCAO: houve mais de uma entrega da mesma mensagem — contagem possivelmente duplicada." ;;
  out_of_memory) log "FALHOU — OutOfMemoryException. O pico e proporcional ao MAIOR ROW GROUP:"
                 log "       reduza --row-group-rows no gerador do parquet (o writer define o row group)." ;;
  oomkilled) log "FALHOU — container morto pelo kernel (OOMKilled, exit 137)."
             log "       O runtime nao checou a levantar excecao: 128MB e inviavel, o piso e ~150-200MB." ;;
  dlq) log "FALHOU — mensagem foi para a DLQ. Veja os atributos de triagem:"
       log "       docker exec poc-ministack aws sqs receive-message --queue-url <dlq> --endpoint-url http://localhost:4566" ;;
  *) log "INCONCLUSIVO — timeout. Veja o log do worker: docker logs $CONTAINER --tail 50" ;;
esac

log "log : $LOG"
log "csv : $MON"
log "worker: ${LOG%.log}.worker.log"
exit 0
