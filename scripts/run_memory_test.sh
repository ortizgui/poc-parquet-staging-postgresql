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
# 1. A FILA E DRENADA ANTES DO DISPARO, COM JANELA DE CONFIRMACAO.
#    O PurgeQueue do SQS e ASSINCRONO (pode levar ate 60s). O drain purga main E
#    DLQ, exige visiveis E em-voo = 0, espera uma JANELA DE SETTLE (15s) e
#    reconfere — so entao libera o disparo. Sem isso, a notificacao do proprio
#    upload do arquivo para o S3 (tambem assincrona) entra junto com o disparo:
#    o MESMO arquivo e processado duas vezes e a contagem final sai inflada, sem
#    que nada esteja errado no worker. Orcamento total do drain: ~120s.
#
# 2. A CONCLUSAO EXIGE QUE TODA ENTREGA TENHA TERMINADO (entregas == conclusoes).
#    Um "Result:" no log nao significa que o teste acabou: pode haver outra
#    entrega em andamento.
#
# 3. A FALHA POR MEMORIA TEM DOIS MODOS, E O RUNNER COBRE OS DOIS.
#    a) OOMKilled do kernel (exit 137): NAO existe excecao no log — o container
#       simplesmente morre. Procurar so "OutOfMemoryException" faz o teste
#       reportar "timeout" justamente no cenario de pouca memoria, que e o que
#       ele existe para detectar.
#    b) System.OutOfMemoryException gerenciada: o runtime lanca ANTES do cgroup
#       matar (depende do heap hard limit / DOTNET_GCHeapHardLimitPercent) e o
#       processo pode terminar com exit 0. Por isso o runner checa OS DOIS.
# -----------------------------------------------------------------------------
#
# Pre-requisitos:
#   docker compose up -d
#   pip install -r requirements.txt                 # deps no .venv (boto3/pyarrow)
#   python3 scripts/setup_infra.py                  # bucket + fila + DLQ
#   python3 scripts/generate_large_parquet.py ...   # o arquivo de teste no S3
#
# O script prefere ./.venv/bin/python3 quando existir; sobrescreva com $PYTHON.
#
# Uso:
#   ./scripts/run_memory_test.sh --key input/prod_400k_20rg.parquet
#   ./scripts/run_memory_test.sh --key input/large_1gb.parquet --limit-mb 512
#
# Saida: pico de memoria, linhas lidas, registros gravados e veredito
#        (ok / out_of_memory / oomkilled / dlq / timeout).
# =============================================================================

set -uo pipefail

# C locale: garante decimal com PONTO no awk (pt_BR usaria virgula e quebraria o
# CSV e as comparacoes numericas). Nao afeta os textos ASCII/pt-BR impressos.
export LC_ALL=C

# Resolve o interpretador Python: $PYTHON > ./.venv/bin/python3 > python3.
if [[ -n "${PYTHON:-}" ]]; then
  PY="$PYTHON"
elif [[ -x "./.venv/bin/python3" ]]; then
  PY="./.venv/bin/python3"
else
  PY="python3"
fi

KEY=""
LIMIT_MB=512
TIMEOUT=1200
CONTAINER="poc-consumer"
BUCKET="${S3_BUCKET:-poc-bucket}"
QUEUE="${SQS_QUEUE:-poc-notification-queue}"
DLQ="${SQS_DLQ:-poc-notification-dlq}"
OUT_DIR="reports"
INTERVAL=1
SKIP_PURGE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --key)        KEY="$2"; shift 2 ;;
    --limit-mb)   LIMIT_MB="$2"; shift 2 ;;
    --timeout)    TIMEOUT="$2"; shift 2 ;;
    --container)  CONTAINER="$2"; shift 2 ;;
    --out-dir)    OUT_DIR="$2"; shift 2 ;;
    --skip-purge) SKIP_PURGE=1; shift ;;
    -h|--help)    sed -n '2,52p' "$0"; exit 0 ;;
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

# Converte a MemUsage do `docker stats` para MiB, tolerando MiB/GiB/KiB/B.
mem_mib() {
  docker stats --no-stream --format '{{.MemUsage}}' "$1" 2>/dev/null | awk '{
    v = $1
    if (v == "") { print "0"; exit }
    u = v; sub(/[0-9.]+/, "", u)
    sub(/[A-Za-z]+$/, "", v)
    if (v == "") { print "0"; exit }
    if      (u == "GiB") v *= 1024
    else if (u == "KiB") v /= 1024
    else if (u == "B")   v /= 1048576
    printf "%.1f", v
  }'
}

log "teste de memoria | arquivo=s3://$BUCKET/$KEY | limite=${LIMIT_MB}MB | container=$CONTAINER"

# --- 1. drena a fila (purge main+DLQ + janela de confirmacao) ----------------
if [[ "$SKIP_PURGE" == "1" ]]; then
  log "AVISO: --skip-purge informado — a contagem final pode vir contaminada por mensagem residual"
else
  log "drenando fila (purge main+DLQ + janela de confirmacao)"
  AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}" \
  AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}" \
  AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}" \
  AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost:4566}" \
  QUEUE="$QUEUE" DLQ="$DLQ" "$PY" - >>"$LOG" 2>&1 <<'PY'
import os, time, boto3


def main():
    kw = dict(endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
              region_name=os.getenv("AWS_DEFAULT_REGION"),
              aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
              aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))
    sqs = boto3.client("sqs", **kw)
    urls = {}
    for n in (os.getenv("QUEUE"), os.getenv("DLQ")):
        try:
            urls[n] = sqs.get_queue_url(QueueName=n)["QueueUrl"]
        except Exception as e:
            print(f"fila {n} indisponivel: {e}")
    if os.getenv("QUEUE") not in urls:
        raise SystemExit("fila principal nao encontrada — rode scripts/setup_infra.py")

    def depths():
        out = {}
        for n, u in urls.items():
            a = sqs.get_queue_attributes(QueueUrl=u, AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
            out[n] = (int(a["ApproximateNumberOfMessages"]),
                      int(a["ApproximateNumberOfMessagesNotVisible"]))
        return out

    def total(d):
        return sum(v[0] + v[1] for v in d.values())

    # Orcamento ~120s. So confirma o vazio se persistir apos a janela de settle
    # (a notificacao do S3 e assincrona) por 2 ciclos consecutivos.
    deadline = time.time() + 120
    settle = 15
    confirmed = 0
    while time.time() < deadline:
        for u in urls.values():
            try:
                sqs.purge_queue(QueueUrl=u)
            except Exception as e:
                print(f"aviso no purge: {e}")
        time.sleep(3)
        d = depths()
        if total(d) != 0:
            confirmed = 0
            print(f"aguardando purge: visiveis/em_voo={d}")
            continue
        time.sleep(settle)
        d2 = depths()
        if total(d2) == 0:
            confirmed += 1
            print(f"fila vazia confirmada ({confirmed}/2) apos {settle}s de settle")
            if confirmed >= 2:
                print("fila zerada e CONFIRMADA (visiveis=0, em voo=0, dlq=0)")
                return
        else:
            confirmed = 0
            print(f"residual pos-settle: {d2}; purgando de novo")
    print("AVISO: fila nao zerou no orcamento — o resultado pode vir contaminado")


main()
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
"$PY" scripts/simulate_s3_notification.py --bucket "$BUCKET" --key "$KEY" --mode sqs >>"$LOG" 2>&1

# --- 4. monitor --------------------------------------------------------------
echo "t_s,mem_mib,pct_limite,linhas" > "$MON"
PEAK=0
START=$(date +%s)
RESULT="timeout"

while :; do
  ELAPSED=$(( $(date +%s) - START ))
  [[ $ELAPSED -gt $TIMEOUT ]] && { RESULT="timeout"; break; }

  MEM=$(mem_mib "$CONTAINER")
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
  if grep -qE "Result:.*${KEY##*/}" <<<"$LOGS"; then
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
  oomkilled) log "FALHOU — container morto pelo kernel (OOMKilled, exit 137), sem excecao no log."
             log "       O piso e o MAIOR ROW GROUP, nao o runtime: reduza --row-group-rows no writer." ;;
  dlq) log "FALHOU — mensagem foi para a DLQ. Veja os atributos de triagem:"
       log "       docker exec poc-ministack aws sqs receive-message --queue-url <dlq> --endpoint-url http://localhost:4566" ;;
  *) log "INCONCLUSIVO — timeout. Veja o log do worker: docker logs $CONTAINER --tail 50" ;;
esac

log "log : $LOG"
log "csv : $MON"
log "worker: ${LOG%.log}.worker.log"
exit 0
