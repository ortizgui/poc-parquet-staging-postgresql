#!/usr/bin/env bash
# =============================================================================
# A/B DECISIVO: paginacao por row group
#
# Prova o ponto central da POC: com o MESMO limite de memoria e dois arquivos do
# MESMO tamanho, o que decide o pico NAO e o tamanho do arquivo — e o maior row group.
#
#   Cenario MUITOS_RG : N row groups   -> deve concluir, com pico chapado
#   Cenario UM_RG     : 1 row group    -> deve estourar o mesmo limite por memoria
#
# Como o UM_RG falha depende do limite do GC/cgroup — as DUAS provam o piso do row group:
#   - OOMKilled do kernel (exit 137, SEM excecao no log); ou
#   - System.OutOfMemoryException gerenciada ANTES do cgroup matar
#     (@192m com DOTNET_GCHeapHardLimitPercent=0x4B o runtime lanca antes do kill).
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
# REQUISITOS:
#   - dependencias Python (boto3/pyarrow) instaladas no .venv:
#       pip install -r requirements.txt
#     O script usa ./.venv/bin/python3 quando existir; sobrescreva com $PYTHON.
#   - setup_infra.py e RECOMENDADO antes (cria fila + DLQ + RedrivePolicy). O
#     bucket e criado por este script (idempotente), mas a FILA precisa existir
#     para o consumer consumir.
#
# -----------------------------------------------------------------------------
# HIGIENE DA FILA E DETERMINISMO (a parte que mais da errado — leia antes de mexer)
#
# 1. PurgeQueue e ASSINCRONO (pode levar ate 60s). Confirmar "0 mensagens" uma vez
#    nao basta: o drain exige visiveis E em-voo = 0, espera uma JANELA DE SETTLE
#    (15s) e reconfere, com orcamento total de ~120s.
# 2. A notificacao de evento do S3 tambem e ASSINCRONA (o ministack ENTREGA
#    S3->SQS normalmente — o problema aqui e a corrida, nao a entrega): um upload
#    feito segundos antes pode ENTREGAR UMA MENSAGEM DEPOIS do purge ter
#    reportado zero. No A/B isso e fatal: o consumer do MUITOS_RG podia pegar o
#    arquivo de UM_RG e falhar por memoria — um FALSO FAIL. Correcao em DUAS
#    camadas:
#      a) a notificacao do bucket e DESLIGADA durante o run (config salva e
#         restaurada no fim, mesmo em falha) PARA DETERMINISMO. O unico disparo
#         passa a ser o `simulate_s3_notification.py --mode sqs` explicito; e
#      b) o drain roda com o consumer DOWN, antes de subir, e DE NOVO
#         imediatamente antes do disparo.
# 3. Mensagem em voo quando o consumer morre (OOMKilled) fica invisivel por ate
#    VisibilityTimeoutSeconds e reaparece depois. Para o cenario UM_RG, rode o
#    MUITOS_RG PRIMEIRO: a mensagem que sobrar do estouro e a ultima.
#
# ORDEM DE CADA CENARIO (nao mexa):
#   rm consumer -> drena (consumer DOWN) -> truncate -> sobe consumer (mem limit)
#   -> settle -> drena DE NOVO -> dispara explicitamente.
#
# Uso:
#   ./scripts/test_rowgroup_ab.sh --limit-mb 192
#   ./scripts/test_rowgroup_ab.sh --rows 40000 --limit-mb 64     # smoke
#   ./scripts/test_rowgroup_ab.sh --reuse                        # nao regera os arquivos
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
    -h|--help)      sed -n '2,75p' "$0"; exit 0 ;;
    *) echo "opcao desconhecida: $1"; exit 1 ;;
  esac
done

mkdir -p "$OUT_DIR" "$DATA_DIR"
STAMP="$(date +%Y%m%d_%H%M%S)"
LOG="$OUT_DIR/rowgroup_ab_${STAMP}.log"
log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }

# --- Notificacao do bucket: desliga durante o run e restaura no fim -----------
# Os uploads no inicio do script geram notificacoes S3 ASSINCRONAS que podem
# chegar DEPOIS do drain e virar uma mensagem extra. No pior caso o consumer do
# MUITOS_RG pega o arquivo de UM_RG e falha por memoria — um FALSO FAIL. O disparo
# explicito (--mode sqs) NAO depende da notificacao, entao salvamos a config,
# desligamos durante o run e restauramos no fim (inclusive em falha).
NOTIF_SAVED="$OUT_DIR/notif_config_${STAMP}.json"
notif_disable() {
  SAVE="$NOTIF_SAVED" BUCKET="$BUCKET" "$PY" - <<'PY'
import json, os, sys, boto3
s3 = boto3.client('s3', endpoint_url=os.getenv('AWS_ENDPOINT_URL', 'http://localhost:4566'),
                  region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
                  aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'test'),
                  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'test'))
b, save = os.getenv('BUCKET'), os.getenv('SAVE')
keep = ('QueueConfigurations', 'TopicConfigurations',
        'LambdaFunctionConfigurations', 'EventBridgeConfiguration')
try:
    cfg = s3.get_bucket_notification_configuration(Bucket=b)
except Exception as e:
    print(f"   AVISO: nao li a config de notificacao ({e}); seguindo so com o drain")
    sys.exit(0)
clean = {k: cfg[k] for k in keep if k in cfg}
with open(save, 'w') as fh:
    json.dump(clean, fh)
try:
    s3.put_bucket_notification_configuration(Bucket=b, NotificationConfiguration={})
    print(f"   notificacao do bucket DESLIGADA durante o A/B (config salva em {save})")
except Exception as e:
    print(f"   AVISO: nao desliguei a notificacao ({e}); o drain reforcado cobre")
PY
}
# shellcheck disable=SC2329  # invocada via trap EXIT/INT/TERM (shellcheck nao segue traps)
notif_restore() {
  [[ -f "$NOTIF_SAVED" ]] || return 0
  SAVE="$NOTIF_SAVED" BUCKET="$BUCKET" "$PY" - <<'PY'
import json, os, boto3
s3 = boto3.client('s3', endpoint_url=os.getenv('AWS_ENDPOINT_URL', 'http://localhost:4566'),
                  region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
                  aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'test'),
                  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'test'))
b, save = os.getenv('BUCKET'), os.getenv('SAVE')
try:
    with open(save) as fh:
        cfg = json.load(fh)
    s3.put_bucket_notification_configuration(Bucket=b, NotificationConfiguration=cfg)
    os.remove(save)
    print("   notificacao do bucket RESTAURADA")
except Exception as e:
    print(f"   AVISO: nao restaurei a notificacao ({e}); rode scripts/setup_infra.py se precisar")
PY
}
trap notif_restore EXIT INT TERM

F_MUITOS="$DATA_DIR/ab_${ROWS}_${MANY_RG_ROWS}rg.parquet"
F_UM="$DATA_DIR/ab_${ROWS}_1rg.parquet"

# --- 1. garante o bucket ANTES de gerar/subir (idempotente) ------------------
# O create_bucket e idempotente: seguro numa maquina limpa ou ja provisionada.
# Isto faz o caminho DEFAULT funcionar numa maquina nova sem exigir um
# setup_infra.py so para o bucket. O setup_infra.py continua RECOMENDADO: ele
# tambem cria fila + DLQ + RedrivePolicy, sem os quais o consumer nao consome.
log "garantindo o bucket s3://$BUCKET"
BUCKET="$BUCKET" "$PY" - <<'PY'
import os, boto3
s3 = boto3.client('s3', endpoint_url=os.getenv('AWS_ENDPOINT_URL', 'http://localhost:4566'),
                  region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
                  aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'test'),
                  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'test'))
b = os.getenv('BUCKET', 'poc-bucket')
try:
    s3.create_bucket(Bucket=b)
    print(f"   bucket {b} criado")
except Exception:
    print(f"   bucket {b} ja existe (criacao idempotente)")
PY

# Desliga a notificacao do bucket ANTES do upload: os uploads nao geram mensagens
# S3 assincronas concorrendo com o disparo explicito (ver HIGIENE DA FILA).
notif_disable

# --- 2. gera (e sobe) os dois arquivos: mesmo total de linhas, row group diferente
if [[ "$REUSE" == "1" && -f "$F_MUITOS" && -f "$F_UM" ]]; then
  log "reusando os arquivos existentes (--reuse)"
  log "  subindo os arquivos existentes para o S3"
  BUCKET="$BUCKET" "$PY" - "$F_MUITOS" "$F_UM" <<'PY'
import os, sys, boto3
s3 = boto3.client('s3', endpoint_url=os.getenv('AWS_ENDPOINT_URL', 'http://localhost:4566'),
                  region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
                  aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'test'),
                  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'test'))
b = os.getenv('BUCKET', 'poc-bucket')
for f in sys.argv[1:]:
    s3.upload_file(f, b, f"input/{os.path.basename(f)}")
    print(f"   input/{os.path.basename(f)}")
PY
else
  log "gerando: $ROWS linhas, $COLUMNS colunas"
  log "  MUITOS_RG: row groups de $MANY_RG_ROWS linhas"
  "$PY" scripts/generate_large_parquet.py --rows "$ROWS" --row-group-rows "$MANY_RG_ROWS" \
      --columns "$COLUMNS" --output "$F_MUITOS" --upload >>"$LOG" 2>&1 \
      || { echo "falha ao gerar/subir $F_MUITOS"; exit 1; }
  log "  UM_RG: 1 unico row group com as $ROWS linhas"
  # --row-group-rows == --rows faz o writer emitir UM unico row group.
  "$PY" scripts/generate_large_parquet.py --rows "$ROWS" --row-group-rows "$ROWS" \
      --columns "$COLUMNS" --output "$F_UM" --upload >>"$LOG" 2>&1 \
      || { echo "falha ao gerar/subir $F_UM"; exit 1; }
fi

# Confere o que foi realmente escrito (o teste depende disso, nao do que pedimos).
for f in "$F_MUITOS" "$F_UM"; do
  "$PY" - "$f" <<'PY'
import sys, pyarrow.parquet as pq
md = pq.ParquetFile(sys.argv[1]).metadata
sz = [md.row_group(i).total_byte_size for i in range(md.num_row_groups)]
print(f"  {sys.argv[1]}: {md.num_row_groups} row group(s), "
      f"maior descomprimido = {max(sz)/1024/1024:.1f} MB, {md.num_rows:,} linhas")
PY
done

# --- 3. drena a fila antes de cada cenario -----------------------------------
drena_fila() {
  QUEUE="${SQS_QUEUE:-poc-notification-queue}" DLQ="${SQS_DLQ:-poc-notification-dlq}" "$PY" - <<'PY'
import os, time, boto3


def main():
    kw = dict(endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566"),
              region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
              aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
              aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"))
    sqs = boto3.client("sqs", **kw)
    urls = {
        "main": sqs.get_queue_url(QueueName=os.getenv("QUEUE"))["QueueUrl"],
        "dlq": sqs.get_queue_url(QueueName=os.getenv("DLQ"))["QueueUrl"],
    }

    def depths():
        out = {}
        for key, u in urls.items():
            a = sqs.get_queue_attributes(QueueUrl=u, AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
            out[key] = (int(a["ApproximateNumberOfMessages"]),
                        int(a["ApproximateNumberOfMessagesNotVisible"]))
        return out

    def total(d):
        return sum(v[0] + v[1] for v in d.values())

    def purge_all():
        for u in urls.values():
            try:
                sqs.purge_queue(QueueUrl=u)
            except Exception as e:
                print(f"   aviso no purge: {e}")

    # Orcamento ~120s. So confirma o vazio se ele persistir apos a janela de
    # settle (a notificacao do S3 e assincrona) por 2 ciclos consecutivos.
    deadline = time.time() + 120
    settle = 15
    confirmed = 0
    while time.time() < deadline:
        purge_all()
        time.sleep(3)
        d = depths()
        if total(d) != 0:
            confirmed = 0
            print(f"   aguardando purge: visiveis/em_voo={d}")
            continue
        time.sleep(settle)
        d2 = depths()
        if total(d2) == 0:
            confirmed += 1
            print(f"   fila vazia confirmada ({confirmed}/2) apos janela de {settle}s")
            if confirmed >= 2:
                print("   fila zerada e CONFIRMADA")
                return
        else:
            confirmed = 0
            print(f"   residual pos-settle: {d2}; purgando de novo")
    print("   AVISO: fila nao zerou no orcamento — o resultado pode vir contaminado")


main()
PY
}

# Converte a MemUsage do `docker stats` para MiB, tolerando MiB/GiB/KiB/B.
# Ex.: "108.5MiB" -> 108.5 | "1.2GiB" -> 1228.8 | "512KiB" -> 0.5
mem_mib() {
  docker stats --no-stream --format '{{.MemUsage}}' "$1" 2>/dev/null | awk '{
    v = $1
    if (v == "") { print "0"; exit }
    u = v; sub(/[0-9.]+/, "", u)       # unidade (MiB, GiB, KiB, B)
    sub(/[A-Za-z]+$/, "", v)           # valor numerico
    if (v == "") { print "0"; exit }
    if      (u == "GiB") v *= 1024
    else if (u == "KiB") v /= 1024
    else if (u == "B")   v /= 1048576
    printf "%.1f", v
  }'
}

roda_cenario() {   # $1=rotulo  $2=key  -> ecoa "resultado|pico|linhas"
  local TAG="$1" KEY="$2" BASE="$OUT_DIR/ab_${1}_${STAMP}"
  log "cenario $TAG ($KEY) @ ${LIMIT_MB}m"

  # 1. derruba o consumer: drenar com ele no ar e corrida perdida.
  docker compose rm -sf consumer >/dev/null 2>&1

  # 2. drena (purge main+DLQ + janela de confirmacao) com o consumer DOWN.
  drena_fila

  # 3. zera as tabelas com o consumer parado.
  docker compose exec -T postgres psql -U pocuser -d pocdb -q \
    -c "TRUNCATE custody_position, custody_position_error, custody_position_staging;" >>"$LOG" 2>&1

  # 4. sobe o consumer com o limite do cenario.
  CONSUMER_MEM_LIMIT="${LIMIT_MB}m" docker compose up -d consumer >/dev/null 2>&1

  # 5. settle do startup.
  sleep 6

  # 6. drena DE NOVO imediatamente antes do disparo explicito (unica fonte).
  drena_fila

  # 7. dispara explicitamente.
  "$PY" scripts/simulate_s3_notification.py --bucket "$BUCKET" --key "$KEY" --mode sqs >>"$LOG" 2>&1

  echo "t_s,mem_mib,linhas" > "$BASE.csv"
  local PEAK=0 RESULT=timeout START
  START=$(date +%s)
  while :; do
    local E=$(( $(date +%s) - START )); [ "$E" -gt 600 ] && break
    local MEM; MEM=$(mem_mib poc-consumer); MEM=${MEM:-0}
    local ROWS; ROWS=$(docker compose exec -T postgres psql -U pocuser -d pocdb -t -A -c 'SELECT count(*) FROM custody_position;' 2>/dev/null | tr -d '[:space:]')
    echo "$E,$MEM,$ROWS" >> "$BASE.csv"
    awk -v a="$MEM" -v b="$PEAK" 'BEGIN{exit !(a>b)}' && PEAK=$MEM
    local LOGS; LOGS=$(docker logs poc-consumer 2>&1)
    if [ "$(docker inspect poc-consumer --format '{{.State.OOMKilled}}' 2>/dev/null)" = "true" ]; then RESULT=oomkilled; break; fi
    if grep -q "OutOfMemoryException" <<<"$LOGS"; then RESULT=out_of_memory; break; fi
    if grep -qE "Result:.*${KEY##*/}" <<<"$LOGS"; then RESULT=ok; break; fi
    sleep 1
  done
  docker logs poc-consumer > "$BASE.worker.log" 2>&1
  local LIN; LIN=$(docker compose exec -T postgres psql -U pocuser -d pocdb -t -A -c 'SELECT count(*) FROM custody_position;' 2>/dev/null | tr -d '[:space:]')
  log "  -> $RESULT | pico ${PEAK} MiB | ${LIN} linhas | OutOfMemoryException no log: $(grep -c 'OutOfMemoryException' "$BASE.worker.log")"
  echo "${RESULT}|${PEAK}|${LIN}"
}

K_MUITOS="input/$(basename "$F_MUITOS")"
K_UM="input/$(basename "$F_UM")"
R_MUITOS=$(roda_cenario MUITOS_RG "$K_MUITOS" | tail -1)
R_UM=$(roda_cenario UM_RG "$K_UM" | tail -1)

RES_MUITOS="${R_MUITOS%%|*}"; PICO_MUITOS=$(echo "$R_MUITOS" | cut -d'|' -f2)
RES_UM="${R_UM%%|*}";         PICO_UM=$(echo "$R_UM" | cut -d'|' -f2)

# --- 4. veredito com asercao ---------------------------------------------------
log "==================== VEREDITO (limite ${LIMIT_MB}m) ===================="
log "  MUITOS_RG : $RES_MUITOS (pico ${PICO_MUITOS} MiB)"
log "  UM_RG     : $RES_UM (pico ${PICO_UM} MiB)"

if [[ "$RES_MUITOS" != "ok" ]]; then
  log "  FALHA: o arquivo com VARIOS row groups nao concluiu. O teste nao prova nada assim."
  FALHAS=$((FALHAS+1))
fi
# O UM_RG TEM que falhar POR MEMORIA. `timeout`/`dlq` nao provam a tese e por isso
# nao passam: o unico aceito e oomkilled (kernel, exit 137) ou out_of_memory (excecao).
if [[ "$RES_UM" != "oomkilled" && "$RES_UM" != "out_of_memory" ]]; then
  log "  FALHA: o arquivo de UM row group nao falhou por memoria (resultado: '$RES_UM')."
  log "         Esperado: 'oomkilled' (kernel, exit 137, sem excecao) OU 'out_of_memory'"
  log "         (System.OutOfMemoryException antes do cgroup matar)."
  log "         'timeout'/'dlq' nao demonstram o piso do row group. Verifique o --limit-mb e o log."
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
