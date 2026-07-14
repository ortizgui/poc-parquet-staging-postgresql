#!/bin/bash
# =============================================================================
# Complete End-to-End Test
#
# Fluxos:
#
#   --target direct (padrao, producao):
#     1. Setup Docker + PostgreSQL (clean)
#     2. Setup infra (S3 + SQS + S3 Notification)
#     3. Seed da base com registros existentes (opcional)
#     4. Iniciar N consumers em background (processando)
#     5. Gerar e fazer upload dos parquets COM DELAY (enquanto consumers rodam)
#     6. Monitorar SQS depth + custody_position count
#     7. Quando SQS vazia + principal estavel, concluir
#     8. Parar consumers e gerar relatorio
#
#   --target staging (backward compat):
#     Fluxo original: parquets -> SNS -> consumer -> staging -> merge -> principal
#
# Uso:
#   ./run_complete_test.sh                                  # Teste padrao (direct)
#   ./run_complete_test.sh --target direct                  # Explícito
#   ./run_complete_test.sh --target staging                 # Fluxo staging original
#   ./run_complete_test.sh --files 20                       # Numero de arquivos
#   ./run_complete_test.sh --consumers 3                    # Consumers paralelos
#   ./run_complete_test.sh --records-per-file 5000          # Registros por arquivo
#   ./run_complete_test.sh --existing 100000                # Registros existentes
#   ./run_complete_test.sh --no-seed                        # Pula seed
#   ./run_complete_test.sh --keep-docker                    # Nao recria Docker
# =============================================================================

set -e

# =============================================================================
# Configuracoes
# =============================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Defaults
TARGET="direct"
NUM_CONSUMERS=2
NUM_FILES=10
RECORDS_PER_FILE=5000
EXISTING_RECORDS=100000
MERGE_BATCH_SIZE=2000
MERGE_DELAY=0.5
KEEP_DOCKER=false
DO_SEED=true
CSV_OUTPUT="reports/metrics_complete_$(date +%Y%m%d_%H%M%S).csv"
CSV_METRICS="reports/metrics_batches_$(date +%Y%m%d_%H%M%S).csv"

# Cores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

log() { echo -e "${BLUE}[INFO]${NC} $1"; }
success() { echo -e "${GREEN}[OK]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; }

# =============================================================================
# Parse Argumentos
# =============================================================================
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --target) TARGET="$2"; shift 2 ;;
            --consumers) NUM_CONSUMERS="$2"; shift 2 ;;
            --keep-docker) KEEP_DOCKER=true; shift ;;
            --files) NUM_FILES="$2"; shift 2 ;;
            --records-per-file) RECORDS_PER_FILE="$2"; shift 2 ;;
            --existing) EXISTING_RECORDS="$2"; shift 2 ;;
            --batch) MERGE_BATCH_SIZE="$2"; shift 2 ;;
            --delay) MERGE_DELAY="$2"; shift 2 ;;
            --output) CSV_OUTPUT="reports/$2"; shift 2 ;;
            --no-seed) DO_SEED=false; shift ;;
            --help|-h)
                echo "Uso: $0 [opcoes]"
                echo "  --target {staging|direct}   Fluxo (default: direct)"
                echo "  --consumers N               Consumers paralelos (default: 2)"
                echo "  --files NUM                 Arquivos Parquet (default: $NUM_FILES)"
                echo "  --records-per-file N        Registros por arquivo (default: $RECORDS_PER_FILE)"
                echo "  --existing NUM              Registros existentes (default: $EXISTING_RECORDS)"
                echo "  --batch NUM                 Batch size do merge (staging only)"
                echo "  --delay NUM                 Delay do merge (staging only)"
                echo "  --no-seed                   Pula seed da base"
                echo "  --keep-docker               Não recria Docker"
                exit 0
                ;;
            *) error "Unknown: $1"; exit 1 ;;
        esac
    done
}

# =============================================================================
# Verificacoes
# =============================================================================
check_prereqs() {
    log "Verificando prerequisitos..."
    command -v docker &>/dev/null || { error "Docker nao encontrado"; exit 1; }
    docker compose version &>/dev/null || { error "docker compose nao encontrado"; exit 1; }
    command -v python3 &>/dev/null || { error "Python3 nao encontrado"; exit 1; }
    success "Prerequisitos OK"
}

# =============================================================================
# Docker helpers
# =============================================================================
run_psql() {
    docker compose exec -T postgres psql -U pocuser -d pocdb "$@"
}

setup_docker() {
    if [ "$KEEP_DOCKER" = true ]; then
        if docker compose ps &>/dev/null; then
            log "Reusando containers existentes (--keep-docker)"
            return
        fi
    fi

    log "=============================================="
    log "  CLEAN START - Recriando ambiente Docker"
    log "=============================================="

    docker compose down -v 2>/dev/null || true
    docker compose rm -f 2>/dev/null || true

    log "Subindo servicos..."
    docker compose up -d

    log "Aguardando PostgreSQL..."
    local max_attempts=30
    for i in $(seq 1 $max_attempts); do
        if run_psql -c "SELECT 1" &>/dev/null; then
            success "PostgreSQL pronto"
            return
        fi
        echo -n "."
        sleep 1
    done
    error "PostgreSQL nao ficou disponivel"
    exit 1
}

setup_python() {
    log "Setup ambiente Python..."
    if [ ! -d ".venv" ]; then
        python3 -m venv .venv
    fi
    source .venv/bin/activate
    pip install --quiet -r requirements.txt
    success "Python pronto"
}

setup_database() {
    log "Criando tabelas..."
    run_psql -f /docker-entrypoint-initdb.d/001_init.sql
    success "Tabelas criadas"
}

setup_infra() {
    source .venv/bin/activate
    log "Setup infraestrutura S3/SQS/S3 Notification..."
    if [ "$TARGET" = "direct" ]; then
        python3 scripts/setup_infra.py
    else
        python3 scripts/setup_infra.py --sns
    fi
    success "Infraestrutura pronta"
}

seed_database() {
    if [ "$DO_SEED" = false ]; then
        warn "Pulando seed (--no-seed)"
        return
    fi

    source .venv/bin/activate
    log "Seed principal table com $EXISTING_RECORDS registros..."
    python3 scripts/seed_database.py --records $EXISTING_RECORDS
    success "Seed completo"
}

clear_all_tables() {
    log "Limpando TODAS as tabelas (clean start)..."
    run_psql -c "TRUNCATE custody_position_staging CASCADE;" 2>/dev/null || true
    run_psql -c "TRUNCATE custody_position_error CASCADE;" 2>/dev/null || true
    run_psql -c "TRUNCATE custody_position CASCADE;" 2>/dev/null || true
    success "Todas as tabelas limpas"
}

# =============================================================================
# Gerar e subir Parquets
# =============================================================================
generate_and_upload_parquets() {
    source .venv/bin/activate
    log "=============================================="
    log "  Gerando $NUM_FILES arquivos Parquet"
    log "  ($RECORDS_PER_FILE registros cada)"
    log "=============================================="

    python3 scripts/generate_unique_test_data.py \
        --files $NUM_FILES \
        --records-per-file $RECORDS_PER_FILE \
        --prefix "input/" \
        --upload

    local total_records=$((NUM_FILES * RECORDS_PER_FILE))
    success "Gerados $NUM_FILES arquivos ($total_records total registros)"
}

generate_and_upload_parquets_with_delay() {
    source .venv/bin/activate
    log "=============================================="
    log "  Gerando e subindo $NUM_FILES arquivos UM POR UM (delay 2s)"
    log "  ($RECORDS_PER_FILE registros cada)"
    log "=============================================="

    local upload_start=$(date +%s)

    for i in $(seq 1 $NUM_FILES); do
        local prefix="input/test_part_"

        python3 scripts/generate_unique_test_data.py \
            --files 1 \
            --records-per-file $RECORDS_PER_FILE \
            --prefix "input/direct_part_" \
            --upload

        echo -ne "${CYAN}[UPLOAD]${NC} File $i/$NUM_FILES uploaded   \r"

        if [ $i -lt $NUM_FILES ]; then
            sleep 2
        fi
    done

    echo ""
    local upload_end=$(date +%s)
    local upload_elapsed=$((upload_end - upload_start))
    success "Upload completo: $NUM_FILES arquivos em ${upload_elapsed}s"
}

trigger_notifications() {
    source .venv/bin/activate
    log "Triggering SNS notifications for all parquet files..."

    local count=0
    local parquet_files=$(python3 -c "
import boto3
import os
from dotenv import load_dotenv
load_dotenv('.env')

s3 = boto3.client('s3',
    endpoint_url=os.getenv('AWS_ENDPOINT_URL'),
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)

resp = s3.list_objects_v2(Bucket='poc-bucket', Prefix='input/')
keys = [obj['Key'] for obj in resp.get('Contents', []) if obj['Key'].endswith('.parquet')]
print('|'.join(keys))
" 2>/dev/null)

    IFS='|' read -ra PARQUET_ARRAY <<< "$parquet_files"
    for key in "${PARQUET_ARRAY[@]}"; do
        if [ -n "$key" ]; then
            python3 scripts/simulate_s3_notification.py \
                --bucket poc-bucket \
                --key "$key" \
                --topic poc-notification-topic 2>/dev/null
            count=$((count + 1))
            echo -n "."
        fi
    done
    echo ""
    success "Triggered $count SNS notifications"
}

# =============================================================================
# Staging flow monitoring (backward compat)
# =============================================================================
wait_for_staging_data() {
    log "Aguardando dados chegarem na staging..."
    local max_wait=120
    local elapsed=0

    while [ $elapsed -lt $max_wait ]; do
        local staging_count=$(run_psql -t -c "SELECT COUNT(*) FROM custody_position_staging" 2>/dev/null | tr -d ' ')

        if [ "$staging_count" -gt 0 ]; then
            success "Staging tem $staging_count registros!"
            return 0
        fi

        echo -ne "${CYAN}[WAIT]${NC} elapsed=${elapsed}s staging=$staging_count   \r"
        sleep 5
        elapsed=$((elapsed + 5))
    done

    echo ""
    warn "Timeout esperando dados na staging"
    return 1
}

monitor_staging() {
    log "Monitorando staging table..."
    local max_wait=600
    local elapsed=0
    local last_staging=0
    local stagnant=0

    while [ $elapsed -lt $max_wait ]; do
        local staging_count=$(run_psql -t -c "SELECT COUNT(*) FROM custody_position_staging" 2>/dev/null | tr -d ' ')
        local principal_count=$(run_psql -t -c "SELECT COUNT(*) FROM custody_position" 2>/dev/null | tr -d ' ')
        local error_count=$(run_psql -t -c "SELECT COUNT(*) FROM custody_position_error" 2>/dev/null | tr -d ' ')

        if [ "$staging_count" = "$last_staging" ] && [ "$staging_count" != "0" ]; then
            stagnant=$((stagnant + 1))
        else
            stagnant=0
        fi
        last_staging=$staging_count

        echo -ne "${CYAN}[MONITOR]${NC} elapsed=${elapsed}s staging=${staging_count} principal=${principal_count} errors=${error_count} stagnant=${stagnant}   \r"

        if [ "$staging_count" = "0" ] && [ $elapsed -gt 30 ]; then
            echo ""
            success "Staging table vazia - merge completo!"
            return 0
        fi

        if [ $stagnant -ge 12 ]; then
            echo ""
            warn "Staging estagnou em $staging_count registros por 60s"
            return 1
        fi

        sleep 5
        elapsed=$((elapsed + 5))
    done

    echo ""
    warn "Timeout esperando staging vazia (${max_wait}s)"
    return 1
}

# =============================================================================
# Direct flow monitoring (production mode)
# =============================================================================
monitor_direct_flow() {
    log "Monitorando fluxo direct (SQS depth + principal count)..."

    local max_wait=600
    local elapsed=0
    local last_principal=0
    local stable_count=0
    local max_sqs_depth=0

    while [ $elapsed -lt $max_wait ]; do
        local principal_count=$(run_psql -t -c "SELECT COUNT(*) FROM custody_position" 2>/dev/null | tr -d ' ')

        # Query SQS depth from LocalStack
        local sqs_depth=$(docker compose exec -T ministack \
            aws --endpoint-url=http://localhost:4566 sqs get-queue-attributes \
            --queue-url "http://localhost:4566/000000000000/poc-notification-queue" \
            --attribute-names ApproximateNumberOfMessages \
            --query "Attributes.ApproximateNumberOfMessages" \
            --output text 2>/dev/null || echo "?")

        if [ "$sqs_depth" != "?" ] && [ "$sqs_depth" -gt "$max_sqs_depth" ] 2>/dev/null; then
            max_sqs_depth=$sqs_depth
        fi

        # Stability detection
        if [ "$principal_count" = "$last_principal" ] && [ "$principal_count" != "0" ]; then
            stable_count=$((stable_count + 1))
        else
            stable_count=0
        fi
        last_principal=$principal_count

        echo -ne "${CYAN}[MONITOR]${NC} elapsed=${elapsed}s principal=${principal_count} sqs_depth=${sqs_depth} stable=${stable_count}   \r"

        # Done when SQS is empty and principal stable for 30s (6 cycles)
        if [ "$sqs_depth" = "0" ] && [ $stable_count -ge 6 ] && [ $elapsed -gt 30 ]; then
            echo ""
            success "Fluxo direct concluido: SQS vazia + principal estavel por 30s"
            echo "MAX_SQS_DEPTH=$max_sqs_depth"
            return 0
        fi

        sleep 5
        elapsed=$((elapsed + 5))
    done

    echo ""
    warn "Timeout esperando conclusao do fluxo direct (${max_wait}s)"
    echo "MAX_SQS_DEPTH=$max_sqs_depth"
    return 1
}

# =============================================================================
# Coleta metricas e relatorio
# =============================================================================
collect_and_report_staging() {
    source .venv/bin/activate

    log "Coletando metricas finais (staging)..."
    local staging_final=$(run_psql -t -c "SELECT COUNT(*) FROM custody_position_staging" | tr -d ' ')
    local principal_final=$(run_psql -t -c "SELECT COUNT(*) FROM custody_position" | tr -d ' ')
    local error_final=$(run_psql -t -c "SELECT COUNT(*) FROM custody_position_error" | tr -d ' ')
    local expected_total=$((NUM_FILES * RECORDS_PER_FILE))
    local inserted_new=$((principal_final - EXISTING_RECORDS))

    echo ""
    echo "=============================================="
    echo "  RESULTADO DO TESTE (STAGING)"
    echo "=============================================="
    echo "  Arquivos processados:   $NUM_FILES"
    echo "  Registros por arquivo:  $RECORDS_PER_FILE"
    echo "  Total registros:        $expected_total"
    echo "  Registros existentes:   $EXISTING_RECORDS"
    echo "  Staging (restante):     $staging_final"
    echo "  Principal (final):     $principal_final"
    echo "  Erros:                  $error_final"
    echo "  Novos inseridos:       $inserted_new"
    echo "=============================================="
}

collect_and_report_direct() {
    source .venv/bin/activate

    log "Coletando metricas finais (direct)..."
    local principal_final=$(run_psql -t -c "SELECT COUNT(*) FROM custody_position" | tr -d ' ')
    local error_final=$(run_psql -t -c "SELECT COUNT(*) FROM custody_position_error" | tr -d ' ')
    local expected_total=$((NUM_FILES * RECORDS_PER_FILE))
    local inserted_estimated=$((principal_final - EXISTING_RECORDS))

    echo ""
    echo "=============================================="
    echo "  RESULTADO DO TESTE (DIRECT)"
    echo "=============================================="
    echo "  Arquivos processados:   $NUM_FILES"
    echo "  Consumers:               $NUM_CONSUMERS"
    echo "  Registros por arquivo:  $RECORDS_PER_FILE"
    echo "  Total registros:        $expected_total"
    echo "  Registros existentes:   $EXISTING_RECORDS"
    echo "  Principal (final):     $principal_final"
    echo "  Erros:                  $error_final"
    echo "  Estimativa inseridos:  $inserted_estimated"
    echo "  Max SQS depth:          ${MAX_SQS_DEPTH:-N/A}"
    echo "=============================================="
}

# =============================================================================
# Staging flow (backward compat)
# =============================================================================
run_staging_flow() {
    echo ""
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║    COMPLETE END-TO-END TEST (STAGING FLOW)              ║"
    echo "║    Parquet -> SNS -> Consumer -> Staging -> Merge       ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo ""

    # CRITICAL: Clear ALL tables before starting
    clear_all_tables

    if [ "$DO_SEED" = true ]; then
        seed_database
    fi

    # Generate and upload parquets FIRST
    generate_and_upload_parquets

    # Start merge job in background (continuous mode)
    log "Iniciando merge job em background (modo continuo)..."
    source .venv/bin/activate
    MERGE_BATCH_SIZE=$MERGE_BATCH_SIZE MERGE_DELAY_SECONDS=$MERGE_DELAY \
        python3 scripts/merge_staging.py --continuous --metrics-csv "$CSV_METRICS" &
    MERGE_PID=$!
    success "Merge job started (PID=$MERGE_PID, continuous mode)"

    # Start consumer in background (staging target)
    log "Iniciando consumer em background (target=staging)..."
    python3 scripts/consume_s3_event.py --target staging --consumer-id "test-staging" &
    CONSUMER_PID=$!
    success "Consumer started (PID=$CONSUMER_PID)"

    # Trigger SNS notifications (AFTER consumer is listening)
    trigger_notifications

    # Wait for staging to have data
    wait_for_staging_data || true

    # Monitor until staging is empty
    echo ""
    log "Aguardando processamento (timeout 10min)..."
    monitor_staging
    local monitor_result=$?

    # Stop consumer
    log "Parando consumer..."
    kill $CONSUMER_PID 2>/dev/null || true
    sleep 2

    # Stop merge
    log "Parando merge job..."
    kill $MERGE_PID 2>/dev/null || true
    wait $MERGE_PID 2>/dev/null || true
    success "Merge job stopped"

    # Collect metrics and generate report
    collect_and_report_staging

    if [ $monitor_result -eq 0 ]; then
        echo ""
        success "TESTE COMPLETO COM SUCESSO!"
    else
        echo ""
        warn "TESTE COMPLETO COM PROBLEMAS (estagnou ou timeout)"
    fi
}

# =============================================================================
# Direct flow (production mode)
# =============================================================================
run_direct_flow() {
    echo ""
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║    COMPLETE END-TO-END TEST (DIRECT FLOW)               ║"
    echo "║    Parquet -> S3 -> SQS -> Consumer -> custody_position  ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo ""

    # CRITICAL: Clear ALL tables before starting
    clear_all_tables

    if [ "$DO_SEED" = true ]; then
        seed_database
    fi

    # Start N consumers in background BEFORE uploading files
    declare -a CONSUMER_PIDS=()
    for (( c=1; c<=NUM_CONSUMERS; c++ )); do
        log "Iniciando consumer $c/$NUM_CONSUMERS em background (target=direct)..."
        source .venv/bin/activate
        python3 scripts/consume_s3_event.py \
            --target direct \
            --consumer-id "test-direct-$c" &
        local pid=$!
        CONSUMER_PIDS+=($pid)
        success "Consumer $c started (PID=$pid)"
    done

    # Give consumers time to start listening
    sleep 3

    # Record start time
    local flow_start=$(date +%s)

    # Generate and upload parquets WITH DELAY (one by one while consumers process)
    generate_and_upload_parquets_with_delay

    # Monitor direct flow: SQS depth + principal count
    echo ""
    log "Aguardando processamento (timeout 10min)..."
    monitor_direct_flow
    local monitor_result=$?

    local flow_end=$(date +%s)
    local flow_elapsed=$((flow_end - flow_start))

    # Stop all consumers
    log "Parando ${NUM_CONSUMERS} consumers..."
    for pid in "${CONSUMER_PIDS[@]}"; do
        kill $pid 2>/dev/null || true
    done
    sleep 2
    success "Consumers parados"

    # Collect metrics
    collect_and_report_direct

    # Calculate throughput
    local expected_total=$((NUM_FILES * RECORDS_PER_FILE))
    if [ $flow_elapsed -gt 0 ]; then
        local throughput=$(echo "scale=1; $expected_total / $flow_elapsed" | bc 2>/dev/null || echo "N/A")
        echo "  Tempo total:            ${flow_elapsed}s"
        echo "  Throughput:             ${throughput} reg/s"
    fi

    if [ $monitor_result -eq 0 ]; then
        echo ""
        success "TESTE COMPLETO COM SUCESSO!"
    else
        echo ""
        warn "TESTE COMPLETO COM PROBLEMAS (timeout ou estagnou)"
    fi
}

# =============================================================================
# Main
# =============================================================================
main() {
    parse_args "$@"

    # Criar diretorio de reports
    mkdir -p reports

    # Setup
    check_prereqs
    setup_docker
    setup_python
    setup_database
    setup_infra

    if [ "$TARGET" = "direct" ]; then
        run_direct_flow
    else
        run_staging_flow
    fi

    echo ""
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║         TESTE FINALIZADO                               ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo ""
}

main "$@"
