#!/bin/bash
# =============================================================================
# Complete End-to-End Test
#
# Executa o fluxo completo:
# 1. Setup Docker + PostgreSQL (clean)
# 2. Setup infraestrutura (S3, SNS, SQS)
# 3. Seed da base com dados existentes (opcional)
# 4. Merge job em background (modo continuo)
# 5. Consumer em background (polling SQS)
# 6. Gerar e subir multiplos arquivos Parquet
# 7. Trigger SNS notifications
# 8. Aguardar processamento
# 9. Gerar relatorio
#
# Uso:
#   ./run_complete_test.sh                        # Teste completo padrao
#   ./run_complete_test.sh --keep-docker          # Não recria Docker
#   ./run_complete_test.sh --files 20             # Numero de arquivos Parquet
#   ./run_complete_test.sh --records-per-file 5000 # Registros por arquivo
#   ./run_complete_test.sh --existing 100000       # Registros existentes na base
#   ./run_complete_test.sh --no-seed               # Pula seed (teste de ingestão pura)
# =============================================================================

set -e

# =============================================================================
# Configuracoes
# =============================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Defaults
NUM_FILES=10
RECORDS_PER_FILE=5000
EXISTING_RECORDS=100000
MERGE_BATCH_SIZE=2000
MERGE_DELAY=0.5
KEEP_DOCKER=false
DO_SEED=true
CSV_OUTPUT="reports/metrics_complete_$(date +%Y%m%d_%H%M%S).csv"

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
                echo "  --files NUM           Numero de arquivos Parquet (default: $NUM_FILES)"
                echo "  --records-per-file N  Registros por arquivo (default: $RECORDS_PER_FILE)"
                echo "  --existing NUM        Registros existentes (default: $EXISTING_RECORDS)"
                echo "  --batch NUM           Batch size do merge (default: $MERGE_BATCH_SIZE)"
                echo "  --delay NUM           Delay do merge (default: $MERGE_DELAY)"
                echo "  --no-seed             Não faz seed da base (teste de ingestão pura)"
                echo "  --keep-docker         Não recria Docker"
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
    log "Setup infraestrutura S3/SNS/SQS..."
    python3 scripts/setup_infra.py
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

trigger_notifications() {
    source .venv/bin/activate
    log "Triggering SNS notifications for all parquet files..."

    local count=0
    # List all parquet files in S3 and trigger notification for each
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
# Monitoramento
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

        # Detect stagnation
        if [ "$staging_count" = "$last_staging" ] && [ "$staging_count" != "0" ]; then
            stagnant=$((stagnant + 1))
        else
            stagnant=0
        fi
        last_staging=$staging_count

        echo -ne "${CYAN}[MONITOR]${NC} elapsed=${elapsed}s staging=${staging_count} principal=${principal_count} errors=${error_count} stagnant=${stagnant}   \r"

        # Check if staging is empty (all merged)
        if [ "$staging_count" = "0" ] && [ $elapsed -gt 30 ]; then
            echo ""
            success "Staging table vazia - merge completo!"
            return 0
        fi

        # Check if stuck (staging not changing for 60 seconds)
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
# Coleta metricas e relatorio
# =============================================================================
collect_and_report() {
    source .venv/bin/activate

    log "Coletando metricas finais..."

    # Count records
    local staging_final=$(run_psql -t -c "SELECT COUNT(*) FROM custody_position_staging" | tr -d ' ')
    local principal_final=$(run_psql -t -c "SELECT COUNT(*) FROM custody_position" | tr -d ' ')
    local error_final=$(run_psql -t -c "SELECT COUNT(*) FROM custody_position_error" | tr -d ' ')

    local expected_total=$((NUM_FILES * RECORDS_PER_FILE))

    # Write CSV with metrics
    cat > "$CSV_OUTPUT" << METRICSEOF
metric,value
total_files,$NUM_FILES
records_per_file,$RECORDS_PER_FILE
total_records_expected,$expected_total
staging_final,$staging_final
principal_final,$principal_final
error_final,$error_final
existing_records,$EXISTING_RECORDS
merge_batch_size,$MERGE_BATCH_SIZE
merge_delay,$MERGE_DELAY
METRICSEOF

    success "Metricas salvas em $CSV_OUTPUT"

    # Generate HTML report
    log "Gerando relatorio HTML..."
    local report_output="${CSV_OUTPUT%.csv}_report.html"
    python3 scripts/generate_report.py "$CSV_OUTPUT" 2>/dev/null

    if [ -f "metrics_report.html" ]; then
        mv metrics_report.html "$report_output"
        success "Relatorio HTML: $report_output"
    fi

    # Print summary
    echo ""
    echo "=============================================="
    echo "  RESULTADO DO TESTE COMPLETO"
    echo "=============================================="
    echo "  Arquivos processados:   $NUM_FILES"
    echo "  Registros por arquivo:  $RECORDS_PER_FILE"
    echo "  Total registros:        $expected_total"
    echo "  Registros existentes:   $EXISTING_RECORDS"
    echo "  Staging (restante):     $staging_final"
    echo "  Principal (final):      $principal_final"
    echo "  Erros:                  $error_final"
    echo "  Validos inseridos:      $((principal_final - EXISTING_RECORDS))"
    echo "=============================================="
}

# =============================================================================
# Main
# =============================================================================
main() {
    echo ""
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║         COMPLETE END-TO-END TEST                        ║"
    echo "║         Parquet -> SNS -> Consumer -> Staging -> Merge  ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo ""

    parse_args "$@"

    # Cria diretorio de reports
    mkdir -p reports

    # Setup
    check_prereqs
    setup_docker
    setup_python
    setup_database
    setup_infra

    # CRITICAL: Clear ALL tables before starting (prevents unique constraint violations)
    clear_all_tables

    if [ "$DO_SEED" = true ]; then
        seed_database
    fi

    # Generate and upload parquets FIRST
    generate_and_upload_parquets

    # THEN start merge and consumer (in order that ensures data is ready)
    # Start merge job in background (continuous mode)
    log "Iniciando merge job em background (modo continuo)..."
    source .venv/bin/activate
    MERGE_BATCH_SIZE=$MERGE_BATCH_SIZE MERGE_DELAY_SECONDS=$MERGE_DELAY \
        python3 scripts/merge_staging.py --continuous &
    MERGE_PID=$!
    success "Merge job started (PID=$MERGE_PID, continuous mode)"

    # Start consumer in background
    log "Iniciando consumer em background..."
    python3 scripts/consume_s3_event.py &
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
    monitor_result=$?

    # Stop consumer first (it will exit when queue is empty)
    log "Parando consumer..."
    kill $CONSUMER_PID 2>/dev/null || true
    sleep 2

    # Stop merge (it's in continuous mode)
    log "Parando merge job..."
    kill $MERGE_PID 2>/dev/null || true
    wait $MERGE_PID 2>/dev/null || true
    success "Merge job stopped"

    # Collect metrics and generate report
    collect_and_report

    if [ $monitor_result -eq 0 ]; then
        echo ""
        success "TESTE COMPLETO COM SUCESSO!"
    else
        echo ""
        warn "TESTE COMPLETO COM PROBLEMAS (estagnou ou timeout)"
    fi

    echo ""
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║         TESTE FINALIZADO                               ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo ""
}

main "$@"
