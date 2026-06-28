#!/bin/bash
# =============================================================================
# Load Simulation Runner
# 
# Executa a simulação completa de carga:
# 1. Setup do ambiente (Docker, PostgreSQL) - SEMPRE com clean
# 2. Criação das tabelas
# 3. Seed da base com dados existentes
# 4. Simulação de carga
# 5. Geração de relatório
#
# Uso:
#   ./run_simulation.sh                    # Padrão (clean + execução)
#   ./run_simulation.sh --keep-docker      # Não recria Docker (mais rápido)
#   ./run_simulation.sh --existing 100000  # Quantidade de registros existentes
#   ./run_simulation.sh --ingestion 500000 # Quantidade para ingestão
#   ./run_simulation.sh --batch 2000       # Batch size
#   ./run_simulation.sh --delay 0.5       # Delay entre batches
#   ./run_simulation.sh --report-only     # Apenas gera relatório do último CSV
# =============================================================================

set -e

# =============================================================================
# Descobre o diretório do script e do projeto
# =============================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# =============================================================================
# Configurações Padrão
# =============================================================================
EXISTING_RECORDS=500000
INGESTION_SIZE=1000000
UPDATE_RATIO=60
BATCH_SIZE=2000
DELAY=0.5
KEEP_DOCKER=false
REPORT_ONLY=false
CSV_OUTPUT="metrics_$(date +%Y%m%d_%H%M%S).csv"

# =============================================================================
# Cores para output
# =============================================================================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# =============================================================================
# Parse Argumentos
# =============================================================================
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --keep-docker)
                KEEP_DOCKER=true
                shift
                ;;
            --report-only)
                REPORT_ONLY=true
                shift
                ;;
            --existing)
                EXISTING_RECORDS="$2"
                shift 2
                ;;
            --ingestion)
                INGESTION_SIZE="$2"
                shift 2
                ;;
            --batch)
                BATCH_SIZE="$2"
                shift 2
                ;;
            --delay)
                DELAY="$2"
                shift 2
                ;;
            --output)
                CSV_OUTPUT="$2"
                shift 2
                ;;
            --help|-h)
                echo "Uso: $0 [opções]"
                echo ""
                echo "Opções:"
                echo "  --existing NUM    Registros existentes na base (default: $EXISTING_RECORDS)"
                echo "  --ingestion NUM  Quantidade para ingestão (default: $INGESTION_SIZE)"
                echo "  --batch NUM      Batch size (default: $BATCH_SIZE)"
                echo "  --delay NUM      Delay entre batches em segundos (default: $DELAY)"
                echo "  --output FILE    Nome do arquivo CSV de saída"
                echo "  --keep-docker   Não recria Docker (mais rápido, reuse containers)"
                echo "  --report-only   Apenas gera relatório do último CSV"
                echo "  --help, -h      Mostra esta ajuda"
                exit 0
                ;;
            *)
                error "Argumento desconhecido: $1"
                exit 1
                ;;
        esac
    done
}

# =============================================================================
# Verificações Iniciais
# =============================================================================
check_prerequisites() {
    log "Verificando pré-requisitos..."
    
    # Verifica Docker
    if ! command -v docker &> /dev/null; then
        error "Docker não encontrado. Instale o Docker primeiro."
        exit 1
    fi
    
    # Verifica Docker Compose
    if ! docker compose version &> /dev/null; then
        error "docker compose não encontrado."
        exit 1
    fi
    
    # Verifica Python
    if ! command -v python3 &> /dev/null; then
        error "Python3 não encontrado."
        exit 1
    fi
    
    # Verifica pip
    if ! command -v pip3 &> /dev/null; then
        error "pip3 não encontrado."
        exit 1
    fi
    
    success "Pré-requisitos OK"
}

# =============================================================================
# Executa comando no PostgreSQL (via Docker)
# =============================================================================
run_psql() {
    docker compose exec -T postgres psql -U pocuser -d pocdb "$@"
}

# =============================================================================
# Setup do Ambiente Docker (SEMPRE faz clean)
# =============================================================================
setup_docker() {
    if [ "$KEEP_DOCKER" = true ]; then
        warn "Pulando setup do Docker (--keep-docker)"
        
        # Verifica se está rodando
        if ! docker compose ps &>/dev/null; then
            warn "Docker não está rodando, iniciando..."
            KEEP_DOCKER=false
        else
            log "Reusando containers existentes"
            return
        fi
    fi
    
    log "=============================================="
    log "  LIMPEZA DO AMBIENTE (sempre é clean start)"
    log "=============================================="
    
    # Para e remove containers existentes
    log "Parando containers existentes..."
    docker compose down -v 2>/dev/null || true
    docker compose rm -f 2>/dev/null || true
    
    success "Ambiente limpo!"
    
    # Sobe os serviços frescos
    log "Subindo serviços (docker compose up -d)..."
    docker compose up -d
    
    # Aguarda PostgreSQL estar pronto
    log "Aguardando PostgreSQL iniciar..."
    local max_attempts=30
    local attempt=0
    while [ $attempt -lt $max_attempts ]; do
        if run_psql -c "SELECT 1" &>/dev/null; then
            success "PostgreSQL está pronto!"
            return
        fi
        attempt=$((attempt + 1))
        echo -n "."
        sleep 1
    done
    
    error "PostgreSQL não ficou disponível a tempo"
    exit 1
}

# =============================================================================
# Setup do Ambiente Python
# =============================================================================
setup_python() {
    log "Verificando ambiente Python..."
    
    # Verifica se .venv existe
    if [ ! -d ".venv" ]; then
        log "Criando ambiente virtual..."
        python3 -m venv .venv
    fi
    
    # Ativa virtualenv
    source .venv/bin/activate
    
    # Instala dependências
    log "Instalando dependências Python..."
    pip install --quiet -r requirements.txt
    
    success "Ambiente Python pronto"
}

# =============================================================================
# Setup do Banco de Dados
# =============================================================================
setup_database() {
    log "Setup do banco de dados..."
    
    # Aguarda um pouco para o PostgreSQL estar totalmente pronto
    sleep 2
    
    # Recria as tabelas (sempre)
    log "Criando tabelas..."
    run_psql -f /docker-entrypoint-initdb.d/001_init.sql
    
    success "Banco de dados pronto"
}

# =============================================================================
# Execução da Simulação
# =============================================================================
run_simulation() {
    source .venv/bin/activate
    
    log ""
    log "=============================================="
    log "  INICIANDO SIMULAÇÃO DE CARGA"
    log "=============================================="
    log ""
    log "  Registros existentes: $EXISTING_RECORDS"
    log "  Tamanho ingestão:     $INGESTION_SIZE"
    log "  % Updates:           $UPDATE_RATIO%"
    log "  Batch size:          $BATCH_SIZE"
    log "  Delay entre batches:   ${DELAY}s"
    log "  Output CSV:          $CSV_OUTPUT"
    log ""
    log "=============================================="
    
    # Executa a simulação
    python3 scripts/simulate_load.py \
        --existing-records $EXISTING_RECORDS \
        --ingestion-size $INGESTION_SIZE \
        --update-ratio $UPDATE_RATIO \
        --batch-size $BATCH_SIZE \
        --delay $DELAY \
        --output-csv "$CSV_OUTPUT"
    
    success "Simulação concluída!"
}

# =============================================================================
# Geração de Relatório
# =============================================================================
generate_report() {
    source .venv/bin/activate
    
    # Encontra o último CSV se não especificou
    if [ -z "$CSV_OUTPUT" ] || [ ! -f "$CSV_OUTPUT" ]; then
        CSV_OUTPUT=$(ls -t metrics_*.csv 2>/dev/null | head -1)
        if [ -z "$CSV_OUTPUT" ]; then
            warn "Nenhum arquivo CSV encontrado para gerar relatório"
            return
        fi
        log "Usando CSV mais recente: $CSV_OUTPUT"
    fi
    
    log "Gerando relatório HTML..."
    
    python3 scripts/generate_report.py "$CSV_OUTPUT"
    
    local report_file="${CSV_OUTPUT%.csv}_report.html"
    success "Relatório gerado: $report_file"
    
    # Sugere abrir o relatório
    echo ""
    echo "Para abrir o relatório:"
    echo "  open $report_file"
}

# =============================================================================
# Main
# =============================================================================
main() {
    echo ""
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║         LOAD SIMULATION RUNNER                         ║"
    echo "║         (clean start - sempre recria o ambiente)       ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo ""
    
    parse_args "$@"
    check_prerequisites
    
    if [ "$REPORT_ONLY" = true ]; then
        generate_report
        exit 0
    fi
    
    setup_docker
    setup_python
    setup_database
    run_simulation
    generate_report
    
    echo ""
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║         SIMULAÇÃO CONCLUÍDA COM SUCESSO!                ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo ""
}

main "$@"
