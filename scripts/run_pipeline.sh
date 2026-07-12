#!/bin/bash
# ============================================
# run_pipeline.sh
# Purpose: Orchestrate the full Olist pipeline
# Usage  : ./scripts/run_pipeline.sh [env]
# Example: ./scripts/run_pipeline.sh dev
# ============================================

set -euo pipefail

ENV=${1:-dev}
CONFIG_FILE="config.env"
# CONFIG_FILE="../config.env"
LOG_DIR="./logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
PIPELINE_LOG="$LOG_DIR/pipeline_${TIMESTAMP}.log"

mkdir -p "$LOG_DIR"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" |
    tee -a "$PIPELINE_LOG"
}

check_success() {
    if [ $? -ne 0 ]; then
        log "ERROR $1 failed. Aborting."
        exit 1
    fi
    log "SUCCESS: $1 completed."
}

# ─────────────────────────────────────────
log "============================================"
log "OLIST PIPELINE STARTED | ENV: $ENV"
log "============================================"

# Load environment
source "$CONFIG_FILE"
log "Config loaded from $CONFIG_FILE"
log "Project: $PROJECT_NAME"
log "Raw path: $RAW_PATH"


# Activate venv
# source venv/bin/activate
source venv_linux/bin/activate
log "Virtual environment activated"

# Phase 1: Profile raw data
log "PHASE 1: Raw data profiling"
# python3 src/ingestion/profiler.py
python -m src.ingestion.profiler
check_success "Data profiling"

# Phase 2: PySpark transformation
log "PHASE 2: PySpark transformation"
# python3 src/transform/spark_pipeline.py
python -m src.transform.spark_pipeline
check_success "Spark transformation"

# Phase 3: Copy reports to archive
log "PHASE 3: Archiving outputs"
ARCHIVE_DIR="./archive/run_$TIMESTAMP"
mkdir -p "$ARCHIVE_DIR"
cp -r reports/ "$ARCHIVE_DIR/"
cp -r logs/ "$ARCHIVE_DIR/"
check_success "Archiving"

# Phase 4: Compress archive
log "PHASE 4: Compressing archive"
tar -czf "${ARCHIVE_DIR}.tar.gz" \
    -C archive "run_$TIMESTAMP"
rm -rf "$ARCHIVE_DIR"
check_success "Compression"

log "============================================"
log "PIPELINE COMPLETE"
log "Archive: ${ARCHIVE_DIR}.tar.gz"
log "Log: $PIPELINE_LOG"
log "============================================"

# ---- end of script ----