#!/bin/bash
# ============================================
# profile_raw_data.sh
# Purpose: Profile all raw CSV files before
#          pipeline execution
# Usage: ./scripts/profile_raw_data.sh
# ============================================

set -euo pipefail
# set -e → exit immediately on any error
# set -u → treat unset variables as error
# set -o pipefail → catch errors in pipes

RAW_DIR="./raw"
LOG_DIR="./logs"
REPORT="$LOG_DIR/raw_profile_$(date +%Y%m%d_%H%M%S).txt"

mkdir -p "$LOG_DIR"

echo "======================================" | tee "$REPORT"
echo "RAW DATA PROFILE REPORT"               | tee -a "$REPORT"
echo "Generated: $(date)"                    | tee -a "$REPORT"
echo "======================================" | tee -a "$REPORT"

for file in "$RAW_DIR"/*.csv; do
    filename=$(basename "$file")
    echo ""                                  | tee -a "$REPORT"
    echo "File: $filename"                   | tee -a "$REPORT"
    echo "--------------------------------------" | tee -a "$REPORT"

    # Total lines including header
    total_lines=$(wc -l < "$file")
    data_rows=$((total_lines - 1))
    echo "Total rows (excl header): $data_rows" | tee -a "$REPORT"

    # Column count from header
    col_count=$(head -1 "$file" | tr ',' '\n' | wc -l)
    echo "Column count: $col_count"          | tee -a "$REPORT"

    # File size
    size=$(du -sh "$file" | cut -f1)
    echo "File size: $size"                  | tee -a "$REPORT"

    # Header row
    echo "Columns: $(head -1 "$file")"       | tee -a "$REPORT"

    # Count empty/null-like fields
    empty_count=$(grep -o ',,' "$file" | wc -l || true)
    echo "Empty field occurrences: $empty_count" | tee -a "$REPORT"
done

echo ""                                      | tee -a "$REPORT"
echo "Profile complete. Report saved to:"   | tee -a "$REPORT"
echo "$REPORT"                              | tee -a "$REPORT"

# ---- end of script ----