#!/usr/bin/env bash
set -e

# filepath: airflow\scripts\export_gold_tables.sh

# Export all Gold layer tables to CSV files

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_DIR="../../data"

# Create data directory if it doesn't exist
mkdir -p "${OUTPUT_DIR}"

echo "Exporting Gold layer tables to CSV..."
echo "Output directory: ${OUTPUT_DIR}"
echo "Timestamp: ${TIMESTAMP}"
echo ""

# Get list of all tables in gold schema
TABLES=$(docker exec postgres psql -U airflow -d dwh -t -c "
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'gold' 
    AND table_type = 'BASE TABLE'
    ORDER BY table_name;
")

# Counter for success/failure
SUCCESS_COUNT=0
FAIL_COUNT=0

# Export each table
for TABLE in $TABLES; do
    # Remove leading/trailing whitespace
    TABLE=$(echo "$TABLE" | xargs)
    
    if [ -n "$TABLE" ]; then
        OUTPUT_FILE="${OUTPUT_DIR}/${TABLE}_${TIMESTAMP}.csv"
        
        echo "Exporting: gold.${TABLE}..."
        
        docker exec postgres psql -U airflow -d dwh -c "\COPY (
            SELECT * FROM gold.${TABLE}
        ) TO STDOUT WITH CSV HEADER" > "${OUTPUT_FILE}"
        
        if [ $? -eq 0 ]; then
            ROW_COUNT=$(tail -n +2 "${OUTPUT_FILE}" | wc -l)
            echo "   Success! Rows: ${ROW_COUNT}"
            echo "   File: ${OUTPUT_FILE}"
            ((SUCCESS_COUNT++))
        else
            echo "   Failed to export ${TABLE}"
            ((FAIL_COUNT++))
            # Remove empty file if export failed
            rm -f "${OUTPUT_FILE}"
        fi
        echo ""
    fi
done

# Summary
echo "========================================"
echo "Export Summary"
echo "========================================"
echo "Successfully exported: ${SUCCESS_COUNT} table(s)"
echo "Failed exports: ${FAIL_COUNT} table(s)"
echo "Output directory: ${OUTPUT_DIR}"
echo "========================================"

if [ $FAIL_COUNT -gt 0 ]; then
    exit 1
fi