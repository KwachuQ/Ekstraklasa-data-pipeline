#!/usr/bin/env bash

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_DIR="../../data"

mkdir -p "${OUTPUT_DIR}"

echo "Exporting Gold layer tables and views to CSV..."
echo "Output directory: ${OUTPUT_DIR}"
echo "Timestamp: ${TIMESTAMP}"
echo ""

# Get list of all tables and views into an array
mapfile -t TABLES < <(docker exec postgres psql -U airflow -d dwh -t -A -c "
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'gold' 
    AND table_type IN ('BASE TABLE', 'VIEW')
    ORDER BY table_name;
" | tr -d '\r' | grep -v '^$')

echo "Found ${#TABLES[@]} tables/views:"
printf '%s\n' "${TABLES[@]}" | nl
echo ""

SUCCESS_COUNT=0
FAIL_COUNT=0

# Export each table/view
for i in "${!TABLES[@]}"; do
    TABLE="${TABLES[$i]}"
    
    if [ -n "$TABLE" ]; then
        OUTPUT_FILE="${OUTPUT_DIR}/${TABLE}_${TIMESTAMP}.csv"
        
        echo "[$((i+1))/${#TABLES[@]}] Exporting gold.${TABLE}..."
        
        docker exec postgres psql -U airflow -d dwh -c "\COPY (SELECT * FROM gold.${TABLE}) TO STDOUT WITH CSV HEADER" > "${OUTPUT_FILE}" 2>&1
        EXIT_CODE=$?
        
        if [ $EXIT_CODE -eq 0 ]; then
            ROW_COUNT=$(tail -n +2 "${OUTPUT_FILE}" | wc -l)
            echo "   ✓ Success! Rows: ${ROW_COUNT}"
            ((SUCCESS_COUNT++))
        else
            echo "   ✗ Failed (exit code: $EXIT_CODE)"
            ((FAIL_COUNT++))
            rm -f "${OUTPUT_FILE}"
        fi
        echo ""
    fi
done

echo "========================================"
echo "Export Summary"
echo "========================================"
echo "Successfully exported: ${SUCCESS_COUNT}"
echo "Failed exports: ${FAIL_COUNT}"
echo "Output directory: ${OUTPUT_DIR}"
echo "========================================"