#!/bin/bash
# filepath: airflow\scripts\export_mart_team_season.sh

# Export mart_team_season_summary to CSV

TIMESTAMP=$(date +"%Y%m%d")
OUTPUT_FILE="mart_team_season_summary_${TIMESTAMP}.csv"

echo "📊 Exporting mart_team_season_summary to CSV..."

docker exec postgres psql -U airflow -d dwh -c "\COPY (
    SELECT * FROM gold.mart_team_season_summary
    ORDER BY season_name DESC, team_name
) TO STDOUT WITH CSV HEADER" > "${OUTPUT_FILE}"

if [ $? -eq 0 ]; then
    echo "✅ Export successful!"
    echo "📁 File: ${OUTPUT_FILE}"
    echo "📏 Rows: $(tail -n +2 ${OUTPUT_FILE} | wc -l)"
else
    echo "❌ Export failed!"
    exit 1
fi