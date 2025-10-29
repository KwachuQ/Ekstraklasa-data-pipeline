#!/bin/bash
# filepath: check_dag_health.sh
echo "[INFO] >>> Validating ekstraklasa_backfill.py DAG <<<"

cd ../docker

# Disable Git Bash path conversion
export MSYS_NO_PATHCONV=1

# Set RAPIDAPI_KEY to avoid warnings
export RAPIDAPI_KEY=23326581afmsha80927a889f5d69p1c1e08jsnb6e742b99b4a

# Check if DAG file exists in container
echo "[INFO] Checking DAG file in container..."
docker-compose exec airflow-webserver test -f /opt/airflow/dags/ekstraklasa_backfill.py
if [ $? -eq 0 ]; then
    echo "[SUCCESS] DAG file found in container"
else
    echo "[ERROR] DAG file not found in container"
    exit 1
fi

# Test Python syntax inside container
echo "[INFO] Checking Python syntax..."
docker-compose exec airflow-webserver python -m py_compile /opt/airflow/dags/ekstraklasa_backfill.py
if [ $? -eq 0 ]; then
    echo "[SUCCESS] Python syntax is valid"
else
    echo "[ERROR] Python syntax errors found"
    exit 1
fi

# Test DAG imports inside container
echo "[INFO] Testing DAG imports..."
docker-compose exec airflow-webserver python -c "
import sys
sys.path.append('/opt/airflow/dags')
try:
    import ekstraklasa_backfill
    print('[SUCCESS] DAG imports successfully')
except Exception as e:
    print(f'[ERROR] Import failed: {e}')
    import traceback
    traceback.print_exc()
    sys.exit(1)
"

# List all DAGs to see if it's detected
echo "[INFO] Listing all detected DAGs..."
docker-compose exec airflow-webserver airflow dags list | grep -E "(ekstraklasa|init_minio)" && echo "[SUCCESS] DAG detected!" || echo "[WARNING] No matching DAGs found"

# If not found, force DAG refresh
if ! docker-compose exec airflow-webserver airflow dags list | grep -q ekstraklasa; then
    echo "[INFO] Forcing DAG refresh..."
    docker-compose exec airflow-webserver airflow dags reserialize
    
    # Wait and check again
    sleep 5
    docker-compose exec airflow-webserver airflow dags list | grep ekstraklasa && echo "[SUCCESS] DAG detected after refresh!" || echo "[WARNING] DAG still not detected"
fi

# Check scheduler logs for any DAG processing errors
echo "[INFO] Checking scheduler logs for DAG processing errors..."
docker-compose logs --tail=10 airflow-scheduler | grep -i "ekstraklasa\|error\|failed" || echo "[INFO] No recent errors found"

echo "[INFO] Validation completed"