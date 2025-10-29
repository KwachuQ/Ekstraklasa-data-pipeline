#!/bin/bash

echo "🧪 Starting comprehensive smoke test for all services..."
echo "============================================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to test service health
test_service() {
    local service_name=$1
    local test_command=$2
    
    echo -e "\n🔍 Testing ${service_name}..."
    
    if eval "$test_command"; then
        echo -e "${GREEN}✅ ${service_name} - PASS${NC}"
        return 0
    else
        echo -e "${RED}❌ ${service_name} - FAIL${NC}"
        return 1
    fi
}

# Test PostgreSQL
test_postgres() {
    docker exec postgres pg_isready -U airflow -d dwh > /dev/null 2>&1
}

# Test MinIO
test_minio() {
    curl -f http://localhost:9000/minio/health/live > /dev/null 2>&1
}

# Test Airflow Web Server (Updated for Airflow 3.x)
test_airflow_web() {
    # Try new health endpoint first
    if curl -f http://localhost:8080/api/v2/monitor/health > /dev/null 2>&1; then
        return 0
    fi
    # Fallback to root endpoint
    curl -f http://localhost:8080/ > /dev/null 2>&1
}

# Test Metabase
test_metabase() {
    curl -f http://localhost:3000/api/health > /dev/null 2>&1
}

# Test dbt
test_dbt() {
    docker exec dbt dbt --version > /dev/null 2>&1
}

# Test ETL Worker
test_etl() {
    docker exec etl_worker python -c "import psycopg2, minio; print('ETL dependencies OK')" > /dev/null 2>&1
}

# Run all tests
echo "🚀 Running smoke tests..."

failed_tests=0

test_service "PostgreSQL" "test_postgres" || ((failed_tests++))
test_service "MinIO" "test_minio" || ((failed_tests++))
test_service "Airflow Web Server" "test_airflow_web" || ((failed_tests++))
test_service "Metabase" "test_metabase" || ((failed_tests++))
test_service "dbt" "test_dbt" || ((failed_tests++))
test_service "ETL Worker" "test_etl" || ((failed_tests++))

echo -e "\n============================================================"
echo "📊 SMOKE TEST SUMMARY"
echo "============================================================"

if [ $failed_tests -eq 0 ]; then
    echo -e "${GREEN}🎉 All services are healthy! (6/6 passed)${NC}"
    echo -e "\n📍 Service URLs:"
    echo "  • Airflow Web UI: http://localhost:8080 (airflow/airflow)"
    echo "  • Airflow Health: http://localhost:8080/api/v2/monitor/health"
    echo "  • MinIO Console: http://localhost:9001 (minio/minio123)"
    echo "  • Metabase: http://localhost:3000"
    echo "  • PostgreSQL: localhost:5432 (airflow/airflow/dwh)"
    echo -e "\n🔧 Quick Commands:"
    echo "  • Check status: docker-compose ps"
    echo "  • View logs: docker-compose logs <service-name>"
    echo "  • dbt shell: docker exec -it dbt bash"
    echo "  • ETL shell: docker exec -it etl_worker bash"
    exit 0
else
    echo -e "${RED}💥 ${failed_tests} service(s) failed health check${NC}"
    echo -e "\n🔧 Troubleshooting tips:"
    echo "  • Check container status: docker-compose ps"
    echo "  • View logs: docker-compose logs <service-name>"
    echo "  • Wait a few minutes for services to fully start"
    echo "  • For Airflow issues, check: http://localhost:8080/api/v2/monitor/health"
    exit 1
fi