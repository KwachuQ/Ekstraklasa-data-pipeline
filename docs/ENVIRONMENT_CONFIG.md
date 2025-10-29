# Environment Configuration Examples

## 🏭 Production Environment

### .env.production
```bash
# RapidAPI Configuration
RAPIDAPI_KEY=prod_key_here
RAPIDAPI_HOST=sofascore.p.rapidapi.com
RAPIDAPI_REQUEST_TIMEOUT=30

# MinIO Production Settings
MINIO_ACCESS_KEY=production_access_key
MINIO_SECRET_KEY=production_secret_key_complex_and_secure
MINIO_ENDPOINT=minio-cluster.company.com:9000
MINIO_SECURE=true

# PostgreSQL Production
POSTGRES_HOST=postgres-cluster.company.com
POSTGRES_PORT=5432
POSTGRES_USER=airflow_prod
POSTGRES_PASSWORD=secure_production_password
POSTGRES_DB=airflow_production

# Airflow Production Settings
AIRFLOW_UID=1000
AIRFLOW_GID=0
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow_prod:secure_production_password@postgres-cluster.company.com:5432/airflow_production
AIRFLOW__CELERY__RESULT_BACKEND=redis://redis-cluster.company.com:6379/0
AIRFLOW__CELERY__BROKER_URL=redis://redis-cluster.company.com:6379/0
AIRFLOW__CORE__FERNET_KEY=production_fernet_key_32_chars_long
AIRFLOW__WEBSERVER__SECRET_KEY=production_webserver_secret_key

# Security & Monitoring
AIRFLOW__WEBSERVER__EXPOSE_CONFIG=false
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true

# Performance Tuning
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=3
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=10
AIRFLOW__CORE__PARALLELISM=16
AIRFLOW__CORE__DAG_CONCURRENCY=8

# ETL Performance
ETL_MAX_CONCURRENT_REQUESTS=5
ETL_RATE_LIMIT_DELAY=2.0
ETL_MAX_RETRY_ATTEMPTS=5
ETL_BACKOFF_MULTIPLIER=2
```

### docker-compose.production.yml
```yaml
version: '3.8'

services:
  etl_worker:
    build:
      context: .
      dockerfile: Dockerfile.etl
    environment:
      - RAPIDAPI_KEY=${RAPIDAPI_KEY}
      - MINIO_ENDPOINT=${MINIO_ENDPOINT}
      - MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY}
      - MINIO_SECRET_KEY=${MINIO_SECRET_KEY}
    deploy:
      replicas: 3
      resources:
        limits:
          cpus: '2'
          memory: 4G
        reservations:
          cpus: '1'
          memory: 2G
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "python", "etl/health_check.py"]
      interval: 30s
      timeout: 10s
      retries: 3

  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    environment:
      - MINIO_ROOT_USER=${MINIO_ACCESS_KEY}
      - MINIO_ROOT_PASSWORD=${MINIO_SECRET_KEY}
    volumes:
      - minio_data:/data
    deploy:
      resources:
        limits:
          cpus: '1'
          memory: 2G
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 30s
      timeout: 10s
      retries: 3

volumes:
  minio_data:
    driver: local
    driver_opts:
      type: nfs
      o: addr=nfs-server.company.com,vers=4
      device: ":/data/minio"
```

## 🧪 Development Environment

### .env.development
```bash
# RapidAPI Configuration
RAPIDAPI_KEY=dev_key_here
RAPIDAPI_HOST=sofascore.p.rapidapi.com
RAPIDAPI_REQUEST_TIMEOUT=10

# MinIO Development Settings
MINIO_ACCESS_KEY=minio
MINIO_SECRET_KEY=minio123
MINIO_ENDPOINT=localhost:9000
MINIO_SECURE=false

# PostgreSQL Development
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Airflow Development Settings
AIRFLOW_UID=1000
AIRFLOW_GID=0
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
AIRFLOW__CORE__FERNET_KEY=development_fernet_key_32_chars_
AIRFLOW__WEBSERVER__SECRET_KEY=development_secret_key

# Development Features
AIRFLOW__WEBSERVER__EXPOSE_CONFIG=true
AIRFLOW__CORE__LOAD_EXAMPLES=true
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false

# Performance (relaxed for development)
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=1
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=3
AIRFLOW__CORE__PARALLELISM=4

# ETL Development
ETL_MAX_CONCURRENT_REQUESTS=2
ETL_RATE_LIMIT_DELAY=1.0
ETL_MAX_RETRY_ATTEMPTS=2
ETL_BACKOFF_MULTIPLIER=1.5

# Debug Settings
PYTHONPATH=/opt/airflow
AIRFLOW__LOGGING__LOGGING_LEVEL=DEBUG
```

## 🧪 Testing Environment

### .env.testing
```bash
# RapidAPI Configuration (Mock or test endpoints)
RAPIDAPI_KEY=test_key_here
RAPIDAPI_HOST=sofascore.p.rapidapi.com
RAPIDAPI_REQUEST_TIMEOUT=5
USE_MOCK_API=true

# MinIO Testing (in-memory)
MINIO_ACCESS_KEY=test
MINIO_SECRET_KEY=testtest
MINIO_ENDPOINT=localhost:9000
MINIO_SECURE=false

# PostgreSQL Testing (test database)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=test_user
POSTGRES_PASSWORD=test_password
POSTGRES_DB=test_airflow

# Airflow Testing Settings
AIRFLOW_UID=1000
AIRFLOW_GID=0
AIRFLOW__CORE__EXECUTOR=SequentialExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://test_user:test_password@postgres:5432/test_airflow
AIRFLOW__CORE__UNIT_TEST_MODE=true

# Test Performance (fast execution)
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=1
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=1
AIRFLOW__CORE__PARALLELISM=1

# ETL Testing
ETL_MAX_CONCURRENT_REQUESTS=1
ETL_RATE_LIMIT_DELAY=0.1
ETL_MAX_RETRY_ATTEMPTS=1
ETL_BACKOFF_MULTIPLIER=1.0

# Testing Features
PYTEST_ARGS=--verbose --tb=short
MOCK_API_RESPONSES=true
FAST_TEST_MODE=true
```

## 🚀 Staging Environment

### .env.staging
```bash
# RapidAPI Configuration (production-like)
RAPIDAPI_KEY=staging_key_here
RAPIDAPI_HOST=sofascore.p.rapidapi.com
RAPIDAPI_REQUEST_TIMEOUT=20

# MinIO Staging
MINIO_ACCESS_KEY=staging_access_key
MINIO_SECRET_KEY=staging_secret_key_secure
MINIO_ENDPOINT=minio-staging.company.com:9000
MINIO_SECURE=true

# PostgreSQL Staging
POSTGRES_HOST=postgres-staging.company.com
POSTGRES_PORT=5432
POSTGRES_USER=airflow_staging
POSTGRES_PASSWORD=staging_password
POSTGRES_DB=airflow_staging

# Airflow Staging Settings
AIRFLOW_UID=1000
AIRFLOW_GID=0
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow_staging:staging_password@postgres-staging.company.com:5432/airflow_staging
AIRFLOW__CELERY__RESULT_BACKEND=redis://redis-staging.company.com:6379/0
AIRFLOW__CELERY__BROKER_URL=redis://redis-staging.company.com:6379/0

# Staging Performance (production-like but smaller)
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=2
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=5
AIRFLOW__CORE__PARALLELISM=8

# ETL Staging
ETL_MAX_CONCURRENT_REQUESTS=3
ETL_RATE_LIMIT_DELAY=1.5
ETL_MAX_RETRY_ATTEMPTS=3
ETL_BACKOFF_MULTIPLIER=1.8
```

## 🔧 Environment-Specific Scripts

### scripts/deploy-production.sh
```bash
#!/bin/bash
set -e

echo "🚀 Deploying to Production..."

# Validate production environment
if [[ -z "${RAPIDAPI_KEY}" ]]; then
    echo "❌ RAPIDAPI_KEY not set"
    exit 1
fi

# Build production images
docker-compose -f docker-compose.production.yml build

# Run production health checks
docker-compose -f docker-compose.production.yml run --rm etl_worker python etl/health_check.py

# Deploy with zero downtime
docker-compose -f docker-compose.production.yml up -d

# Verify deployment
sleep 30
docker-compose -f docker-compose.production.yml exec etl_worker python scripts/test_ekstraklasa.py

echo "✅ Production deployment completed"
```

### scripts/setup-development.sh
```bash
#!/bin/bash
set -e

echo "🧪 Setting up Development Environment..."

# Copy development configuration
cp .env.development .env

# Start development services
docker-compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services..."
sleep 30

# Initialize development data
docker exec etl_worker python -c "
from scripts.storage_manager import BronzeStorageManager
storage = BronzeStorageManager()
print('✅ Development buckets initialized')
"

# Run sample extraction
docker exec etl_worker python scripts/ekstraklasa_config.py

echo "✅ Development environment ready"
echo "🌐 Airflow UI: http://localhost:8080 (admin/admin)"
echo "📦 MinIO Console: http://localhost:9001 (minio/minio123)"
```

### scripts/run-tests.sh
```bash
#!/bin/bash
set -e

echo "🧪 Running Test Suite..."

# Use testing environment
cp .env.testing .env

# Start test services
docker-compose -f docker-compose.test.yml up -d

# Wait for services
sleep 15

# Run all tests
docker-compose -f docker-compose.test.yml exec etl_worker python -m pytest tests/ -v

# Run integration tests
docker-compose -f docker-compose.test.yml exec etl_worker python scripts/test_ekstraklasa.py

# Cleanup
docker-compose -f docker-compose.test.yml down

echo "✅ All tests passed"
```

## 📊 Monitoring Configuration

### Production Monitoring
```bash
# Prometheus metrics
ENABLE_PROMETHEUS=true
PROMETHEUS_PORT=9090

# Grafana dashboards
ENABLE_GRAFANA=true
GRAFANA_PORT=3000

# Alerting
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
EMAIL_ALERTS=data-team@company.com

# Performance monitoring
ENABLE_APM=true
APM_SERVER_URL=https://apm.company.com
```

### Log Aggregation
```bash
# ELK Stack integration
ELASTICSEARCH_HOST=elasticsearch.company.com:9200
LOGSTASH_HOST=logstash.company.com:5044
KIBANA_URL=https://kibana.company.com

# Log levels by environment
LOG_LEVEL_PRODUCTION=INFO
LOG_LEVEL_STAGING=DEBUG
LOG_LEVEL_DEVELOPMENT=DEBUG
LOG_LEVEL_TESTING=WARNING
```

---

**💡 Pro tip**: Use environment-specific docker-compose files and scripts to ensure consistent deployments across all environments!
