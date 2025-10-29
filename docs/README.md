# Data Pipeline – Sofascore (Football Stats)

End-to-end data pipeline using Apache Airflow, MinIO (S3-compatible), PostgreSQL, dbt, and Metabase. Implements a medallion architecture (bronze → silver → gold) to ingest, transform, and model football stats from the SofaScore API.

## Stack
- Orchestration: Apache Airflow 3.0.6 ([docker/Dockerfile](docker/Dockerfile))
- Storage: MinIO (buckets: bronze, silver, gold)
- Warehouse: PostgreSQL (schemas: bronze, silver, gold)
- Transform: dbt-core ([dbt/dbt_project.yml](dbt/dbt_project.yml))
- BI: Metabase
- Runtime: Docker & Docker Compose ([docker/docker-compose.yml](docker/docker-compose.yml))

## Quick Start
1. Start services:
   ```sh
   cd docker
   docker-compose up -d
   ```
2. Access:
   - Airflow UI: http://localhost:8080 (airflow/airflow)
   - MinIO Console: http://localhost:9001 (minio/minio123)
   - PostgreSQL: localhost:5432 (db: dwh, user: airflow, pass: airflow)
3. Initialize MinIO connection and buckets (if not already done):
   ```sh
   # From inside airflow-webserver container (or use docker exec)
   bash airflow/scripts/create_minio_conn.sh
   bash airflow/scripts/create_minio_buckets.sh
   ```
   - Scripts: [airflow/scripts/create_minio_conn.sh](airflow/scripts/create_minio_conn.sh), [airflow/scripts/create_minio_buckets.sh](airflow/scripts/create_minio_buckets.sh)
   - Optional DAG to manage buckets: [airflow/dags/init_minio_buckets.py](airflow/dags/init_minio_buckets.py)
4. Run dbt:
   ```sh
   # Inside dbt container (compose service name may be "dbt")
   docker exec -it dbt dbt deps
   docker exec -it dbt dbt seed
   docker exec -it dbt dbt run
   docker exec -it dbt dbt test
   ```

## Data Model (dbt)
- Silver models:
  - Matches: [dbt/models/silver/fact_match.sql](dbt/models/silver/fact_match.sql)
  - Team x Match metrics: [dbt/models/silver/fact_team_match.sql](dbt/models/silver/fact_team_match.sql)
  - Team per Season rollups: [dbt/models/silver/fact_season_team.sql](dbt/models/silver/fact_season_team.sql)
- Macros:
  - Period normalization: [dbt/macros/normalize_period.sql](dbt/macros/normalize_period.sql)
  - Metric key cleaning: [dbt/macros/clean_metric_key.sql](dbt/macros/clean_metric_key.sql)
  - Dynamic metric columns: [dbt/macros/get_metric_keys.sql](dbt/macros/get_metric_keys.sql)
- Sources and schemas: [dbt/models/sources.yml](dbt/models/sources.yml)

## Typical Flow
1. Bronze: Raw data lands in MinIO bucket `bronze`.
2. Silver: Cleaning and conformance to warehouse schemas in PostgreSQL `silver`.
3. Gold: Aggregations and analytics-ready tables (optional).

## Development
- Airflow images and deps: [docker/requirements.txt](docker/requirements.txt)
- Airflow services and volumes: [docker/docker-compose.yml](docker/docker-compose.yml)
- Postgres schemas: bronze/silver/gold created on init
- Add new DAGs under Airflow’s DAGs mount; use `minio_s3` connection in hooks/operators.

## Troubleshooting
- Containers:
  ```sh
  docker-compose ps
  docker logs <container>
  ```
- Airflow connection check:
  ```sh
  docker exec airflow_webserver airflow connections get minio_s3
  ```
- Reset volumes (destructive):
  ```sh
  docker-compose down -v && docker volume prune
  ```

## Security Notes
- Change default passwords before production
- Use env vars/secrets for credentials
- Enable HTTPS and regular backups

## License
MIT