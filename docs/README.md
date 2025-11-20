# Data Pipeline – Sofascore (Football Stats)

End-to-end data pipeline for ingesting, modeling and serving Ekstraklasa football data from [SofaScore API](https://rapidapi.com/apidojo/api/sofascore) using a medallion architecture (bronze → silver → gold). Orchestrated with Airflow, stored in MinIO/PostgreSQL, transformed via dbt, and ready for BI (Metabase).

## Contents

- [General Info](#general-info)
- [Tech stack](#tech-stack)
- [Quick start](#quick-start)
- [Code structure](#code-structure)
- [Links](#links)
- [License](#license)

## General Info

My goal was to build a reliable database of matches and stats from Ekstraklasa soccer competition. It can be used for further analysis in BI tools and for prediction models.

Scope:
  - Docker environment for containerization
  - ELT z Sofascore API do MinIO (bronze), PostgreSQL (silver), dbt (gold)
  - Airflow orchestration

## Tech Stack

- Orchestration: Apache Airflow 3.0.6 ([docker/Dockerfile](docker/Dockerfile))
- Storage: MinIO 
- Warehouse: PostgreSQL 
- Transform: dbt-core ([dbt/dbt_project.yml](dbt/dbt_project.yml))
- BI: Metabase
- Runtime: Docker & Docker Compose ([docker/docker-compose.yml](docker/docker-compose.yml))
- Languages: Python, SQL

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

3. Initialize MinIO and Airflow connections (if not already done):
   ```sh
   # From inside airflow-webserver container (or use docker exec)
   bash airflow/scripts/create_minio_conn.sh
   bash airflow/scripts/create_minio_buckets.sh
   bash airflow/scripts/create_postgres_conn.sh
   ```
   - Scripts: [airflow/scripts/create_minio_conn.sh](airflow/scripts/create_minio_conn.sh), [airflow/scripts/create_minio_buckets.sh](airflow/scripts/create_minio_buckets.sh), [airflow/scripts/create_postgres_conn.sh](airflow/scripts/create_postgres_conn.sh)
   

4. Run extracting and loading flow:

  a) historical backfill (requires )
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