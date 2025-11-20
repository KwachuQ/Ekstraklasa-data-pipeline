# Data Pipeline – Ekstraklasa (Football Stats)

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

To use Sofascore API you will need [API key](https://rapidapi.com/apidojo/api/sofascore/pricing). Update .env file with your auth data.

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
  4. Run extract and load flow (Airflow UI or use docker exec):

    **a) Historical backfill (requires Sofascore API paid subscription for one time, full load):**
    
    - **Matches (API) → Bronze (MinIO) → raw_matches (PostgreSQL):**
      - [airflow/dags/bronze_backfill_historical.py](airflow/dags/bronze_backfill_historical.py)
      - [airflow/dags/broze_load_historical_matches.py](airflow/dags/broze_load_historical_matches.py)
    - **Stats (API) → Bronze (MinIO) → raw_stats (PostgreSQL):**
      - [airflow/dags/bronze_extract_historical_stats.py](airflow/dags/bronze_extract_historical_stats.py)
      - [airflow/dags/bronze_load_historical_stats.py](airflow/dags/bronze_load_historical_stats.py)
    - **Silver staging (full):**
      - [airflow/dags/silver_stage_full.py](airflow/dags/silver_stage_full.py)

    **b) Incremental update (only matches after last date recorded in database):**
    
    - **Matches (API) → Bronze (MinIO) → raw_matches (PostgreSQL):**
      - [airflow/dags/01_bronze_extract_incremental_matches.py](airflow/dags/01_bronze_extract_incremental_matches.py)
      - [airflow/dags/02_bronze_load_incremental_matches.py](airflow/dags/02_bronze_load_incremental_matches.py)
    - **Stats (API) → Bronze (MinIO) → raw_stats (PostgreSQL):**
      - [airflow/dags/03_bronze_extract_incremental_stats.py](airflow/dags/03_bronze_extract_incremental_stats.py)
      - [airflow/dags/04_bronze_load_incremental_stats.py](airflow/dags/04_bronze_load_incremental_stats.py)
    - **Silver staging (incremental):**
      - [airflow/dags/05_silver_stage_incremental.py](airflow/dags/05_silver_stage_incremental.py)

5. Run dbt (silver → gold)

 ```sh
# inside dbt container

docker exec -it dbt dbt deps
docker exec -it dbt dbt run
docker exec -it dbt dbt test`

## Code structure

ETL (Bronze):
  - API client: etl.bronze.client.SofascoreClient (etl/bronze/client.py)
  - Storage: etl.bronze.storage.BronzeStorageManager (etl/bronze/storage.py)
  - Extractors:
    - etl.bronze.extractors.statistics_extractor.StatisticsFetcher (etl/bronze/extractors/statistics_extractor.py)
    - etl.bronze.extractors.base_extractor (etl/bronze/extractors/base_extractor.py)
    - etl.bronze.extractors.incremental_extractor (etl/bronze/extractors/incremental_extractor.py)

Diagnostics:
  - etl.bronze.diagnostics.season_diagnostic.EkstraklasaSeasonDiagnostic (etl/bronze/diagnostics/season_diagnostic.py)
  - etl/bronze/diagnostics/content_explorer.py

Airflow DAGs: airflow/dags/

Utility scripts (legacy/backup): docker/backup/scripts/

dbt: dbt/

## Security Notes
- Change default passwords before production
- Use env vars/secrets for credentials
- Enable HTTPS and regular backups

## License
MIT
