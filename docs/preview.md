# Data Pipeline – Ekstraklasa (Football Stats)

End-to-end data pipeline for ingesting, modeling and serving Ekstraklasa football data from the [SofaScore API](https://rapidapi.com/apidojo/api/sofascore) using a medallion architecture (Bronze → Silver → Gold). Orchestrated with Airflow, stored in MinIO/PostgreSQL, transformed via dbt, and ready for BI (Metabase).

## Contents

- [General Info](#general-info)
- [Tech Stack](#tech-stack)
- [Project Architecture & Code Structure](#project-architecture-and-code-structure)
- [Quick Start](#quick-start)
- [Data Flows](#data-flows)
- [Security Notes](#security-notes)
- [License](#license)

## General Info

Goal: build a reliable database of Ekstraklasa matches and statistics for analytics (BI) and predictive models.

Scope:
- Containerized environment (Docker & Docker Compose)
- ELT from SofaScore API → MinIO (Bronze) → PostgreSQL (Silver) → dbt (Gold)
- Airflow orchestration & scheduling

## Tech Stack

- Orchestration: Apache Airflow 3.0.6 ([docker/Dockerfile](docker/Dockerfile))
- Storage: MinIO
- Warehouse: PostgreSQL
- Transform: dbt-core ([dbt/dbt_project.yml](dbt/dbt_project.yml))
- BI: Metabase
- Runtime: Docker & Docker Compose ([docker/docker-compose.yml](docker/docker-compose.yml))
- Languages: Python, SQL

## Project Architecture and Code Structure

<p align="center">
  <img src="DE_pipeline_sofascore.drawio.png" alt="Architektura pipeline'u Ekstraklasa" width="900">
</p>

Bronze ETL Components:
- API client: `etl.bronze.client.SofascoreClient` (etl/bronze/client.py)
- Storage: `etl.bronze.storage.BronzeStorageManager` (etl/bronze/storage.py)
- Extractors:
  - `etl.bronze.extractors.statistics_extractor.StatisticsFetcher` (etl/bronze/extractors/statistics_extractor.py)
  - `etl.bronze.extractors.base_extractor` (etl/bronze/extractors/base_extractor.py)
  - `etl.bronze.extractors.incremental_extractor` (etl/bronze/extractors/incremental_extractor.py)

Diagnostics:
- `etl.bronze.diagnostics.season_diagnostic.EkstraklasaSeasonDiagnostic` (etl/bronze/diagnostics/season_diagnostic.py)
- `etl/bronze/diagnostics/content_explorer.py`

Other:
- Airflow DAGs: `airflow/dags/`
- Utility scripts (legacy/backup): `docker/backup/scripts/`
- dbt project: `dbt/`


## Quick Start

To use SofaScore API you need an [API key](https://rapidapi.com/apidojo/api/sofascore/pricing). Update your `.env` file with credentials.

1. Start services:
   ```sh
   cd docker
   docker-compose up -d
   ```
2. Access endpoints:
   - Airflow UI: http://localhost:8080 (user: `airflow`, pass: `airflow`)
   - MinIO Console: http://localhost:9001 (user: `minio`, pass: `minio123`)
   - PostgreSQL: `localhost:5432` (db: `dwh`, user: `airflow`, pass: `airflow`)
   - Metabase: http://localhost:3000

3. Initialize MinIO buckets & Airflow connections (if not already done):
   ```sh
   # inside airflow-webserver container (or via docker exec)
   bash airflow/scripts/create_minio_conn.sh
   bash airflow/scripts/create_minio_buckets.sh
   bash airflow/scripts/create_postgres_conn.sh
   ```
   Scripts: [airflow/scripts/create_minio_conn.sh](airflow/scripts/create_minio_conn.sh), [airflow/scripts/create_minio_buckets.sh](airflow/scripts/create_minio_buckets.sh), [airflow/scripts/create_postgres_conn.sh](airflow/scripts/create_postgres_conn.sh)

## Data Flows

### a) Historical Backfill (full load – requires paid API plan)

- Matches (API → Bronze → raw_matches in PostgreSQL):
  - [airflow/dags/bronze_backfill_historical.py](airflow/dags/bronze_backfill_historical.py)
  - [airflow/dags/bronze_load_historical_matches.py](airflow/dags/bronze_load_historical_matches.py)
- Stats (API → Bronze → raw_stats in PostgreSQL):
  - [airflow/dags/bronze_extract_historical_stats.py](airflow/dags/bronze_extract_historical_stats.py)
  - [airflow/dags/bronze_load_historical_stats.py](airflow/dags/bronze_load_historical_stats.py)
- Silver staging (full):
  - [airflow/dags/silver_stage_full.py](airflow/dags/silver_stage_full.py)

### b) Incremental Update (only new matches after last recorded date)

- Matches (API → Bronze → raw_matches in PostgreSQL):
  - [airflow/dags/01_bronze_extract_incremental_matches.py](airflow/dags/01_bronze_extract_incremental_matches.py)
  - [airflow/dags/02_bronze_load_incremental_matches.py](airflow/dags/02_bronze_load_incremental_matches.py)
- Stats (API → Bronze → raw_stats in PostgreSQL):
  - [airflow/dags/03_bronze_extract_incremental_stats.py](airflow/dags/03_bronze_extract_incremental_stats.py)
  - [airflow/dags/04_bronze_load_incremental_stats.py](airflow/dags/04_bronze_load_incremental_stats.py)
- Silver staging (incremental):
  - [airflow/dags/05_silver_stage_incremental.py](airflow/dags/05_silver_stage_incremental.py)

### c) dbt (Silver → Gold)

Run inside the `dbt` container:
```sh
docker exec -it dbt dbt deps
docker exec -it dbt dbt run
docker exec -it dbt dbt test
```
## Analytical layer

Metabase is included for BI exploration (service defined in [docker/docker-compose.yml](docker/docker-compose.yml)).

Initial setup:
- On first launch create admin account.
- Add PostgreSQL database:
  - Host: postgres
  - Port: 5432
  - DB name: dwh
  - User: airflow
  - Password: airflow
- After running dbt ([dbt/dbt_project.yml](dbt/dbt_project.yml)) click Sync to load new tables (silver/gold schemas).


## Security Notes

- Change default passwords before production.
- Use environment variables / secret managers for credentials.
- Enable HTTPS and schedule regular backups.

## License

MIT
