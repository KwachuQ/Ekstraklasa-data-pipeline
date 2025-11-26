# Data Pipeline – Ekstraklasa (Football Stats)

End-to-end data pipeline for ingesting, modeling and serving Ekstraklasa football data from the [SofaScore API](https://rapidapi.com/apidojo/api/sofascore) using a medallion architecture (Bronze → Silver → Gold). Orchestrated with Airflow, stored in MinIO/PostgreSQL, transformed via dbt, and ready for BI (Metabase).

## Contents

- [General Info](#general-info)
- [Tech Stack](#tech-stack)
- [Project Architecture & Code Structure](#project-architecture-and-code-structure)
- [Configuration](#configuration)
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

## Configuration

### League Configuration

The pipeline supports multiple European football leagues through a centralized configuration file: [config/league_config.yaml](config/league_config.yaml)

**Supported Leagues:**
- 🇵🇱 Poland: Ekstraklasa, Betclic 1. Liga
- 🇪🇸 Spain: LaLiga, LaLiga 2
- 🇩🇪 Germany: Bundesliga, 2. Bundesliga
- 🇬🇧 England: Premier League, Championship
- 🇮🇹 Italy: Serie A, Serie B
- 🇫🇷 France: Ligue 1, Ligue 2
- 🇵🇹 Portugal: Liga Portugal Betclic, Liga Portugal 2
- 🇳🇱 Netherlands: Eredivisie, Eerste Divisie
- Plus leagues from Norway, Sweden, Denmark, Austria, Czech Republic

**Configuration Options:**


Select active league
```yaml
active_league:
  country: "Poland"
  country_id: 47
  league_name: "Ekstraklasa"
  league_id: 202
```
Select active season
```yaml
active_season:
  name: "Ekstraklasa 25/26"
  year: "25/26"
  season_id: 76477
```
ETL parameters
```yaml
etl:
  max_pages: 20                    # API pagination limit
  batch_size: 50                   # Statistics batch size
```  
Date filtering (YYYY-MM-DD format, null = no limit)
```yaml
  start_date: "2024-08-01"         # Include matches from this date
  end_date: "2024-12-31"           # Include matches up to this date
```
Incremental mode
```yaml
  last_extraction_date: null       # For daily updates (overrides start/end dates)
  ```

### Configuration Examples:
 
Full Historical Load (all available data):
```yaml
  start_date: null
  end_date: null
```

Incremental Updates (only new matches):
```yaml
  last_extraction_date: "2025-01-15"  # Fetch matches after this date
```

Switch League (e.g., to LaLiga):
```yaml
  active_league:
    country: "Spain"
    league_name: "LaLiga"
    league_id: 8
  active_season:
    name: "LaLiga 24/25"
    season_id: 61643  # Use find_season_id.py
```
See config/league_config.yaml for complete documentation and examples.

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

Run DAG: [00_incremental_pipeline_orchestrator](airflow/dags/00_incremental_pipeline_orchestrator.py)
It will execute following process:

- Matches (API → minIO → PostgreSQL):
  - [airflow/dags/01_bronze_extract_incremental_matches.py](airflow/dags/01_bronze_extract_incremental_matches.py)
  - [airflow/dags/02_bronze_load_incremental_matches.py](airflow/dags/02_bronze_load_incremental_matches.py)
- Stats (API → minIO → PostgreSQL):
  - [airflow/dags/03_bronze_extract_incremental_stats.py](airflow/dags/03_bronze_extract_incremental_stats.py)
  - [airflow/dags/04_bronze_load_incremental_stats.py](airflow/dags/04_bronze_load_incremental_stats.py)
- Silver staging (PostgreSQL):
  - [airflow/dags/05_silver_stage_incremental.py](airflow/dags/05_silver_stage_incremental.py)
- Silver transform (PostgreSQL):
  - [airflow/dags/06_silver_transform_dbt.py](airflow/dags/06_silver_transform_dbt.py)
- Gold transform (PostgreSQL):
  - [airflow/dags/07_gold_transform_dbt.py](irflow/dags/07_gold_transform_dbt.py)

## Analytical layer

Metabase is included for BI exploration (service defined in [docker/docker-compose.yml](docker/docker-compose.yml)).

Initial setup:
- On first launch create admin account.
- Add PostgreSQL database
- After running dbt ([dbt/dbt_project.yml](dbt/dbt_project.yml)) click Sync to load new tables (gold schemas).

In order to export gold layer tables to .csv file (to use with other data viz tools) run:
   - [airflow/scripts/export_gold_tables.sh](airflow/scripts/export_gold_tables.sh)
   
## Security Notes

1. **Change ALL default passwords:**
   - PostgreSQL: `airflow:airflow`
   - MinIO: `minio:minio123`
   - Airflow: `airflow:airflow`

2. **Secure API credentials:**
   - Store `RAPIDAPI_KEY` in environment variables or secret manager
   - Never commit API keys to version control

3. **Use .env file for local development:**
   ```bash
   cp docker/.env.example docker/.env
   # Edit docker/.env with your credentials
   ```

4. **For production:**
   - Use Docker secrets
   - Enable HTTPS/TLS for all services
   - Configure proper firewall rules
   - Use strong, unique passwords

## License

MIT
