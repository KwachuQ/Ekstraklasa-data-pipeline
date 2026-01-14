# Data Pipeline – football stats data

![Python](https://img.shields.io/badge/python-3.11+-blue.svg)
![Airflow](https://img.shields.io/badge/Airflow-3.0.6-017CEE.svg)
![dbt](https://img.shields.io/badge/dbt-1.10+-FF694B.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17.6-336791.svg)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

End-to-end data pipeline for ingesting, modeling and serving Ekstraklasa football data from the [SofaScore API](https://rapidapi.com/apidojo/api/sofascore) using a medallion architecture (Bronze → Silver → Gold). Orchestrated with Airflow, stored in MinIO/PostgreSQL, transformed via dbt, and ready for BI (Metabase).

## Project Status

🟢 **Active Development** - Pipeline is fully functional for Ekstraklasa and ready for other European leagues.

| Feature | Status |
|---------|--------|
| Historical data extraction | ✅ Complete |
| Incremental updates | ✅ Complete |
| dbt transformations | ✅ Complete |
| Metabase integration | ✅ Complete |
| Next matches prediction | ✅ Complete |
| Multi-league support | ✅ 20+ leagues configured |

## Contents

- [General Info](#general-info)
- [Tech Stack](#tech-stack)
- [Project Architecture & Code Structure](#project-architecture-and-code-structure)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Data Flows](#data-flows)
- [Results & Demo](#results--demo)
- [Challenges & Learnings](#challenges--learnings)
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
- Warehouse: PostgreSQL 17.6
- Transform: dbt-core 1.10 ([dbt/dbt_project.yml](dbt/dbt_project.yml))
- BI: Metabase
- Runtime: Docker & Docker Compose ([docker/docker-compose.yml](docker/docker-compose.yml))
- Languages: Python, SQL

## Project Architecture and Code Structure

<p align="center">
  <img src="static/DE_pipeline_sofascore.drawio.png" alt="Architektura pipeline'u Ekstraklasa" width="900">
</p>

Bronze ETL Components:
- API client: `etl.bronze.client.SofascoreClient` (etl/bronze/client.py)
- Storage: `etl.bronze.storage.BronzeStorageManager` (etl/bronze/storage.py)
- Extractors:
  - `etl.bronze.extractors.statistics_extractor.StatisticsFetcher` (etl/bronze/extractors/statistics_extractor.py)
  - `etl.bronze.extractors.base_extractor` (etl/bronze/extractors/base_extractor.py)
  - `etl.bronze.extractors.incremental_extractor` (etl/bronze/extractors/incremental_extractor.py)
  - `etl.bronze.extractors.next_matches_extractor` (etl/bronze/extractors/next_matches_extractor.py)

Diagnostics:
- `etl.bronze.diagnostics.season_diagnostic.EkstraklasaSeasonDiagnostic` (etl/bronze/diagnostics/season_diagnostic.py)
- `etl/bronze/diagnostics/content_explorer.py`

Other:
- Airflow DAGs: `airflow/dags/`
- Utility scripts: `etl/utils/` (config_loader, find_season_id, extract_top_leagues)
- dbt project: `dbt/`


## Quick Start

To use SofaScore API you need an [API key](https://rapidapi.com/apidojo/api/sofascore/pricing).

1. Initialize secrets and start services:
```sh 
cd docker
chmod +x init_secrets.sh
./init_secrets.sh

# Edit the secret files with your credentials:
# - secrets/rapidapi_key.txt  (your RapidAPI key)
# - secrets/aws_rds_password.txt  (optional: AWS RDS password)

docker compose up -d
```

2. Access endpoints:

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | `airflow` / `airflow` |
| MinIO Console | http://localhost:9001 | `minio` / `minio123` |
| PostgreSQL | `localhost:5432` | db: `dwh`, user: `airflow`, pass: `airflow` |
| Metabase | http://localhost:3000 | Create on first launch |

3. Configure [`league_config.yaml`](../config/league_config.yaml) file (see: [Configuration](#configuration)) to define from which league/seasons/dates download match data from API.

4. Run master DAG: [`00_historical_pipeline_orchestrator`](../airflow/dags/00_historical_pipeline_orchestrator.py) (from Airflow UI)

5. Run Metabase to start exploring or [`export_gold_tables.sh`](../airflow/scripts/export_gold_tables.sh) to get .csv files in "data" folder.

## Configuration

### League Configuration

The pipeline supports multiple European football leagues through a centralized configuration file: [config/league_config.yaml](../config/league_config.yaml)

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
    season_id: 61643 
```
To find the correct `season_id` for any league, use the helper script:

```bash
# From project root
export RAPIDAPI_KEY={YOUR_API_KEY}
python -m etl.utils.find_season_id

# Or for a specific league
export RAPIDAPI_KEY={YOUR_API_KEY}
python -m etl.utils.find_season_id <league_id> <league_name>
```

See config/league_config.yaml for complete documentation and examples.

## Data Flows

### a) Historical Backfill (full load – requires paid API plan over 500 requests per month)

Run master DAG: [`00_historical_pipeline_orchestrator`](../airflow/dags/00_historical_pipeline_orchestrator.py)

This orchestrator executes the complete historical data pipeline in the following sequence:

**Step 1: Extract Historical Matches**
- [`10_bronze_extract_historical_matches`](../airflow/dags/10_extract_historical_matches.py) - Extract matches from API to MinIO

**Step 2: Load Matches to Bronze**
- [`11_bronze_load_historical_matches`](../airflow/dags/11_bronze_load_historical_matches.py) - Load matches from MinIO to PostgreSQL bronze layer
- Includes automatic dbt refresh of `bronze.full_matches_data`

**Step 3: Extract Historical Statistics**
- [`12_bronze_extract_historical_stats`](../airflow/dags/12_bronze_extract_historical_stats.py) - Extract statistics for historical matches from API to MinIO

**Step 4: Load Statistics to Bronze**
- [`13_bronze_load_historical_stats`](../airflow/dags/13_bronze_load_historical_stats.py) - Load statistics from MinIO to PostgreSQL bronze layer
- Includes automatic dbt refresh of `bronze.full_stats_data`

**Step 5: Create Silver Staging Tables**
- [`14_silver_stage_full`](../airflow/dags/14_silver_stage_full.py) - Create and populate `silver.staging_matches` and `silver.staging_stats`

**Step 6: Transform Silver Layer**
- [`06_silver_transform_dbt`](../airflow/dags/06_silver_transform_dbt.py) - Run dbt models for silver layer transformations

**Step 7: Transform Gold Layer**
- [`07_gold_transform_dbt`](../airflow/dags/07_gold_transform_dbt.py) - Run dbt models for gold layer analytics marts

### b) Incremental Update (only new matches after last recorded date)

Run master DAG: [`00_incremental_pipeline_orchestrator`](../airflow/dags/00_incremental_pipeline_orchestrator.py)

This orchestrator executes the incremental pipeline in the following sequence:

**Step 1: Extract New Matches**
- [`01_bronze_extract_incremental_matches`](../airflow/dags/01_bronze_extract_incremental_matches.py) - Extract new matches from API to MinIO

**Step 2: Load Matches to Bronze**
- [`02_bronze_load_incremental_matches`](../airflow/dags/02_bronze_load_incremental_matches.py) - Load new matches from MinIO to PostgreSQL
- Includes automatic dbt incremental refresh

**Step 3: Extract Statistics**
- [`03_bronze_extract_incremental_stats`](../airflow/dags/03_bronze_extract_incremental_stats.py) - Extract statistics for new matches from API to MinIO

**Step 4: Load Statistics to Bronze**
- [`04_bronze_load_incremental_stats`](../airflow/dags/04_bronze_load_incremental_stats.py) - Load new statistics from MinIO to PostgreSQL
- Includes automatic dbt incremental refresh

**Step 5: Update Silver Staging**
- [`05_silver_stage_incremental`](../airflow/dags/05_silver_stage_incremental.py) - Incrementally update staging tables

**Step 6: Transform Silver Layer**
- [`06_silver_transform_dbt`](../airflow/dags/06_silver_transform_dbt.py) - Run dbt models for silver layer

**Step 7: Transform Gold Layer**
- [`07_gold_transform_dbt`](../airflow/dags/07_gold_transform_dbt.py) - Run dbt models for gold layer

### c) Next Matches Prediction (upcoming fixtures)

Run DAGs for prediction features:

**Step 1: Extract Next Matches**
- [`08_bronze_extract_next_matches`](../airflow/dags/08_bronze_extract_next_matches.py) - Extract upcoming fixtures from API

**Step 2: Load Next Matches**
- [`09_load_next_matches`](../airflow/dags/09_load_next_matches.py) - Load to PostgreSQL for predictions

## Analytical layer

Metabase is included for BI exploration (service defined in [docker/docker-compose.yml](../docker/docker-compose.yml)).

Initial setup:
- On first launch create admin account.
- Add PostgreSQL database
- After running dbt ([dbt/dbt_project.yml](../dbt/dbt_project.yml)) click Sync to load new tables (gold schemas).

In order to export gold layer tables to .csv file (to use with other data viz tools) run:
   - [airflow/scripts/export_gold_tables.sh](../airflow/scripts/export_gold_tables.sh)

## Results & Demo

### Pipeline Output

The pipeline produces **13 analytical mart tables** in the gold layer, ready for BI exploration:

| Mart | Description |
|------|-------------|
| `mart_team_season_summary` | Complete season statistics per team |
| `mart_team_form` | Recent form analysis (last 5 matches) |
| `mart_team_attack` | Attacking metrics (xG, shots, goals) |
| `mart_team_defense` | Defensive metrics (xGA, tackles, clean sheets) |
| `mart_team_possession` | Ball possession statistics |
| `mart_team_discipline` | Cards, fouls analysis |
| `mart_team_btts_analysis` | Both Teams To Score patterns |
| `mart_head_to_head` | Historical matchups between teams |
| `mart_match_predictions` | Pre-match prediction features |
| `mart_upcoming_fixtures` | Scheduled matches |
| `mart_upcoming_predictions` | Predictions for upcoming games |
| `mart_league_averages` | League-wide statistical benchmarks |
| `mart_team_overview` | Quick team summary view |

### Sample Data Volume

From Ekstraklasa 24/25 season extraction:
- **~300 matches** with full statistics
- **50+ statistical metrics** per match
- **11 exported CSV files** in `data/` folder

### Screenshots

**Airflow DAGs:**

<p align="center">
  <img src="static/airflow.png" alt="Airflow DAGs" width="800">
</p>

**Metabase Dashboards:**

<p align="center">
  <img src="static/metabase_viz_1.png" alt="Metabase Dashboard 1" width="800">
</p>

<p align="center">
  <img src="static/metabase_viz_2.png" alt="Metabase Dashboard 2" width="800">
</p>

<p align="center">
  <img src="static/metabase_viz_3.png" alt="Metabase Dashboard 3" width="800">
</p>

## Challenges & Learnings

### 🔧 Technical Challenges

1. **API Rate Limiting**
   - SofaScore API has strict rate limits (RapidAPI tier-based)
   - Solution: Implemented exponential backoff with `tenacity` library in `StatisticsFetcher`
   - Added configurable `rate_limit_delay` parameter (default: 1.5s between requests)

2. **Incremental vs Full Load Logic**
   - Challenge: Supporting both historical backfill and daily updates
   - Solution: Separate DAG orchestrators (`00_historical_*` vs `00_incremental_*`)
   - Used `last_extraction_date` config parameter for incremental mode

3. **Data Validation**
   - Raw API responses vary in structure
   - Solution: Pydantic schemas (`MatchBasic`, `TournamentBasic`, `SeasonBasic`) for validation
   - `APIResponse` wrapper class for standardized error handling

4. **dbt Schema Management**
   - Challenge: Managing bronze/silver/gold schemas in PostgreSQL
   - Solution: Custom schema macro in `dbt/macros/` to override default naming

### 💡 Key Learnings

- **Medallion architecture** provides clear data lineage and easier debugging
- **Configuration-driven pipelines** (YAML) enable easy league switching without code changes
- **Docker Compose** with health checks ensures proper service startup order
- **dbt incremental models** significantly reduce processing time for daily updates
- **MinIO as S3-compatible storage** simplifies local development while maintaining cloud portability

## Security Notes

This project uses **Docker Secrets** for secure credential management:

- All secrets are stored in `docker/secrets/` (gitignored)
- Run `init_secrets.sh` to generate default secrets
- Edit secret files before starting containers:
  - `secrets/rapidapi_key.txt` - Your RapidAPI key
  - `secrets/aws_rds_password.txt` - AWS RDS password  
- For production: change default passwords in `postgres_password.txt` and `minio_root_password.txt`
- Enable HTTPS/TLS for all services in production

## License

MIT
