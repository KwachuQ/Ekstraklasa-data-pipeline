# Data Pipeline – Sofascore (Football Stats)

End-to-end data pipeline for ingesting, modeling and serving Ekstraklasa football data from SofaScore API using a medallion architecture (bronze → silver → gold). Orchestrated with Airflow, stored in MinIO/PostgreSQL, transformed via dbt, and ready for BI (Metabase).

- Architecture: see [architecture.md](architecture.md)
- Environment configs: [docs/ENVIRONMENT_CONFIG.md](docs/ENVIRONMENT_CONFIG.md)
- Data flow: [docs/data_flow.txt](docs/data_flow.txt)

## Spis treści
- [Ogólne informacje](#ogólne-informacje)
- [Technologie](#technologie)
- [Instrukcja uruchomienia](#instrukcja-uruchomienia)
- [Struktura kodu](#struktura-kodu)
- [Przykłady użycia](#przykłady-użycia)
- [Status projektu](#status-projektu)
- [Licencja i autorzy](#licencja-i-autorzy)
- [Linki do dalszej dokumentacji](#linki-do-dalszej-dokumentacji)

## Ogólne informacje
- Cel: zbudowanie wiarygodnej bazy danych meczów i statystyk Ekstraklasy do analiz i predykcji (fair odds).
- Zakres:
  - ETL/ELT z SofaScore API do MinIO (bronze), PostgreSQL (silver), dbt (gold).
  - Orkiestracja Airflow, kontenery Docker.
  - Diagnostyka kompletności danych i testy jakości.

Wymagania biznesowo-analityczne: [docs/specification.txt](docs/specification.txt), przegląd danych: [docs/data_analysis_review.md](docs/data_analysis_review.md).

## Technologie
- Orchestracja: Apache Airflow 3.0.6 
- Składowanie: MinIO
- Hurtownia: PostgreSQL
- Transformacje: dbt-core
- BI: Metabase
- Języki i biblioteki: Python 3.12, Pydantic, psycopg2, minio, requests, pytest
- Runtime: Docker & Docker Compose

Dokładne profile środowisk: [docs/ENVIRONMENT_CONFIG.md](docs/ENVIRONMENT_CONFIG.md)

## Instrukcja uruchomienia
1) Uruchom usługi
```sh
cd docker
docker-compose up -d
```

2) Inicjalizacja połączeń/bucketów (jeśli wymagane)
- Skrypty i DAGi pomocnicze: zob. [docs/README.md](docs/README.md) (sekcja Quick Start) i [airflow/dags/backup/silver_stage_full.py](airflow/dags/backup/silver_stage_full.py)

3) Uruchom przepływ inkrementalny (manualnie z UI lub via docker exec)
- Mecze → Bronze → raw_matches:
  - [airflow/dags/02_bronze_load_incremental_matches.py](airflow/dags/02_bronze_load_incremental_matches.py)
- Statystyki → Bronze → raw_stats:
  - [airflow/dags/backup/03_bronze_extract_incremental_stats.py](airflow/dags/backup/03_bronze_extract_incremental_stats.py)
  - [airflow/dags/backup/04_bronze_load_incremental_stats.py](airflow/dags/backup/04_bronze_load_incremental_stats.py)
- Silver staging (inkrementalnie lub pełny):
  - [airflow/dags/backup/05_silver_stage_incremental.py](airflow/dags/backup/05_silver_stage_incremental.py)
  - [airflow/dags/backup/silver_stage_full.py](airflow/dags/backup/silver_stage_full.py)

4) Uruchom dbt (silver → gold)
```sh
# wewnątrz kontenera dbt
docker exec -it dbt dbt deps
docker exec -it dbt dbt run
docker exec -it dbt dbt test
```

5) Testy i health-check
```sh
docker exec -it etl_worker python tests/smoke_test.py
docker exec -it etl_worker python tests/test_storage.py
docker exec -it etl_worker python tests/health_check.py
```

Backfill historyczny (smart + checkpoint): [airflow/dags/bronze_backfill_historical.py](airflow/dags/bronze_backfill_historical.py)

## Struktura kodu
- ETL (Bronze):
  - Klient API: [`etl.bronze.client.SofascoreClient`](etl/bronze/client.py)
  - Składowanie: [`etl.bronze.storage.BronzeStorageManager`](etl/bronze/storage.py)
  - Ekstraktory:
    - [`etl.bronze.extractors.statistics_extractor.StatisticsFetcher`](etl/bronze/extractors/statistics_extractor.py)
    - [`etl.bronze.extractors.base_extractor`](etl/bronze/extractors/base_extractor.py)
    - [`etl.bronze.extractors.incremental_extractor`](etl/bronze/extractors/incremental_extractor.py)
- Diagnostyka:
  - [`etl.bronze.diagnostics.season_diagnostic.EkstraklasaSeasonDiagnostic`](etl/bronze/diagnostics/season_diagnostic.py)
  - [etl/bronze/diagnostics/content_explorer.py](etl/bronze/diagnostics/content_explorer.py)
- DAGi Airflow: [airflow/dags/](airflow/dags/)
- Skrypty pomocnicze (legacy/backup): [docker/backup/scripts/](docker/backup/scripts/)
- dbt: [dbt/](dbt/)

Pełny opis komponentów: [architecture.md](architecture.md)

## Przykłady użycia
- Pobranie statystyk dla listy match_id do MinIO (Bronze):
```sh
docker exec -e RAPIDAPI_KEY=$RAPIDAPI_KEY etl_worker python docker/backup/scripts/fetch_match_statistics.py
```
- Testowy run ekstraktora statystyk:
```sh
docker exec -e RAPIDAPI_KEY=$RAPIDAPI_KEY etl_worker python -c "from etl.bronze.extractors.statistics_extractor import main; main()"
```
- Diagnostyka sezonów:
```sh
docker exec etl_worker python etl/bronze/diagnostics/season_diagnostic.py
```

Dodatkowe przepływy operacyjne: [docs/data_flow.txt](docs/data_flow.txt)

## Status projektu
- Stabilność: aktywny rozwój (pipeline działa w trybie inkrementalnym; backfill wspierany przez checkpoint).
- Znane kwestie:
  - Nierówne pokrycie historycznych statystyk, szczególnie przed 2013 r. (raporty: [docs/data_analysis_review.md](docs/data_analysis_review.md))
  - Konsolidacja nazw i modułów w trakcie ([to_do.txt](to_do.txt))
- Roadmap: patrz [architecture.md](architecture.md) → Roadmap/Technical Debt

## Licencja i autorzy
- Licencja: MIT (zob. [docs/README.md](docs/README.md))
- Autorzy: Data Engineering Team

## Linki do dalszej dokumentacji
- Architektura: [architecture.md](architecture.md)
- Konfiguracje środowisk: [docs/ENVIRONMENT_CONFIG.md](docs/ENVIRONMENT_CONFIG.md)
- Specyfikacja biznesowa: [docs/specification.txt](docs/specification.txt)
- Endpoints API (referencje): [docs/reference_sources/sofascore_API_endpoints_docs.txt](docs/reference_sources/sofascore_API_endpoints_docs.txt)