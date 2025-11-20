# Architektura – Data Pipeline Sofascore

Dokument opisuje architekturę, komponenty i standardy wdrożenia systemu ETL/ELT dla danych Ekstraklasy z SofaScore.

- Diagramy (źródła): [docs/data_integration.drawio](docs/data_integration.drawio), [docs/DE_pipeline_sofascore.drawio](docs/DE_pipeline_sofascore.drawio)
- Przepływ: [docs/data_flow.txt](docs/data_flow.txt)

## Cel i zakres
- Cel: niezawodny pipeline od pozyskania do warstwy analitycznej (BI/ML).
- Zakres:
  - Ekstrakcja, walidacja i składowanie surowych danych (Bronze).
  - Normalizacja i staging (Silver).
  - Modele analityczne i agregaty (Gold).
  - Orkiestracja, testy, diagnostyka, monitoring.

## Przegląd architektury (Medallion)
- Bronze (MinIO):
  - Strumień NDJSON/JSON pogrupowany po sezonie/dacie/typie.
  - Składowanie wsadowe, manifesty, idempotencja.
- Silver (PostgreSQL):
  - Staging tabel meczów i statystyk, kontrola typów i integralności.
- Gold (dbt):
  - Modele analityczne, agregaty sezonowe i per-mecz.

ASCII:
- API (SofaScore) → Bronze (MinIO) → Silver (PostgreSQL) → dbt (Gold) → BI (Metabase)

## Komponenty
- Klient API:
  - [`etl.bronze.client.SofascoreClient`](etl/bronze/client.py)
  - Schematy walidacji: [`etl.bronze.client.MatchBasic`](etl/bronze/client.py), [`etl.bronze.client.SeasonBasic`](etl/bronze/client.py)
- Składowanie (Bronze):
  - [`etl.bronze.storage.BronzeStorageManager`](etl/bronze/storage.py)
  - Utility: [`etl.bronze.client.get_partition_key`](etl/bronze/client.py), [`etl.bronze.client.get_bronze_path`](etl/bronze/client.py)
- Ekstraktory:
  - Mecze/inkrement: [`etl.bronze.extractors.incremental_extractor`](etl/bronze/extractors/incremental_extractor.py)
  - Statystyki: [`etl.bronze.extractors.statistics_extractor.StatisticsFetcher`](etl/bronze/extractors/statistics_extractor.py)
  - Produkcyjny ETL 25/26 (backup): [docker/backup/scripts/extract_ekstraklasa.py](docker/backup/scripts/extract_ekstraklasa.py)
- Diagnostyka i eksploracja:
  - [`etl.bronze.diagnostics.season_diagnostic.EkstraklasaSeasonDiagnostic`](etl/bronze/diagnostics/season_diagnostic.py)
  - [etl/bronze/diagnostics/content_explorer.py](etl/bronze/diagnostics/content_explorer.py)
- Orkiestracja (Airflow):
  - Incremental matches: [airflow/dags/02_bronze_load_incremental_matches.py](airflow/dags/02_bronze_load_incremental_matches.py)
  - Incremental stats: [airflow/dags/backup/03_bronze_extract_incremental_stats.py](airflow/dags/backup/03_bronze_extract_incremental_stats.py), [airflow/dags/backup/04_bronze_load_incremental_stats.py](airflow/dags/backup/04_bronze_load_incremental_stats.py)
  - Silver staging: [airflow/dags/backup/05_silver_stage_incremental.py](airflow/dags/backup/05_silver_stage_incremental.py), [airflow/dags/backup/silver_stage_full.py](airflow/dags/backup/silver_stage_full.py)
  - Backfill (smart): [airflow/dags/bronze_backfill_historical.py](airflow/dags/bronze_backfill_historical.py)
- Transformacje (dbt):
  - Projekt: [dbt/dbt_project.yml](dbt/dbt_project.yml), modele w [dbt/models/](dbt/models/)
- Testy i narzędzia:
  - Smoke: [tests/smoke_test.py](tests/smoke_test.py)
  - Storage: [tests/test_storage.py](tests/test_storage.py)
  - Health: [tests/health_check.py](tests/health_check.py)

## Model i schemat danych
- Partycjonowanie Bronze:
  - Ścieżka przykładowa: match_statistics/season_2025_26/date=YYYY-MM-DD/match_{id}.json
  - Ogólny klucz partycji:
    - $partition = tournament\_id + "/season\_id=" + season\_id + "/date=" + YYYY\text{-}MM\text{-}DD$
- Silver:
  - Tabele staging: silver.staging_matches, silver.staging_stats (ładowane przez DAGi Silver).
- Gold:
  - Modele analityczne i agregaty (dbt run/test).

Źródła i referencje API: [docs/reference_sources/sofascore_API_endpoints_docs.txt](docs/reference_sources/sofascore_API_endpoints_docs.txt)

## Przepływy (DAGi) – skrót
- Inkrementalny update:
  - Mecze → [02_bronze_load_incremental_matches.py](airflow/dags/02_bronze_load_incremental_matches.py)
  - Statystyki → [03_bronze_extract_incremental_stats.py](airflow/dags/backup/03_bronze_extract_incremental_stats.py) → [04_bronze_load_incremental_stats.py](airflow/dags/backup/04_bronze_load_incremental_stats.py)
  - Staging → [05_silver_stage_incremental.py](airflow/dags/backup/05_silver_stage_incremental.py)
  - dbt → [dbt/dbt_project.yml](dbt/dbt_project.yml)
- Backfill historyczny (z checkpointem) → [airflow/dags/bronze_backfill_historical.py](airflow/dags/bronze_backfill_historical.py)

## Jakość danych i testy
- Testy:
  - pytest smoke/integration: [tests/](tests/)
  - Kontrola staging: [airflow/dags/backup/silver_stage_full.py](airflow/dags/backup/silver_stage_full.py)
  - Diagnostyka sezonów i kompletności: [`etl.bronze.diagnostics.season_diagnostic`](etl/bronze/diagnostics/season_diagnostic.py)
- Metryki jakości (przykład):
  - Freshness: $freshness = now() - \max(start\_timestamp)$
  - Skuteczność ekstrakcji: $success\_rate = \frac{stored\_batches - errors}{stored\_batches}$

## Wdrożenie i infrastruktura
- Kontenery i obrazy:
  - Airflow: [docker/Dockerfile](docker/Dockerfile)
  - ETL/dbt: [docker/Dockerfile.etl](docker/Dockerfile.etl), [docker/Dockerfile.dbt](docker/Dockerfile.dbt)
  - Compose: [docker/docker-compose.yml](docker/docker-compose.yml)
- Profile dbt: [docker/dbt_profiles.yml](docker/dbt_profiles.yml)
- Konfiguracje środowisk (prod/dev/test/staging): [docs/ENVIRONMENT_CONFIG.md](docs/ENVIRONMENT_CONFIG.md)

Proponowany CI/CD (zarys):
- Lint/format → Testy (pytest) → Build obrazów → dbt compile/test → Deploy Airflow/ETL.

## Bezpieczeństwo i zarządzanie danymi
- Sekrety przez zmienne środowiskowe/Secret Manager (np. RAPIDAPI_KEY).
- Dostępy MinIO ograniczone per bucket/prefix.
- Logi strukturalne i centralizacja (do wdrożenia zgodnie z [docs/ENVIRONMENT_CONFIG.md](docs/ENVIRONMENT_CONFIG.md)).
- Zgodność: audyt dostępu do danych produkcyjnych; rotacja kluczy.

## Skalowalność i modularność
- Concurrency, rate limiting i retry w ekstraktorach (zmienne ENV – patrz [docs/ENVIRONMENT_CONFIG.md](docs/ENVIRONMENT_CONFIG.md)).
- Horyzontalne skalowanie ETL (repliki w compose/Swarm/K8s).
- Modularne ekstraktory i ujednolicone kontrakty (wynik batchu, manifesty).

## Integracje zewnętrzne
- SofaScore API (RapidAPI): referencje endpointów w [docs/reference_sources/sofascore_API_endpoints_docs.txt](docs/reference_sources/sofascore_API_endpoints_docs.txt)
- Metabase (BI) – konsumpcja modeli Gold.

## Uwagi dot. utrzymania
- Runbook operacyjny:
  - Smoke test: [tests/smoke_test.py](tests/smoke_test.py)
  - Diagnostyka sezonów: [etl/bronze/diagnostics/season_diagnostic.py](etl/bronze/diagnostics/season_diagnostic.py)
  - Eksploracja Bronze: [etl/bronze/diagnostics/content_explorer.py](etl/bronze/diagnostics/content_explorer.py)
- Monitoring/alerting: sekcja Monitoring w [docs/ENVIRONMENT_CONFIG.md](docs/ENVIRONMENT_CONFIG.md)

## Roadmap / Technical Debt
- Ujednolicenie nazewnictwa i refaktoryzacja (por. [to_do.txt](to_do.txt))
- Logi JSON + centralizacja
- Format kolumnowy (Parquet) i format tabelaryczny z ewolucją schematu
- Testy dbt Source Freshness i dokumentacja dbt docs

## Referencje
- Quick Start i development: [docs/README.md](docs/README.md)
- Data flow: [docs/data_flow.txt](docs/data_flow.txt)
- Specyfikacja i analiza: [docs/specification.txt](docs/specification.txt), [docs/data_analysis_review.md](docs/data_analysis_review.md)
- Kluczowe DAGi: [airflow/dags/](airflow/dags/)
- ETL Bronze: [etl/bronze/](etl/bronze/)