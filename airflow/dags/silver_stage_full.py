from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_bronze_data(**context):
    """Sprawdź czy dane w bronze istnieją"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    matches_count = hook.get_first("SELECT COUNT(*) FROM bronze.full_matches_data")[0]
    stats_count = hook.get_first("SELECT COUNT(*) FROM bronze.full_stats_data")[0]
    
    print(f"📊 Bronze layer status:")
    print(f"   - full_matches_data: {matches_count:,} records")
    print(f"   - full_stats_data: {stats_count:,} records")
    
    if matches_count == 0:
        raise ValueError("⚠️  bronze.full_matches_data is EMPTY!")
    if stats_count == 0:
        raise ValueError("⚠️  bronze.full_stats_data is EMPTY!")
    
    context['ti'].xcom_push(key='bronze_matches_count', value=matches_count)
    context['ti'].xcom_push(key='bronze_stats_count', value=stats_count)

def validate_staging_data(**context):
    """Walidacja danych staging"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Pobierz liczby z poprzedniego kroku
    bronze_matches = context['ti'].xcom_pull(task_ids='check_bronze_data', key='bronze_matches_count')
    
    # Sprawdź staging
    staging_matches = hook.get_first("SELECT COUNT(*) FROM silver.staging_matches")[0]
    staging_stats = hook.get_first("SELECT COUNT(*) FROM silver.staging_stats")[0]
    
    print(f"\n📊 Staging validation:")
    print(f"   - Bronze matches: {bronze_matches:,}")
    print(f"   - Staging matches: {staging_matches:,}")
    print(f"   - Staging stats: {staging_stats:,}")
    
    if staging_matches == 0:
        raise ValueError("❌ staging_matches is EMPTY after insert!")
    
    # Testy walidacyjne
    tests = [
        ("NULL in key columns", "SELECT COUNT(*) FROM silver.staging_matches WHERE match_id IS NULL OR home_team_id IS NULL OR away_team_id IS NULL"),
        ("Excluded matches", "SELECT COUNT(*) FROM silver.staging_matches WHERE status_type IN ('postponed', 'cancelled', 'retired')"),
        ("Orphan statistics", "SELECT COUNT(DISTINCT s.match_id) FROM silver.staging_stats s LEFT JOIN silver.staging_matches m ON s.match_id = m.match_id WHERE m.match_id IS NULL"),
    ]
    
    for test_name, query in tests:
        result = hook.get_first(query)[0]
        if result > 0:
            raise ValueError(f"❌ Validation failed: {test_name} ({result} records)")
        print(f"   ✅ {test_name}: PASSED")
    
    print("\n✅ All validations PASSED!")

def generate_report(**context):
    """Raport końcowy"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Podsumowanie
    summary_df = hook.get_pandas_df("""
        SELECT 
            'Staging Matches' as table_name,
            COUNT(*) as total_records,
            COUNT(DISTINCT match_id) as unique_matches,
            MIN(start_timestamp)::DATE as earliest_match,
            MAX(start_timestamp)::DATE as latest_match,
            COUNT(DISTINCT season_name) as seasons
        FROM silver.staging_matches
        
        UNION ALL
        
        SELECT 
            'Staging Stats' as table_name,
            COUNT(*) as total_records,
            COUNT(DISTINCT match_id) as unique_matches,
            NULL, NULL,
            COUNT(DISTINCT group_name) as groups
        FROM silver.staging_stats
    """)
    
    print("\n" + "="*60)
    print("📊 STAGING LAYER SUMMARY")
    print("="*60)
    print(summary_df.to_string(index=False))
    
    # Grupy statystyk
    groups_df = hook.get_pandas_df("""
        SELECT group_name, COUNT(*) as records 
        FROM silver.staging_stats 
        GROUP BY group_name 
        ORDER BY records DESC
    """)
    
    print("\n📈 Statistics by group:")
    print(groups_df.to_string(index=False))
    print("="*60)

with DAG(
    dag_id='silver_stage_full',
    default_args=default_args,
    description='Create staging tables in Silver layer',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'silver', 'staging']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    check_bronze = PythonOperator(
        task_id='check_bronze_data',
        python_callable=check_bronze_data
    )
    
    create_staging_matches = PostgresOperator(
        task_id='create_staging_matches',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE SCHEMA IF NOT EXISTS silver;
            DROP TABLE IF EXISTS silver.staging_stats CASCADE;
            DROP TABLE IF EXISTS silver.staging_matches CASCADE;
            
            CREATE TABLE silver.staging_matches (
                match_id INTEGER PRIMARY KEY,
                match_slug TEXT,
                start_timestamp TIMESTAMPTZ,
                status_type TEXT,
                status_description TEXT,
                winner_code INTEGER,
                home_score_current INTEGER,
                away_score_current INTEGER,
                home_score_period1 INTEGER,
                home_score_period2 INTEGER,
                away_score_period1 INTEGER,
                away_score_period2 INTEGER,
                home_team_id INTEGER NOT NULL,
                home_team_name TEXT,
                away_team_id INTEGER NOT NULL,
                away_team_name TEXT,
                tournament_id INTEGER,
                tournament_name TEXT,
                season_id INTEGER,
                season_name TEXT,
                season_year TEXT,
                country_name TEXT,
                injury_time_2 INTEGER,
                current_period_start_timestamp TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX idx_staging_matches_timestamp ON silver.staging_matches(start_timestamp);
            CREATE INDEX idx_staging_matches_tournament ON silver.staging_matches(tournament_id);
            CREATE INDEX idx_staging_matches_season ON silver.staging_matches(season_id);
            CREATE INDEX idx_staging_matches_teams ON silver.staging_matches(home_team_id, away_team_id);
        """,
        execution_timeout=timedelta(minutes=10)
    )
    
    load_staging_matches = PostgresOperator(
        task_id='load_staging_matches',
        postgres_conn_id='postgres_default',
        sql="""
            INSERT INTO silver.staging_matches
            SELECT 
                match_id, match_slug, start_timestamp, status_type, status_description,
                winner_code, home_score_current, away_score_current,
                home_score_period1, home_score_period2, away_score_period1, away_score_period2,
                home_team_id, home_team_name, away_team_id, away_team_name,
                tournament_id, tournament_name, season_id, season_name, season_year,
                country_name, injury_time_2, current_period_start_timestamp, CURRENT_TIMESTAMP
            FROM bronze.full_matches_data
            WHERE status_type NOT IN ('postponed', 'canceled', 'retired')
              AND (status_description != 'Removed' 
                   OR EXISTS (SELECT 1 FROM bronze.full_stats_data WHERE match_id = full_matches_data.match_id));
        """,
        execution_timeout=timedelta(minutes=10)
    )
    
    create_staging_stats = PostgresOperator(
        task_id='create_staging_stats',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE silver.staging_stats (
                stat_id SERIAL PRIMARY KEY,
                match_id INTEGER NOT NULL REFERENCES silver.staging_matches(match_id) ON DELETE CASCADE,
                period TEXT,
                group_name TEXT,
                statistic_key TEXT,
                statistic_name TEXT,
                tournament_id INTEGER,
                season_id INTEGER,
                statistics_type TEXT,
                value_type TEXT,
                home_value_text TEXT,
                away_value_text TEXT,
                home_value_numeric NUMERIC,
                away_value_numeric NUMERIC,
                advantage TEXT,
                home_percentage NUMERIC,
                away_percentage NUMERIC,
                value_difference NUMERIC,
                stat_order_in_group BIGINT,
                stats_count_in_group BIGINT,
                total_statistics_count BIGINT,
                periods_count BIGINT,
                groups_count BIGINT,
                statistics_in_period BIGINT,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX idx_staging_stats_match ON silver.staging_stats(match_id);
            CREATE INDEX idx_staging_stats_period ON silver.staging_stats(period);
            CREATE INDEX idx_staging_stats_group ON silver.staging_stats(group_name);
            CREATE INDEX idx_staging_stats_composite ON silver.staging_stats(match_id, period, group_name, statistic_key);
        """,
        execution_timeout=timedelta(minutes=10)
    )
    
    load_staging_stats = PostgresOperator(
        task_id='load_staging_stats',
        postgres_conn_id='postgres_default',
        sql="""
            INSERT INTO silver.staging_stats (
                match_id, period, group_name, statistic_key, statistic_name,
                tournament_id, season_id, statistics_type, value_type,
                home_value_text, away_value_text, home_value_numeric, away_value_numeric,
                advantage, home_percentage, away_percentage, value_difference,
                stat_order_in_group, stats_count_in_group, total_statistics_count,
                periods_count, groups_count, statistics_in_period
            )
            SELECT 
                s.match_id, s.period, s.group_name, s.statistic_key, s.statistic_name,
                s.tournament_id, s.season_id, s.statistics_type, s.value_type,
                s.home_value_text, s.away_value_text, s.home_value_numeric, s.away_value_numeric,
                s.advantage, s.home_percentage, s.away_percentage, s.value_difference,
                s.stat_order_in_group, s.stats_count_in_group, s.total_statistics_count,
                s.periods_count, s.groups_count, s.statistics_in_period
            FROM bronze.full_stats_data s
            INNER JOIN silver.staging_matches m ON s.match_id = m.match_id;
        """,
        execution_timeout=timedelta(minutes=10)
    )
    
    validate = PythonOperator(
        task_id='validate_staging_data',
        python_callable=validate_staging_data
    )
    
    report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report
    )
    
    end = EmptyOperator(task_id='end')
    
    # Flow
    start >> check_bronze >> create_staging_matches >> load_staging_matches
    load_staging_matches >> create_staging_stats >> load_staging_stats >> validate >> report >> end