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

def get_max_timestamp(**context):
    """Znajdź maksymalny start_timestamp w staging"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    table_exists = hook.get_first("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'silver' 
            AND table_name = 'staging_matches'
        )
    """)[0]
    
    if not table_exists:
        print("⚠️  Staging tables don't exist - will load ALL data")
        max_timestamp = '1900-01-01 00:00:00'
    else:
        result = hook.get_first("SELECT COALESCE(MAX(start_timestamp), '1900-01-01 00:00:00'::timestamp) FROM silver.staging_matches")
        max_timestamp = result[0] if result else '1900-01-01 00:00:00'
        print(f"📊 Current max start_timestamp in staging: {max_timestamp}")
    
    context['ti'].xcom_push(key='max_timestamp', value=str(max_timestamp))

def check_new_data(**context):
    """Sprawdź ile nowych danych jest w Bronze"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    max_timestamp = context['ti'].xcom_pull(task_ids='get_max_timestamp', key='max_timestamp')
    
    new_matches = hook.get_first(f"""
        SELECT COUNT(*) 
        FROM bronze.full_matches_data 
        WHERE start_timestamp > '{max_timestamp}'::timestamp
          AND status_type NOT IN ('postponed', 'cancelled', 'retired')
          AND (status_description != 'Removed' 
               OR EXISTS (SELECT 1 FROM bronze.full_stats_data WHERE match_id = full_matches_data.match_id))
    """)[0]
    
    print(f"\n📊 New data to process:")
    print(f"   - Max staging timestamp: {max_timestamp}")
    print(f"   - New matches to insert: {new_matches}")
    
    if new_matches == 0:
        print("\n✅ No new data - staging is up to date!")
    
    context['ti'].xcom_push(key='new_matches_count', value=new_matches)

def validate_incremental_load(**context):
    """Walidacja załadowanych danych"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    new_count = context['ti'].xcom_pull(task_ids='check_new_data', key='new_matches_count')
    
    if new_count == 0:
        print("⏭️  Skipping validation - no new data loaded")
        return
    
    staging_matches = hook.get_first("SELECT COUNT(*) FROM silver.staging_matches")[0]
    staging_stats = hook.get_first("SELECT COUNT(*) FROM silver.staging_stats")[0]
    
    print(f"\n📊 Load validation:")
    print(f"   - New matches inserted: {new_count}")
    print(f"   - Total staging matches: {staging_matches}")
    print(f"   - Total staging stats: {staging_stats}")
    
    tests = [
        ("NULL in key columns", 
         "SELECT COUNT(*) FROM silver.staging_matches WHERE match_id IS NULL OR home_team_id IS NULL OR away_team_id IS NULL"),
        ("Excluded statuses", 
         "SELECT COUNT(*) FROM silver.staging_matches WHERE status_type IN ('postponed', 'cancelled', 'retired')"),
        ("Orphan statistics", 
         "SELECT COUNT(DISTINCT s.match_id) FROM silver.staging_stats s LEFT JOIN silver.staging_matches m ON s.match_id = m.match_id WHERE m.match_id IS NULL"),
        ("Duplicate match_ids", 
         "SELECT COUNT(*) - COUNT(DISTINCT match_id) FROM silver.staging_matches"),
    ]
    
    all_passed = True
    for test_name, query in tests:
        result = hook.get_first(query)[0]
        if result > 0:
            print(f"   ❌ {test_name}: FAILED ({result} records)")
            all_passed = False
        else:
            print(f"   ✅ {test_name}: PASSED")
    
    if not all_passed:
        raise ValueError("❌ Validation FAILED")
    
    print("\n✅ All validations PASSED!")

def generate_incremental_report(**context):
    """Raport inkrementalnego ładowania"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    max_timestamp = context['ti'].xcom_pull(task_ids='get_max_timestamp', key='max_timestamp')
    new_count = context['ti'].xcom_pull(task_ids='check_new_data', key='new_matches_count')
    
    if new_count == 0:
        print("⏭️  No new data loaded - skipping report")
        return
    
    summary_df = hook.get_pandas_df(f"""
        SELECT 
            'All Data' as data_range,
            COUNT(*) as total_matches,
            MIN(start_timestamp)::DATE as earliest_match,
            MAX(start_timestamp)::DATE as latest_match
        FROM silver.staging_matches
        
        UNION ALL
        
        SELECT 
            'Newly Added' as data_range,
            COUNT(*) as total_matches,
            MIN(start_timestamp)::DATE as earliest_match,
            MAX(start_timestamp)::DATE as latest_match
        FROM silver.staging_matches
        WHERE start_timestamp > '{max_timestamp}'::timestamp
    """)
    
    print("\n" + "="*60)
    print("📊 INCREMENTAL LOAD SUMMARY")
    print("="*60)
    print(summary_df.to_string(index=False))
    print("="*60)

with DAG(
    dag_id='05_silver_stage_incremental',
    default_args=default_args,
    description='Incremental update of Silver staging tables',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'silver', 'staging', 'incremental']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    get_max_ts = PythonOperator(
        task_id='get_max_timestamp',
        python_callable=get_max_timestamp
    )
    
    check_new = PythonOperator(
        task_id='check_new_data',
        python_callable=check_new_data
    )
    
    insert_new_matches = PostgresOperator(
        task_id='insert_new_matches',
        postgres_conn_id='postgres_default',
        sql="""
            INSERT INTO silver.staging_matches (
                match_id, match_slug, start_timestamp, status_type, status_description,
                winner_code, home_score_current, away_score_current,
                home_score_period1, home_score_period2, away_score_period1, away_score_period2,
                home_team_id, home_team_name, away_team_id, away_team_name,
                tournament_id, tournament_name, season_id, season_name, season_year,
                country_name, injury_time_2, current_period_start_timestamp, created_at
            )
            SELECT 
                match_id, match_slug, start_timestamp, status_type, status_description,
                winner_code, home_score_current, away_score_current,
                home_score_period1, home_score_period2, away_score_period1, away_score_period2,
                home_team_id, home_team_name, away_team_id, away_team_name,
                tournament_id, tournament_name, season_id, season_name, season_year,
                country_name, injury_time_2, current_period_start_timestamp, CURRENT_TIMESTAMP
            FROM bronze.full_matches_data
            WHERE start_timestamp > (SELECT COALESCE(MAX(start_timestamp), '1900-01-01 00:00:00'::timestamp) FROM silver.staging_matches)
              AND status_type NOT IN ('postponed', 'cancelled', 'retired')
              AND (status_description != 'Removed' 
                   OR EXISTS (SELECT 1 FROM bronze.full_stats_data WHERE match_id = full_matches_data.match_id))
            ON CONFLICT (match_id) DO NOTHING;
        """,
        execution_timeout=timedelta(minutes=10)
    )
    
    insert_new_stats = PostgresOperator(
        task_id='insert_new_stats',
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
            WHERE s.match_id IN (
                SELECT match_id 
                FROM silver.staging_matches 
                WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '5 minutes'
            );
        """,
        execution_timeout=timedelta(minutes=10)
    )
    
    validate = PythonOperator(
        task_id='validate_incremental_load',
        python_callable=validate_incremental_load
    )
    
    report = PythonOperator(
        task_id='generate_incremental_report',
        python_callable=generate_incremental_report
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> get_max_ts >> check_new >> insert_new_matches >> insert_new_stats >> validate >> report >> end