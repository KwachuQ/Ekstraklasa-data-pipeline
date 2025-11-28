from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowFailException
import logging

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def check_bronze_data(**context):
    """Check if data exists in bronze layer before proceeding"""
    logging.info("Checking bronze layer data availability...")
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Check if bronze tables exist
    tables_check = """
        SELECT 
            EXISTS (SELECT FROM information_schema.tables 
                    WHERE table_schema = 'bronze' AND table_name = 'full_matches_data') as matches_exists,
            EXISTS (SELECT FROM information_schema.tables 
                    WHERE table_schema = 'bronze' AND table_name = 'full_stats_data') as stats_exists
    """
    
    result = hook.get_first(tables_check)
    matches_exists, stats_exists = result
    
    if not matches_exists:
        raise AirflowFailException(
            "Table bronze.full_matches_data does not exist! "
            "Run DAG 11_bronze_load_historical_matches first."
        )
    
    if not stats_exists:
        raise AirflowFailException(
            "Table bronze.full_stats_data does not exist! "
            "Run DAG 13_bronze_load_historical_stats first."
        )
    
    # Check data counts
    matches_count = hook.get_first("SELECT COUNT(*) FROM bronze.full_matches_data")[0]
    stats_count = hook.get_first("SELECT COUNT(*) FROM bronze.full_stats_data")[0]
    
    logging.info(f"Bronze layer status:")
    logging.info(f"  ✓ full_matches_data: {matches_count:,} records")
    logging.info(f"  ✓ full_stats_data: {stats_count:,} records")
    
    if matches_count == 0:
        raise AirflowFailException(
            "bronze.full_matches_data is EMPTY! "
            "Run DAG 11_bronze_load_historical_matches to populate it."
        )
    
    if stats_count == 0:
        raise AirflowFailException(
            "bronze.full_stats_data is EMPTY! "
            "Run DAG 13_bronze_load_historical_stats to populate it."
        )
    
    context['ti'].xcom_push(key='bronze_matches_count', value=matches_count)
    context['ti'].xcom_push(key='bronze_stats_count', value=stats_count)
    
    return {'matches': matches_count, 'stats': stats_count}


def validate_staging_data(**context):
    """Validate staging data quality and integrity"""
    logging.info("Validating staging data...")
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Get counts from previous step
    bronze_matches = context['ti'].xcom_pull(task_ids='check_bronze_data', key='bronze_matches_count')
    
    # Check staging counts
    staging_matches = hook.get_first("SELECT COUNT(*) FROM silver.staging_matches")[0]
    staging_stats = hook.get_first("SELECT COUNT(*) FROM silver.staging_stats")[0]
    
    logging.info(f"Staging validation:")
    logging.info(f"  Bronze matches: {bronze_matches:,}")
    logging.info(f"  Staging matches: {staging_matches:,}")
    logging.info(f"  Staging stats: {staging_stats:,}")
    
    if staging_matches == 0:
        raise AirflowFailException("staging_matches is EMPTY after insert!")
    
    # Critical validation tests (must pass)
    critical_tests = [
        (
            "NULL in key columns",
            """SELECT COUNT(*) FROM silver.staging_matches 
               WHERE match_id IS NULL OR home_team_id IS NULL OR away_team_id IS NULL"""
        ),
        (
            "Excluded status types in staging",
            """SELECT COUNT(*) FROM silver.staging_matches 
               WHERE status_type IN ('postponed', 'cancelled', 'retired')"""
        ),
        (
            "Orphan statistics (stats without matches)",
            """SELECT COUNT(DISTINCT s.match_id) 
               FROM silver.staging_stats s 
               LEFT JOIN silver.staging_matches m ON s.match_id = m.match_id 
               WHERE m.match_id IS NULL"""
        ),
    ]
    
    # Run critical tests
    failed_tests = []
    for test_name, query in critical_tests:
        result = hook.get_first(query)[0]
        if result > 0:
            failed_tests.append(f"{test_name}: {result} records")
            logging.error(f"  ✗ FAILED: {test_name} ({result} records)")
        else:
            logging.info(f"  ✓ PASSED: {test_name}")
    
    if failed_tests:
        raise AirflowFailException(
            f"Critical validation failed:\n  - " + "\n  - ".join(failed_tests)
        )
    
    # Check for matches without statistics (detailed reporting)
    matches_without_stats_query = """
        SELECT 
            m.match_id,
            m.start_timestamp,
            m.status_type,
            m.status_description,
            m.home_team_name,
            m.away_team_name,
            m.home_score_current,
            m.away_score_current
        FROM silver.staging_matches m 
        WHERE NOT EXISTS (
            SELECT 1 FROM silver.staging_stats s WHERE s.match_id = m.match_id
        )
        ORDER BY m.start_timestamp DESC
    """
    
    matches_without_stats = hook.get_pandas_df(matches_without_stats_query)
    
    if len(matches_without_stats) > 0:
        logging.warning("="*70)
        logging.warning(f"⚠ WARNING: Found {len(matches_without_stats)} match(es) without statistics")
        logging.warning("="*70)
        
        # Filter for finished matches (should have stats)
        finished_statuses = ['finished', 'ended', 'played', 'ft']
        finished_without_stats = matches_without_stats[
            matches_without_stats['status_type'].str.lower().isin(finished_statuses)
        ]
        
        if len(finished_without_stats) > 0:
            logging.warning(f"\n🔴 ATTENTION: {len(finished_without_stats)} FINISHED match(es) missing statistics!")
            logging.warning("These matches should have statistics but don't:\n")
            
            for idx, row in finished_without_stats.iterrows():
                logging.warning(f"  Match ID: {row['match_id']}")
                logging.warning(f"    Date: {row['start_timestamp']}")
                logging.warning(f"    Status: {row['status_type']} ({row['status_description']})")
                logging.warning(f"    Match: {row['home_team_name']} vs {row['away_team_name']}")
                logging.warning(f"    Score: {row['home_score_current']} - {row['away_score_current']}")
                logging.warning(f"    Action: Check API response for match_id={row['match_id']}\n")
        
        # Show non-finished matches without stats (expected)
        not_finished_without_stats = matches_without_stats[
            ~matches_without_stats['status_type'].str.lower().isin(finished_statuses)
        ]
        
        if len(not_finished_without_stats) > 0:
            logging.info(f"\nℹ️  INFO: {len(not_finished_without_stats)} non-finished match(es) without statistics (expected):")
            
            for idx, row in not_finished_without_stats.iterrows():
                logging.info(f"  - Match {row['match_id']}: {row['home_team_name']} vs {row['away_team_name']} ({row['status_type']})")
        
        logging.warning("="*70 + "\n")
    else:
        logging.info(f"  ✓ PASSED: All matches have statistics")
    
    logging.info("\n✓ All critical validations PASSED!")
    
    return {
        'staging_matches': staging_matches,
        'staging_stats': staging_stats,
        'validation_passed': True,
        'matches_without_stats': len(matches_without_stats),
        'finished_without_stats': len(finished_without_stats) if len(matches_without_stats) > 0 else 0,
        'problem_match_ids': finished_without_stats['match_id'].tolist() if len(matches_without_stats) > 0 and len(finished_without_stats) > 0 else []
    }


def generate_report(**context):
    """Generate comprehensive staging layer report"""
    logging.info("Generating staging layer report...")
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Summary statistics
    summary_query = """
        SELECT 
            'Staging Matches' as table_name,
            COUNT(*) as total_records,
            COUNT(DISTINCT match_id) as unique_matches,
            MIN(start_timestamp)::DATE as earliest_match,
            MAX(start_timestamp)::DATE as latest_match,
            COUNT(DISTINCT season_name) as seasons,
            COUNT(DISTINCT tournament_id) as tournaments
        FROM silver.staging_matches
        
        UNION ALL
        
        SELECT 
            'Staging Stats' as table_name,
            COUNT(*) as total_records,
            COUNT(DISTINCT match_id) as unique_matches,
            NULL, NULL,
            COUNT(DISTINCT group_name) as groups,
            COUNT(DISTINCT statistic_key) as unique_stats
        FROM silver.staging_stats
    """
    
    summary_df = hook.get_pandas_df(summary_query)
    
    logging.info("\n" + "="*70)
    logging.info("STAGING LAYER SUMMARY")
    logging.info("="*70)
    logging.info("\n" + summary_df.to_string(index=False))
    
    # Statistics breakdown by group
    groups_query = """
        SELECT 
            group_name, 
            COUNT(*) as total_records,
            COUNT(DISTINCT match_id) as matches,
            COUNT(DISTINCT statistic_key) as unique_stats
        FROM silver.staging_stats 
        GROUP BY group_name 
        ORDER BY total_records DESC
    """
    
    groups_df = hook.get_pandas_df(groups_query)
    
    logging.info("\nStatistics by Group:")
    logging.info(groups_df.to_string(index=False))
    
    # Season breakdown
    season_query = """
        SELECT 
            season_name,
            COUNT(*) as matches,
            COUNT(DISTINCT home_team_id) + COUNT(DISTINCT away_team_id) as unique_teams,
            MIN(start_timestamp)::DATE as first_match,
            MAX(start_timestamp)::DATE as last_match
        FROM silver.staging_matches
        GROUP BY season_name
        ORDER BY season_name DESC
    """
    
    season_df = hook.get_pandas_df(season_query)
    
    logging.info("\nMatches by Season:")
    logging.info(season_df.to_string(index=False))
    logging.info("="*70 + "\n")
    
    return {
        'report_generated': True,
        'summary': summary_df.to_dict(),
        'groups': groups_df.to_dict()
    }


with DAG(
    dag_id='14_silver_stage_full',
    default_args=default_args,
    description='Create staging tables in Silver layer from Bronze full data (historical load)',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['silver', 'historical', 'staging', 'full-load']
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
            -- Create silver schema if not exists
            CREATE SCHEMA IF NOT EXISTS silver;
            
            -- Drop existing tables (full refresh)
            DROP TABLE IF EXISTS silver.staging_stats CASCADE;
            DROP TABLE IF EXISTS silver.staging_matches CASCADE;
            
            -- Create staging_matches table
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
            
            -- Create indexes for performance
            CREATE INDEX idx_staging_matches_timestamp ON silver.staging_matches(start_timestamp);
            CREATE INDEX idx_staging_matches_tournament ON silver.staging_matches(tournament_id);
            CREATE INDEX idx_staging_matches_season ON silver.staging_matches(season_id);
            CREATE INDEX idx_staging_matches_teams ON silver.staging_matches(home_team_id, away_team_id);
            CREATE INDEX idx_staging_matches_status ON silver.staging_matches(status_type);
        """,
        execution_timeout=timedelta(minutes=10)
    )
    
    load_staging_matches = PostgresOperator(
        task_id='load_staging_matches',
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
                country_name, injury_time_2, current_period_start_timestamp, 
                CURRENT_TIMESTAMP
            FROM bronze.full_matches_data
            WHERE status_type NOT IN ('postponed', 'cancelled', 'retired')
              AND (status_description != 'Removed' 
                   OR EXISTS (
                       SELECT 1 FROM bronze.full_stats_data 
                       WHERE match_id = full_matches_data.match_id
                   ));
        """,
        execution_timeout=timedelta(minutes=10)
    )
    
    create_staging_stats = PostgresOperator(
        task_id='create_staging_stats',
        postgres_conn_id='postgres_default',
        sql="""
            -- Create staging_stats table with foreign key to staging_matches
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
            
            -- Create indexes for performance
            CREATE INDEX idx_staging_stats_match ON silver.staging_stats(match_id);
            CREATE INDEX idx_staging_stats_period ON silver.staging_stats(period);
            CREATE INDEX idx_staging_stats_group ON silver.staging_stats(group_name);
            CREATE INDEX idx_staging_stats_key ON silver.staging_stats(statistic_key);
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
                periods_count, groups_count, statistics_in_period, created_at
            )
            SELECT 
                s.match_id, s.period, s.group_name, s.statistic_key, s.statistic_name,
                s.tournament_id, s.season_id, s.statistics_type, s.value_type,
                s.home_value_text, s.away_value_text, s.home_value_numeric, s.away_value_numeric,
                s.advantage, s.home_percentage, s.away_percentage, s.value_difference,
                s.stat_order_in_group, s.stats_count_in_group, s.total_statistics_count,
                s.periods_count, s.groups_count, s.statistics_in_period,
                CURRENT_TIMESTAMP
            FROM bronze.full_stats_data s
            INNER JOIN silver.staging_matches m ON s.match_id = m.match_id;
        """,
        execution_timeout=timedelta(minutes=15)
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
    
    # Task dependencies
    start >> check_bronze >> create_staging_matches >> load_staging_matches
    load_staging_matches >> create_staging_stats >> load_staging_stats >> validate >> report >> end