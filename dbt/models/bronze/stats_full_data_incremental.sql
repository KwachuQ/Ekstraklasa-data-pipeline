{{
  config(
    materialized='incremental',
    alias='full_stats_data',
    unique_key=['match_id', 'period', 'group_name', 'statistic_key'],
    on_schema_change='fail',
    incremental_strategy='merge'
  )
}}

WITH source_data AS (
    SELECT 
        rs.id,
        rs.data,
        rs.match_id,
        rs.tournament_id,
        rs.file_path,
        rs.batch_id,
        rs.ingestion_timestamp,
        fm.season_id
    FROM {{ source('bronze', 'raw_stats') }} rs
    LEFT JOIN {{ source('bronze', 'full_matches_data') }} fm ON rs.match_id = fm.match_id
    WHERE rs.data IS NOT NULL
      AND rs.data->'statistics' IS NOT NULL
    {% if is_incremental() %}
        AND rs.match_id NOT IN (
            SELECT DISTINCT match_id 
            FROM {{ this }}
        )
    {% endif %}
),

-- Flatten the statistics structure
flattened_stats AS (
    SELECT
        -- Core Match Information
        source_data.match_id,
        source_data.tournament_id,
        source_data.season_id,
        source_data.file_path,
        source_data.batch_id,
        source_data.ingestion_timestamp,
        
        -- Statistics Period
        stat_period.value->>'period' as period,
        
        -- Statistics Groups
        stat_group.value->>'groupName' as group_name,
        
        -- Individual Statistics Items
        stat_item.value->>'name' as statistic_name,
        stat_item.value->>'key' as statistic_key,
        stat_item.value->>'home' as home_value_text,
        stat_item.value->>'away' as away_value_text,
        (stat_item.value->>'homeValue')::NUMERIC as home_value_numeric,
        (stat_item.value->>'awayValue')::NUMERIC as away_value_numeric,
        (stat_item.value->>'compareCode')::INTEGER as compare_code,
        stat_item.value->>'statisticsType' as statistics_type,
        stat_item.value->>'valueType' as value_type,
        (stat_item.value->>'renderType')::INTEGER as render_type,
        
        -- Original JSON data for reference
        source_data.data as original_data
        
    FROM source_data
    CROSS JOIN LATERAL jsonb_array_elements(source_data.data->'statistics'->'statistics') as stat_period(value)
    CROSS JOIN LATERAL jsonb_array_elements(stat_period.value->'groups') as stat_group(value)
    CROSS JOIN LATERAL jsonb_array_elements(stat_group.value->'statisticsItems') as stat_item(value)
),

-- Calculate match-level aggregations
match_aggregations AS (
    SELECT 
        match_id,
        COUNT(*) as total_statistics_count,
        COUNT(DISTINCT period) as periods_count,
        COUNT(DISTINCT group_name) as groups_count
    FROM flattened_stats
    GROUP BY match_id
),

-- Calculate period-level aggregations
period_aggregations AS (
    SELECT 
        match_id,
        period,
        COUNT(*) as statistics_in_period
    FROM flattened_stats
    GROUP BY match_id, period
),

-- Create the final transformed stats table
transformed_stats AS (
    SELECT
        -- Primary Key (composite)
        f.match_id,
        f.period,
        f.group_name,
        f.statistic_key,
        
        -- Core Match References
        f.tournament_id,
        f.season_id,
        
        -- Statistic Details
        f.statistic_name,
        f.statistics_type,
        f.value_type,
        f.render_type,
        f.compare_code,
        
        -- Values
        f.home_value_text,
        f.away_value_text,
        f.home_value_numeric,
        f.away_value_numeric,
        
        -- Value Comparisons
        CASE 
            WHEN f.home_value_numeric > f.away_value_numeric THEN 'HOME'
            WHEN f.away_value_numeric > f.home_value_numeric THEN 'AWAY'
            WHEN f.home_value_numeric = f.away_value_numeric THEN 'EQUAL'
            ELSE 'UNKNOWN'
        END as advantage,
        
        -- Percentage calculations
        CASE 
            WHEN f.home_value_numeric + f.away_value_numeric > 0 
            THEN ROUND(f.home_value_numeric * 100.0 / (f.home_value_numeric + f.away_value_numeric), 2)
            ELSE NULL 
        END as home_percentage,
        
        CASE 
            WHEN f.home_value_numeric + f.away_value_numeric > 0 
            THEN ROUND(f.away_value_numeric * 100.0 / (f.home_value_numeric + f.away_value_numeric), 2)
            ELSE NULL 
        END as away_percentage,
        
        -- Difference calculations
        f.home_value_numeric - f.away_value_numeric as value_difference,
        
        -- Group statistics
        ROW_NUMBER() OVER (PARTITION BY f.match_id, f.period, f.group_name ORDER BY f.statistic_key) as stat_order_in_group,
        COUNT(*) OVER (PARTITION BY f.match_id, f.period, f.group_name) as stats_count_in_group,
        
        -- Match-level aggregations
        ma.total_statistics_count,
        ma.periods_count,
        ma.groups_count,
        
        -- Period-level aggregations
        pa.statistics_in_period,
        
        -- Quality indicators
        CASE 
            WHEN f.home_value_numeric IS NOT NULL AND f.away_value_numeric IS NOT NULL THEN TRUE
            ELSE FALSE 
        END as has_numeric_values,
        
        CASE 
            WHEN f.statistic_key IS NOT NULL AND f.statistic_name IS NOT NULL THEN TRUE
            ELSE FALSE 
        END as is_complete_statistic,
        
        -- File metadata
        f.file_path,
        f.batch_id,
        
        -- Original data reference
        f.original_data,
        
        -- Audit columns
        f.ingestion_timestamp,
        NULL::TIMESTAMP WITH TIME ZONE as created_at,
        CURRENT_TIMESTAMP as updated_at
        
    FROM flattened_stats f
    LEFT JOIN match_aggregations ma ON f.match_id = ma.match_id
    LEFT JOIN period_aggregations pa ON f.match_id = pa.match_id AND f.period = pa.period
)

SELECT * FROM transformed_stats