{{
  config(
    materialized='incremental',
    alias='full_matches_data',
    unique_key='match_id',
    on_schema_change='fail',
    incremental_strategy='merge'
  )
}}

WITH source_data AS (
    SELECT * FROM {{ source('bronze', 'raw_matches') }}
    WHERE data IS NOT NULL
    {% if is_incremental() %}
        -- Ładuj tylko mecze po ostatniej dacie w docelowej tabeli
        AND TO_TIMESTAMP((data->>'startTimestamp')::BIGINT) > (
            SELECT COALESCE(MAX(start_timestamp), '1970-01-01'::timestamp)
            FROM {{ this }}
        )
    {% endif %}
),

transformed_matches AS (
    SELECT
        -- Core Match Information
        (data->>'id')::INTEGER as match_id,
        data->>'slug' as match_slug,
        data->>'customId' as custom_id,
        TO_TIMESTAMP((data->>'startTimestamp')::BIGINT) as start_timestamp,
        
        -- Match Status & Results
        (data->'status'->>'code')::INTEGER as status_code,
        data->'status'->>'type' as status_type,
        data->'status'->>'description' as status_description,
        (data->>'winnerCode')::INTEGER as winner_code,
        
        -- Scores
        (data->'homeScore'->>'current')::INTEGER as home_score_current,
        (data->'awayScore'->>'current')::INTEGER as away_score_current,
        (data->'homeScore'->>'period1')::INTEGER as home_score_period1,
        (data->'homeScore'->>'period2')::INTEGER as home_score_period2,
        (data->'awayScore'->>'period1')::INTEGER as away_score_period1,
        (data->'awayScore'->>'period2')::INTEGER as away_score_period2,
        
        -- Home Team
        (data->'homeTeam'->>'id')::INTEGER as home_team_id,
        data->'homeTeam'->>'name' as home_team_name,
        data->'homeTeam'->>'slug' as home_team_slug,
        data->'homeTeam'->>'shortName' as home_team_short_name,
        data->'homeTeam'->>'nameCode' as home_team_name_code,
        
        -- Away Team
        (data->'awayTeam'->>'id')::INTEGER as away_team_id,
        data->'awayTeam'->>'name' as away_team_name,
        data->'awayTeam'->>'slug' as away_team_slug,
        data->'awayTeam'->>'shortName' as away_team_short_name,
        data->'awayTeam'->>'nameCode' as away_team_name_code,
        
        -- Tournament & Season
        (data->'tournament'->>'id')::INTEGER as tournament_id,
        data->'tournament'->>'name' as tournament_name,
        data->'tournament'->>'slug' as tournament_slug,
        (data->'tournament'->'uniqueTournament'->>'id')::INTEGER as unique_tournament_id,
        (data->'season'->>'id')::INTEGER as season_id,
        data->'season'->>'name' as season_name,
        data->'season'->>'year' as season_year,
        
        -- Country/Category
        data->'tournament'->'category'->'country'->>'name' as country_name,
        data->'tournament'->'category'->'country'->>'alpha2' as country_alpha2,
        data->'tournament'->'category'->'country'->>'alpha3' as country_alpha3,
        
        -- Round & Time Details
        (data->'roundInfo'->>'round')::INTEGER as round_number,
        (data->'time'->>'injuryTime2')::INTEGER as injury_time_2,
        CASE 
            WHEN data->'time'->>'currentPeriodStartTimestamp' IS NOT NULL 
            THEN TO_TIMESTAMP((data->'time'->>'currentPeriodStartTimestamp')::BIGINT)
            ELSE NULL 
        END as current_period_start_timestamp,
        
        -- Match Flags
        (data->>'isEditor')::BOOLEAN as is_editor,
        (data->>'feedLocked')::BOOLEAN as feed_locked,
        (data->>'finalResultOnly')::BOOLEAN as final_result_only,
        (data->>'hasGlobalHighlights')::BOOLEAN as has_global_highlights,
        
        -- Metadata
        data->'_metadata'->>'source' as source,
        data->'_metadata'->>'batch_id' as batch_id,
        (data->'_metadata'->>'ingestion_timestamp')::TIMESTAMP as ingestion_timestamp,
        
        -- Audit columns - nie nadpisujemy created_at, tylko updated_at
        NULL::TIMESTAMP WITH TIME ZONE as created_at,
        CURRENT_TIMESTAMP as updated_at
        
    FROM source_data
)

SELECT * FROM transformed_matches