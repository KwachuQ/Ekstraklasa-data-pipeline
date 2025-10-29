
  
    

  create  table "dwh"."silver"."stg_matches__dbt_tmp"
  
  
    as
  
  (
    

with raw_matches as (
    -- Zakładając że dane są w bronze.raw_matches jako JSONB
    select * from bronze.raw_matches  
    where (data->'tournament'->'uniqueTournament'->>'id')::integer = 202  -- Właściwe pole dla Ekstraklasy
),

parsed_matches as (
    select
        -- Primary identifiers
        (data->>'id')::integer as match_id,
        (data->'tournament'->>'id')::integer as tournament_id,  -- Season-specific tournament ID (64)
        (data->'tournament'->'uniqueTournament'->>'id')::integer as unique_tournament_id,  -- Właściwe ID (202)
        (data->'season'->>'id')::integer as season_id,
        data->>'customId' as custom_id,
        data->>'slug' as match_slug,
        
        -- Timestamps and dates
        (data->>'startTimestamp')::bigint as start_timestamp_unix,
        to_timestamp((data->>'startTimestamp')::bigint) as match_start_timestamp,
        date(to_timestamp((data->>'startTimestamp')::bigint)) as match_date,
        extract(year from to_timestamp((data->>'startTimestamp')::bigint)) as match_year,
        extract(month from to_timestamp((data->>'startTimestamp')::bigint)) as match_month,
        
        -- Tournament information
        data->'tournament'->>'name' as tournament_name,
        data->'tournament'->>'slug' as tournament_slug,
        data->'tournament'->'uniqueTournament'->>'name' as unique_tournament_name,
        data->'tournament'->'uniqueTournament'->>'slug' as unique_tournament_slug,
        (data->'tournament'->>'priority')::integer as tournament_priority,
        
        -- Season information
        data->'season'->>'name' as season_name,
        data->'season'->>'year' as season_year,
        
        -- Round information
        (data->'roundInfo'->>'round')::integer as round_number,
        
        -- Teams - Home
        (data->'homeTeam'->>'id')::integer as home_team_id,
        data->'homeTeam'->>'name' as home_team_name,
        data->'homeTeam'->>'slug' as home_team_slug,
        data->'homeTeam'->>'shortName' as home_team_short_name,
        data->'homeTeam'->>'nameCode' as home_team_code,
        data->'homeTeam'->'country'->>'name' as home_team_country,
        data->'homeTeam'->'country'->>'alpha2' as home_team_country_code,
        
        -- Teams - Away  
        (data->'awayTeam'->>'id')::integer as away_team_id,
        data->'awayTeam'->>'name' as away_team_name,
        data->'awayTeam'->>'slug' as away_team_slug,
        data->'awayTeam'->>'shortName' as away_team_short_name,
        data->'awayTeam'->>'nameCode' as away_team_code,
        data->'awayTeam'->'country'->>'name' as away_team_country,
        data->'awayTeam'->'country'->>'alpha2' as away_team_country_code,
        
        -- Final Scores
        case 
            when data->'status'->>'type' = 'finished' then
                (data->'homeScore'->>'current')::integer
            else null
        end as home_score_final,
        
        case 
            when data->'status'->>'type' = 'finished' then
                (data->'awayScore'->>'current')::integer
            else null
        end as away_score_final,
        
        -- 🎯 Half-time Scores (pierwsza połowa)
        case 
            when data->'status'->>'type' = 'finished' and data->'homeScore'->>'period1' is not null then
                (data->'homeScore'->>'period1')::integer
            else null
        end as home_score_period1,
        
        case 
            when data->'status'->>'type' = 'finished' and data->'awayScore'->>'period1' is not null then
                (data->'awayScore'->>'period1')::integer
            else null
        end as away_score_period1,
        
        -- 🎯 Second Half Scores (druga połowa)
        case 
            when data->'status'->>'type' = 'finished' and data->'homeScore'->>'period2' is not null then
                (data->'homeScore'->>'period2')::integer
            else null
        end as home_score_period2,
        
        case 
            when data->'status'->>'type' = 'finished' and data->'awayScore'->>'period2' is not null then
                (data->'awayScore'->>'period2')::integer
            else null
        end as away_score_period2,
        
        -- 🎯 Injury Time (doliczone czasy)
        case 
            when data->'time'->>'injuryTime1' is not null then
                (data->'time'->>'injuryTime1')::integer
            else null
        end as injury_time_period1,
        
        case 
            when data->'time'->>'injuryTime2' is not null then
                (data->'time'->>'injuryTime2')::integer
            else null
        end as injury_time_period2,
        
        -- Match Status
        data->'status'->>'type' as match_status,
        data->'status'->>'description' as status_description,
        (data->'status'->>'code')::integer as status_code,
        
        -- Winner information
        (data->>'winnerCode')::integer as winner_code,
        
        -- Additional match info
        (data->>'hasGlobalHighlights')::boolean as has_highlights,
        (data->>'finalResultOnly')::boolean as final_result_only,
        (data->>'feedLocked')::boolean as feed_locked,
        (data->>'coverage')::integer as coverage_level,
        
        -- ETL metadata
        data->'_metadata'->>'batch_id' as batch_id,
        (data->'_metadata'->>'ingestion_timestamp')::timestamp as ingestion_timestamp,
        data->'_metadata'->>'source' as data_source,
        current_timestamp as dbt_loaded_at  -- ✅ Poprawione: bez nawiasów
        
    from raw_matches
    where data->>'id' is not null
),

-- Derived fields and calculations
enriched_matches as (
    select *,
        
        -- 🎯 Match Results
        case 
            when match_status = 'finished' and home_score_final > away_score_final then 'home_win'
            when match_status = 'finished' and home_score_final < away_score_final then 'away_win'
            when match_status = 'finished' and home_score_final = away_score_final then 'draw'
            else null
        end as match_result,
        
        -- 🎯 Half-time Result
        case 
            when home_score_period1 is not null and away_score_period1 is not null then
                case 
                    when home_score_period1 > away_score_period1 then 'home_win'
                    when home_score_period1 < away_score_period1 then 'away_win'
                    when home_score_period1 = away_score_period1 then 'draw'
                end
            else null
        end as halftime_result,
        
        -- 🎯 Goals in each period
        home_score_period1 as home_goals_period1,
        away_score_period1 as away_goals_period1,
        
        case 
            when home_score_period2 is not null and home_score_period1 is not null then
                home_score_period2 - home_score_period1
            else home_score_period2
        end as home_goals_period2_only,
        
        case 
            when away_score_period2 is not null and away_score_period1 is not null then
                away_score_period2 - away_score_period1  
            else away_score_period2
        end as away_goals_period2_only,
        
        -- 🎯 Total goals and goal difference
        case 
            when home_score_final is not null and away_score_final is not null then
                home_score_final + away_score_final
            else null
        end as total_goals,
        
        case 
            when home_score_final is not null and away_score_final is not null then
                home_score_final - away_score_final
            else null
        end as goal_difference,
        
        -- 🎯 Total injury time
        case 
            when injury_time_period1 is not null and injury_time_period2 is not null then
                injury_time_period1 + injury_time_period2
            when injury_time_period1 is not null then injury_time_period1
            when injury_time_period2 is not null then injury_time_period2
            else null
        end as total_injury_time,
        
        -- Match categories for analysis
        case 
            when home_score_final is not null and away_score_final is not null then
                case 
                    when home_score_final + away_score_final = 0 then 'no_goals'
                    when home_score_final + away_score_final between 1 and 2 then 'low_scoring'
                    when home_score_final + away_score_final between 3 and 4 then 'medium_scoring'
                    when home_score_final + away_score_final >= 5 then 'high_scoring'
                end
            else null
        end as scoring_category,
        
        -- Natural and surrogate keys
        concat(unique_tournament_id, '_', season_id, '_', match_id) as natural_key,
        md5(cast(coalesce(cast(match_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(unique_tournament_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(season_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as match_sk
        
    from parsed_matches
    where match_id is not null
      and unique_tournament_id = 202  -- Używamy właściwego pola
      and season_id is not null
      and home_team_id is not null 
      and away_team_id is not null
      and home_team_id != away_team_id
)

select * from enriched_matches
  );
  