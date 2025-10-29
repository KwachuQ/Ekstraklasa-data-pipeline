
  
    

  create  table "dwh"."silver"."fact_match__dbt_tmp"
  
  
    as
  
  (
    

select
    match_id,
    match_slug,
    start_timestamp::date as match_date,
    start_timestamp,
    status_type,
    status_description,
    winner_code,
    home_score_current as home_score,
    away_score_current as away_score,
    home_score_period1,
    -- Uzupełnij brakujące period2 jako różnica: total - period1
    case 
        when home_score_period2 is null and home_score_period1 is not null 
        then home_score_current - home_score_period1
        else home_score_period2
    end as home_score_period2,
    away_score_period1,
    case 
        when away_score_period2 is null and away_score_period1 is not null 
        then away_score_current - away_score_period1
        else away_score_period2
    end as away_score_period2,
    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,
    tournament_id,
    tournament_name,
    season_id,
    season_name,
    season_year,
    country_name,
    case when status_type = 'finished' then 1 else 0 end as has_statistics,
    current_timestamp as loaded_at
from "dwh"."silver"."staging_matches"
where status_type = 'finished'
  and status_description != 'Canceled'
  and status_description != 'Cancelled'
  and status_description != 'Postponed'
  and status_description != 'Abandoned'
  );
  