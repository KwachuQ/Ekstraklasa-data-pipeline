{{ config(materialized='table') }}

with upcoming_fixtures as (
    select
        match_id,
        match_slug,
        custom_id,
        start_timestamp,
        status_type,
        home_team_id,
        home_team_name,
        away_team_id,
        away_team_name,
        tournament_id_metadata as tournament_id,
        tournament_name,
        season_id_metadata as season_id,
        season_name,
        season_year,
        round_number,
        extraction_date,
        extracted_at,
        row_number() over (partition by match_id order by extracted_at desc) as rn
    from {{ source('silver', 'full_upcoming_fixtures') }}
),
deduplicated as (
    select
        match_id,
        match_slug,
        custom_id,
        start_timestamp,
        status_type,
        home_team_id,
        home_team_name,
        away_team_id,
        away_team_name,
        tournament_id,
        tournament_name,
        season_id,
        season_name,
        season_year,
        round_number,
        extraction_date,
        extracted_at
    from upcoming_fixtures
    where rn = 1  -- Keep only the most recent extraction for each match_id
)
select
    match_id,
    match_slug,
    coalesce(custom_id, 'N/A') as custom_id,
    start_timestamp,
    coalesce(status_type, 'notstarted') as status_type,
    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,
    tournament_id,
    tournament_name,
    season_id,
    season_name,
    coalesce(season_year, extract(year from start_timestamp)::text) as season_year,
    coalesce(round_number, 0) as round_number,
    extraction_date,
    extracted_at,
    current_timestamp as created_at
from deduplicated
where match_id is not null
    and start_timestamp is not null
    and home_team_id is not null
    and home_team_name is not null
    and away_team_id is not null
    and away_team_name is not null
    and tournament_id is not null
    and tournament_name is not null
    and season_id is not null
    and season_name is not null
order by start_timestamp asc