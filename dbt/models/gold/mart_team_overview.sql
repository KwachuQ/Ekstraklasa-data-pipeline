{{ config(materialized='table') }}

with matches as (
    select
        season_id,
        season_name,
        season_year,
        home_team_id as team_id,
        home_team_name as team_name,
        match_id,
        match_date,
        case 
            when winner_code = 1 then 3
            when winner_code = 3 then 1
            when winner_code = 2 then 0
        end as points,
        home_score as goals_for,
        away_score as goals_against,
        case when winner_code = 1 then 1 else 0 end as wins,
        case when winner_code = 3 then 1 else 0 end as draws,
        case when winner_code = 2 then 1 else 0 end as losses,
        case when away_score = 0 then 1 else 0 end as clean_sheets
    from {{ ref('fact_match') }}
    where status_type = 'finished'
    
    union all
    
    select
        season_id,
        season_name,
        season_year,
        away_team_id as team_id,
        away_team_name as team_name,
        match_id,
        match_date,
        case 
            when winner_code = 2 then 3
            when winner_code = 3 then 1
            when winner_code = 1 then 0
        end as points,
        away_score as goals_for,
        home_score as goals_against,
        case when winner_code = 2 then 1 else 0 end as wins,
        case when winner_code = 3 then 1 else 0 end as draws,
        case when winner_code = 1 then 1 else 0 end as losses,
        case when home_score = 0 then 1 else 0 end as clean_sheets
    from {{ ref('fact_match') }}
    where status_type = 'finished'
)
select
    season_id,
    season_name,
    season_year,
    team_id,
    team_name,
    count(*) as matches_played,
    sum(wins) as wins,
    sum(draws) as draws,
    sum(losses) as losses,
    sum(points) as total_points,
    round(sum(points)::numeric / count(*)::numeric, 2) as points_per_game,
    sum(goals_for) as goals_for,
    sum(goals_against) as goals_against,
    sum(goals_for) - sum(goals_against) as goal_difference,
    round(sum(goals_for)::numeric / count(*)::numeric, 2) as goals_per_game,
    round(sum(goals_against)::numeric / count(*)::numeric, 2) as goals_conceded_per_game,
    sum(clean_sheets) as clean_sheets,
    round(sum(clean_sheets)::numeric / count(*)::numeric * 100, 1) as clean_sheet_percentage
from matches
group by season_id, season_name, season_year, team_id, team_name
order by season_year desc, total_points desc