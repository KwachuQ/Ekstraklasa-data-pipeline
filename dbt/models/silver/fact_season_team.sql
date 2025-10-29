{{ config(materialized='table') }}

with ftm as (
  select match_id, team_id, team_name
  from {{ ref('fact_team_match') }}
  where period = 'ALL'
),
m as (
  select
    match_id, season_id, season_name, season_year, tournament_id,
    home_team_id, away_team_id,
    home_score_current as home_score,
    away_score_current as away_score
  from {{ source('silver','staging_matches') }}
),
scored as (
  select
    m.season_id,
    m.season_name,
    m.season_year,
    m.tournament_id,
    f.team_id,
    f.team_name,
    case when f.team_id = m.home_team_id then m.home_score else m.away_score end as goals_for,
    case when f.team_id = m.home_team_id then m.away_score else m.home_score end as goals_against,
    case
      when (case when f.team_id = m.home_team_id then m.home_score else m.away_score end) >
           (case when f.team_id = m.home_team_id then m.away_score else m.home_score end) then 3
      when (case when f.team_id = m.home_team_id then m.home_score else m.away_score end) =
           (case when f.team_id = m.home_team_id then m.away_score else m.home_score end) then 1
      else 0
    end as points
  from ftm f
  join m using (match_id)
)
select
  {{ dbt_utils.generate_surrogate_key(['season_id','team_id']) }} as season_team_key,
  season_id,
  season_name,
  season_year,
  tournament_id,
  team_id,
  team_name,
  count(*) as matches_played,
  sum(goals_for) as goals_for,
  sum(goals_against) as goals_against,
  sum(points) as points
from scored
group by season_id, season_name, season_year, tournament_id, team_id, team_name