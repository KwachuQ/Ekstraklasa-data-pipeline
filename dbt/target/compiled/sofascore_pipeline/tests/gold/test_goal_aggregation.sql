-- Test: Verify goals aggregation in gold layer matches silver layer

with silver_goals as (
    select
        season_id,
        season_name,
        season_year,
        home_team_id as team_id,
        home_team_name as team_name,
        home_score as goals_for,
        away_score as goals_against
    from "dwh"."silver"."fact_match"
    where status_type = 'finished'
    
    union all
    
    select
        season_id,
        season_name,
        season_year,
        away_team_id as team_id,
        away_team_name as team_name,
        away_score as goals_for,
        home_score as goals_against
    from "dwh"."silver"."fact_match"
    where status_type = 'finished'
),
aggregated_goals as (
    select
        season_id,
        team_id,
        sum(goals_for) as total_goals_silver,
        sum(goals_against) as total_goals_against_silver
    from silver_goals
    group by season_id, team_id
)
select
    ta.season_id,
    ta.team_id,
    ta.team_name,
    ta.total_goals as goals_in_gold,
    ag.total_goals_silver,
    abs(ta.total_goals - ag.total_goals_silver) as difference
from "dwh"."gold"."mart_team_attack" ta
join aggregated_goals ag on ta.season_id = ag.season_id and ta.team_id = ag.team_id
where ta.total_goals != ag.total_goals_silver