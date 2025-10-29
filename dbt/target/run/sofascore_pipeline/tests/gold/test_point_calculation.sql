
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: Verify points calculation (3 for win, 1 for draw, 0 for loss)

with calculated_points as (
    select
        season_id,
        season_name,
        season_year,
        home_team_id as team_id,
        home_team_name as team_name,
        case 
            when winner_code = 1 then 3
            when winner_code = 3 then 1
            when winner_code = 2 then 0
        end as points
    from "dwh"."silver"."fact_match"
    where status_type = 'finished'
    
    union all
    
    select
        season_id,
        season_name,
        season_year,
        away_team_id as team_id,
        away_team_name as team_name,
        case 
            when winner_code = 2 then 3
            when winner_code = 3 then 1
            when winner_code = 1 then 0
        end as points
    from "dwh"."silver"."fact_match"
    where status_type = 'finished'
),
aggregated_points as (
    select
        season_id,
        team_id,
        sum(points) as calc_points
    from calculated_points
    group by season_id, team_id
)
select
    mo.season_id,
    mo.team_id,
    mo.team_name,
    mo.total_points as points_in_mart,
    ap.calc_points,
    abs(mo.total_points - ap.calc_points) as difference
from "dwh"."gold"."mart_team_overview" mo
join aggregated_points ap on mo.season_id = ap.season_id and mo.team_id = ap.team_id
where mo.total_points != ap.calc_points
  
  
      
    ) dbt_internal_test