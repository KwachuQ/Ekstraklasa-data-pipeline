
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: Verify statistics are within valid ranges
select
    match_id,
    team_id,
    period,
    'ballpossession' as stat_name,
    stat_ballpossession as stat_value
from "dwh"."silver"."fact_team_match"
where stat_ballpossession is not null
  and (stat_ballpossession < 0 or stat_ballpossession > 100)

union all

select
    match_id,
    team_id,
    period,
    'accuratepasses_pct' as stat_name,
    case when stat_passes > 0 
         then (stat_accuratepasses::numeric / stat_passes::numeric * 100)
         else 0 
    end as stat_value
from "dwh"."silver"."fact_team_match"
where stat_passes > 0
  and (stat_accuratepasses::numeric / stat_passes::numeric) > 1

union all

select
    match_id,
    team_id,
    period,
    'expectedgoals' as stat_name,
    stat_expectedgoals
from "dwh"."silver"."fact_team_match"
where stat_expectedgoals is not null
  and stat_expectedgoals < 0
  
  
      
    ) dbt_internal_test