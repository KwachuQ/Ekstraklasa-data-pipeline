
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: Verify no duplicate combinations of match_id + team_id + period
select
    match_id,
    team_id,
    period,
    count(*) as duplicate_count
from "dwh"."silver"."fact_team_match"
group by match_id, team_id, period
having count(*) > 1
  
  
      
    ) dbt_internal_test