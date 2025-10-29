
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select match_id, team_id, period, possession_pct
from "dwh"."silver"."fact_team_match"
where possession_pct is not null
  and (possession_pct < 0 or possession_pct > 100)
  
  
      
    ) dbt_internal_test