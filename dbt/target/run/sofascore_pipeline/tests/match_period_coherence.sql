
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select match_id, period, count(*) as rows_cnt
from "dwh"."silver"."fact_team_match"
group by match_id, period
having count(*) > 2
  
  
      
    ) dbt_internal_test