
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select match_team_period_key
from "dwh"."silver"."fact_team_match"
where match_team_period_key is null



  
  
      
    ) dbt_internal_test