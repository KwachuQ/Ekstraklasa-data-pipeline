
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select home_team_id
from "dwh"."silver"."fact_match"
where home_team_id is null



  
  
      
    ) dbt_internal_test