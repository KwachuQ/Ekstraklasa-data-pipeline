
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_home
from "dwh"."silver"."fact_team_match"
where is_home is null



  
  
      
    ) dbt_internal_test