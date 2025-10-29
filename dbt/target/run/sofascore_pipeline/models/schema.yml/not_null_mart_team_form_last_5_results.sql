
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select last_5_results
from "dwh"."gold"."mart_team_form"
where last_5_results is null



  
  
      
    ) dbt_internal_test