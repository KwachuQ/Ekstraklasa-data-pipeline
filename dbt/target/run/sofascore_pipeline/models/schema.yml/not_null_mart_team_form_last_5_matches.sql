
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select last_5_matches
from "dwh"."gold"."mart_team_form"
where last_5_matches is null



  
  
      
    ) dbt_internal_test