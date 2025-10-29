
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select goals_total
from "dwh"."gold"."mart_team_attack"
where goals_total is null



  
  
      
    ) dbt_internal_test