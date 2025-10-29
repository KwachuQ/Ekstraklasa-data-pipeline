
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_goals
from "dwh"."gold"."mart_team_attack"
where total_goals is null



  
  
      
    ) dbt_internal_test