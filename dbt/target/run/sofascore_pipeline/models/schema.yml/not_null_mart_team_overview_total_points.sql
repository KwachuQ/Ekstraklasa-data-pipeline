
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_points
from "dwh"."gold"."mart_team_overview"
where total_points is null



  
  
      
    ) dbt_internal_test