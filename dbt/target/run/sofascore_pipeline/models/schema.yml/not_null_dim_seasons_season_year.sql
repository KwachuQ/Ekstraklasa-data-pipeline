
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select season_year
from "dwh"."silver"."dim_seasons"
where season_year is null



  
  
      
    ) dbt_internal_test