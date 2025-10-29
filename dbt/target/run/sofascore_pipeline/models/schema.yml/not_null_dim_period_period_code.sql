
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select period_code
from "dwh"."silver"."dim_period"
where period_code is null



  
  
      
    ) dbt_internal_test