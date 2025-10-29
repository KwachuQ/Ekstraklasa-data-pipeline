
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select has_statistics
from "dwh"."silver"."fact_match"
where has_statistics is null



  
  
      
    ) dbt_internal_test