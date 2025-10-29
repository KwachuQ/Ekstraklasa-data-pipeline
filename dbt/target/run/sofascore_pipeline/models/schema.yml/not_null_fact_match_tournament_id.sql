
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select tournament_id
from "dwh"."silver"."fact_match"
where tournament_id is null



  
  
      
    ) dbt_internal_test