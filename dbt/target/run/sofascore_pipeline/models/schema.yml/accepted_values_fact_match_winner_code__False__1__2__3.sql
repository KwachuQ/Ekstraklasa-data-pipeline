
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        winner_code as value_field,
        count(*) as n_records

    from "dwh"."silver"."fact_match"
    group by winner_code

)

select *
from all_values
where value_field not in (
    1,2,3
)



  
  
      
    ) dbt_internal_test