
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        status_type as value_field,
        count(*) as n_records

    from "dwh"."silver"."fact_match"
    group by status_type

)

select *
from all_values
where value_field not in (
    'finished'
)



  
  
      
    ) dbt_internal_test