
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        period as value_field,
        count(*) as n_records

    from "dwh"."silver"."fact_team_match"
    group by period

)

select *
from all_values
where value_field not in (
    'ALL','1ST','2ND'
)



  
  
      
    ) dbt_internal_test