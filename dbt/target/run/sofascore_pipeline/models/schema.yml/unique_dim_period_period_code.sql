
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    period_code as unique_field,
    count(*) as n_records

from "dwh"."silver"."dim_period"
where period_code is not null
group by period_code
having count(*) > 1



  
  
      
    ) dbt_internal_test