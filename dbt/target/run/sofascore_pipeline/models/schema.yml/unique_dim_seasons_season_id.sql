
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    season_id as unique_field,
    count(*) as n_records

from "dwh"."silver"."dim_seasons"
where season_id is not null
group by season_id
having count(*) > 1



  
  
      
    ) dbt_internal_test