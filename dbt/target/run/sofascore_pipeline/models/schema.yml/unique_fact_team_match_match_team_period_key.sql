
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    match_team_period_key as unique_field,
    count(*) as n_records

from "dwh"."silver"."fact_team_match"
where match_team_period_key is not null
group by match_team_period_key
having count(*) > 1



  
  
      
    ) dbt_internal_test