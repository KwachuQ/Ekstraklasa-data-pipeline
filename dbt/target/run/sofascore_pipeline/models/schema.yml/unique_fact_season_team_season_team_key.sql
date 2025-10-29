
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    season_team_key as unique_field,
    count(*) as n_records

from "dwh"."silver"."fact_season_team"
where season_team_key is not null
group by season_team_key
having count(*) > 1



  
  
      
    ) dbt_internal_test