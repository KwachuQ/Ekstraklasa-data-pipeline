
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select season_team_key
from "dwh"."silver"."fact_season_team"
where season_team_key is null



  
  
      
    ) dbt_internal_test