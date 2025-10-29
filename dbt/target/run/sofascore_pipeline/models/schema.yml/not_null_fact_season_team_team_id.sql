
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select team_id
from "dwh"."silver"."fact_season_team"
where team_id is null



  
  
      
    ) dbt_internal_test