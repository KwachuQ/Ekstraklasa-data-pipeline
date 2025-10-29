
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select matches_played
from "dwh"."silver"."fact_season_team"
where matches_played is null



  
  
      
    ) dbt_internal_test