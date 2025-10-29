
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: Verify each match has exactly 2 teams (home and away)
select
    match_id,
    count(distinct team_id) as team_count
from "dwh"."silver"."fact_team_match"
where period = 'ALL'
group by match_id
having count(distinct team_id) != 2
  
  
      
    ) dbt_internal_test