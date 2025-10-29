
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: Verify all statistics have corresponding match records

select
    s.match_id,
    s.period
from "dwh"."silver"."staging_stats" s
left join "dwh"."silver"."staging_matches" m on s.match_id = m.match_id
where m.match_id is null
  
  
      
    ) dbt_internal_test