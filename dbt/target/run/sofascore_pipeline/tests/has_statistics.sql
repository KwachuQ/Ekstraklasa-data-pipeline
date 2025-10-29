
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select m.match_id
from "dwh"."silver"."fact_match" m
left join (select distinct match_id from "dwh"."silver"."staging_stats") s using (match_id)
where (m.has_statistics = 1 and s.match_id is null)
   or (m.has_statistics = 0 and s.match_id is not null)
  
  
      
    ) dbt_internal_test