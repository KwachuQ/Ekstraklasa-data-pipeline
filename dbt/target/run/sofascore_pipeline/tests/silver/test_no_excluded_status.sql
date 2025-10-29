
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: Verify no matches with excluded status types exist
select
    match_id,
    status_type,
    status_description
from "dwh"."silver"."fact_match"
where status_type in ('postponed', 'canceled', 'cancelled', 'abandoned', 'interrupted', 'suspended')
  
  
      
    ) dbt_internal_test