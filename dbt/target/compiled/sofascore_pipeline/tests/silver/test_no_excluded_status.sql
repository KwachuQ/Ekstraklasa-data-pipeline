-- Test: Verify no matches with excluded status types exist
select
    match_id,
    status_type,
    status_description
from "dwh"."silver"."fact_match"
where status_type in ('postponed', 'canceled', 'cancelled', 'abandoned', 'interrupted', 'suspended')