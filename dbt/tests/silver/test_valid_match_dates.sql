-- Test: Verify match dates are reasonable (not in future, not too old)

select
    match_id,
    match_date,
    season_year,
    start_timestamp
from {{ ref('fact_match') }}
where match_date > current_date + interval '7 days'  -- Allow some scheduling ahead
   or match_date < '2000-01-01'
   or extract(year from match_date) < 2000  -- Basic sanity check
   or extract(year from match_date) > extract(year from current_date) + 1