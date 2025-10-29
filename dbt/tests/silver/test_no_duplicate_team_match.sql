-- Test: Verify no duplicate combinations of match_id + team_id + period
select
    match_id,
    team_id,
    period,
    count(*) as duplicate_count
from {{ ref('fact_team_match') }}
group by match_id, team_id, period
having count(*) > 1