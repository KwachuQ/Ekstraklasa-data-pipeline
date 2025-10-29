-- Test: Verify fact_season_team aggregations match fact_team_match counts
with match_counts as (
    select
        m.season_id,
        ftm.team_id,
        count(distinct ftm.match_id) as matches_from_fact
    from "dwh"."silver"."fact_team_match" ftm
    join "dwh"."silver"."fact_match" m on ftm.match_id = m.match_id
    where ftm.period = 'ALL'
    group by m.season_id, ftm.team_id
)
select
    fst.season_id,
    fst.team_id,
    fst.matches_played as matches_in_season_table,
    mc.matches_from_fact,
    abs(fst.matches_played - mc.matches_from_fact) as difference
from "dwh"."silver"."fact_season_team" fst
join match_counts mc on fst.season_id = mc.season_id and fst.team_id = mc.team_id
where fst.matches_played != mc.matches_from_fact