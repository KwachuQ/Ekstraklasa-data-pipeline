with calc as (
  select
    m.season_id,
    ftm.team_id,
    count(*) as matches_calc
  from "dwh"."silver"."fact_team_match" ftm
  join "dwh"."silver"."fact_match" m using (match_id)
  where ftm.period = 'ALL'
  group by 1,2
)
select fst.season_id, fst.team_id, fst.matches_played, c.matches_calc
from "dwh"."silver"."fact_season_team" fst
join calc c using (season_id, team_id)
where fst.matches_played <> c.matches_calc