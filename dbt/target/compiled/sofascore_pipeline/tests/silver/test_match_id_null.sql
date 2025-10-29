select ftm.match_id
from "dwh"."silver"."fact_team_match" ftm
left join "dwh"."silver"."fact_match" fm on fm.match_id = ftm.match_id
where fm.match_id is null