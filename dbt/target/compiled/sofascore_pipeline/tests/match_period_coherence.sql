select match_id, period, count(*) as rows_cnt
from "dwh"."silver"."fact_team_match"
group by match_id, period
having count(*) > 2