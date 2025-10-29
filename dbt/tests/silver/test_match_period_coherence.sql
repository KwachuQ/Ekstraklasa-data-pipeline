select match_id, period, count(*) as rows_cnt
from {{ ref('fact_team_match') }}
group by match_id, period
having count(*) > 2