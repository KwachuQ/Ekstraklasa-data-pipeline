select ftm.match_id
from {{ ref('fact_team_match') }} ftm
left join {{ ref('fact_match') }} fm on fm.match_id = ftm.match_id
where fm.match_id is null