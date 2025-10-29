





with validation_errors as (

    select
        match_id, team_id, period
    from "dwh"."silver"."fact_team_match"
    group by match_id, team_id, period
    having count(*) > 1

)

select *
from validation_errors


