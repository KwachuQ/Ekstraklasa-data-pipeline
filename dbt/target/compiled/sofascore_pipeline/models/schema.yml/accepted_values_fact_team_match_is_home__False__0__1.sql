
    
    

with all_values as (

    select
        is_home as value_field,
        count(*) as n_records

    from "dwh"."silver"."fact_team_match"
    group by is_home

)

select *
from all_values
where value_field not in (
    0,1
)


