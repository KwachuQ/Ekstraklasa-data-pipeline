
    
    

with child as (
    select match_id as from_field
    from "dwh"."silver"."fact_team_match"
    where match_id is not null
),

parent as (
    select match_id as to_field
    from "dwh"."silver"."fact_match"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


