
    
    

with child as (
    select tournament_id as from_field
    from "dwh"."silver"."fact_match"
    where tournament_id is not null
),

parent as (
    select tournament_id as to_field
    from "dwh"."silver"."dim_tournaments"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


