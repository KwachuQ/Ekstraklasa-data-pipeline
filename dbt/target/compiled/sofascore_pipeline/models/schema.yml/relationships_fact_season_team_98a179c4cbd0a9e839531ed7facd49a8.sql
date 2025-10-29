
    
    

with child as (
    select season_id as from_field
    from "dwh"."silver"."fact_season_team"
    where season_id is not null
),

parent as (
    select season_id as to_field
    from "dwh"."silver"."dim_seasons"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


