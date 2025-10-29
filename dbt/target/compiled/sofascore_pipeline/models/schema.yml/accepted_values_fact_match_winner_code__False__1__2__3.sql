
    
    

with all_values as (

    select
        winner_code as value_field,
        count(*) as n_records

    from "dwh"."silver"."fact_match"
    group by winner_code

)

select *
from all_values
where value_field not in (
    1,2,3
)


