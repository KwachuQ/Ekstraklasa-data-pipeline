
    
    

with all_values as (

    select
        has_statistics as value_field,
        count(*) as n_records

    from "dwh"."silver"."fact_match"
    group by has_statistics

)

select *
from all_values
where value_field not in (
    0,1
)


