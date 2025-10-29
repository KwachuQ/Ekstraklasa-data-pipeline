
    
    

with all_values as (

    select
        period_code as value_field,
        count(*) as n_records

    from "dwh"."silver"."dim_period"
    group by period_code

)

select *
from all_values
where value_field not in (
    'ALL','1ST','2ND'
)


