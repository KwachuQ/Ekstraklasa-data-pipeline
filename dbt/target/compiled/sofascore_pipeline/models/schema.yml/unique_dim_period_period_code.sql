
    
    

select
    period_code as unique_field,
    count(*) as n_records

from "dwh"."silver"."dim_period"
where period_code is not null
group by period_code
having count(*) > 1


