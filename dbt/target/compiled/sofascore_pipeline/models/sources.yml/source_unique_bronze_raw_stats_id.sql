
    
    

select
    id as unique_field,
    count(*) as n_records

from "dwh"."bronze"."raw_stats"
where id is not null
group by id
having count(*) > 1


