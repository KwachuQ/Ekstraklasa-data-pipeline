
    
    

select
    match_id as unique_field,
    count(*) as n_records

from "dwh"."silver"."fact_match"
where match_id is not null
group by match_id
having count(*) > 1


