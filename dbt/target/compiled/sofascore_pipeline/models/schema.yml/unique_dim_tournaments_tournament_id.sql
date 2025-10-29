
    
    

select
    tournament_id as unique_field,
    count(*) as n_records

from "dwh"."silver"."dim_tournaments"
where tournament_id is not null
group by tournament_id
having count(*) > 1


