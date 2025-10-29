
    
    

select
    season_team_key as unique_field,
    count(*) as n_records

from "dwh"."silver"."fact_season_team"
where season_team_key is not null
group by season_team_key
having count(*) > 1


