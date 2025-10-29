

select distinct
  tournament_id,
  tournament_name,
  country_name
from "dwh"."silver"."staging_matches"
where tournament_id is not null
order by tournament_id