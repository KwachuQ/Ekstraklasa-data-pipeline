

select distinct
  season_id,
  season_name,
  season_year
from "dwh"."silver"."staging_matches"
where season_id is not null
order by season_id