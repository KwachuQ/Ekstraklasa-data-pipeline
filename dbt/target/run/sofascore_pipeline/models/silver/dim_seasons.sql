
  
    

  create  table "dwh"."silver"."dim_seasons__dbt_tmp"
  
  
    as
  
  (
    

select distinct
  season_id,
  season_name,
  season_year
from "dwh"."silver"."staging_matches"
where season_id is not null
order by season_id
  );
  