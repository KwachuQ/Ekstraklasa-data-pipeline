{{ config(materialized='table') }}

select distinct
  tournament_id,
  tournament_name,
  country_name
from {{ source('silver','staging_matches') }}
where tournament_id is not null
order by tournament_id