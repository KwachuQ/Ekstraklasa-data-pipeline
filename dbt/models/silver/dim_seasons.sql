{{ config(materialized='table') }}

select distinct
  season_id,
  season_name,
  season_year
from {{ source('silver','staging_matches') }}
where season_id is not null
order by season_id