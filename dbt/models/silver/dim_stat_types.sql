{{ config(materialized='table') }}

select distinct
  statistic_key,
  statistic_name,
  group_name,
  statistics_type,
  value_type
from {{ source('silver','staging_stats') }}
where statistic_key is not null
order by group_name, statistic_key