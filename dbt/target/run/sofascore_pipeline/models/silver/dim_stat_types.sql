
  
    

  create  table "dwh"."silver"."dim_stat_types__dbt_tmp"
  
  
    as
  
  (
    

select distinct
  statistic_key,
  statistic_name,
  group_name,
  statistics_type,
  value_type
from "dwh"."silver"."staging_stats"
where statistic_key is not null
order by group_name, statistic_key
  );
  