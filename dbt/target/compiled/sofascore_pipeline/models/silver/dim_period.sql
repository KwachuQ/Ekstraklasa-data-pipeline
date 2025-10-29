

select distinct
  period as period_code,
  case 
    when period = 'ALL' then 'Full match'
    when period = '1ST' then 'First half'
    when period = '2ND' then 'Second half'
    else period
  end as period_name
from "dwh"."silver"."staging_stats"
where period is not null
order by period