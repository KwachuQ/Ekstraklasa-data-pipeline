{{ config(materialized='table') }}

{% set metrics = get_metric_keys() %}

with s as (
  select
    match_id,
    {{ normalize_period('period') }} as period,
    coalesce(nullif(statistic_key,''), statistic_name) as metric_key_raw,
    value_type,
    home_value_numeric, away_value_numeric,
    home_percentage,    away_percentage
  from {{ source('silver','staging_stats') }}
),
teams as (
  select match_id, home_team_id, home_team_name, away_team_id, away_team_name
  from {{ source('silver','staging_matches') }}
),
u as (
  select
    s.match_id,
    t.home_team_id as team_id,
    t.home_team_name as team_name,
    s.period,
    s.metric_key_raw,
    coalesce(
      case when s.value_type ilike '%percent%' and s.home_percentage is not null then s.home_percentage end,
      s.home_value_numeric
    )::numeric as value_num
  from s
  join teams t using (match_id)
  union all
  select
    s.match_id,
    t.away_team_id as team_id,
    t.away_team_name as team_name,
    s.period,
    s.metric_key_raw,
    coalesce(
      case when s.value_type ilike '%percent%' and s.away_percentage is not null then s.away_percentage end,
      s.away_value_numeric
    )::numeric as value_num
  from s
  join teams t using (match_id)
),
pivoted as (
  select
    match_id,
    team_id,
    team_name,
    period
    {% for m in metrics %}
    , max(case when metric_key_raw = '{{ m['raw_key'] | replace("'", "''") }}' then value_num end)::numeric as {{ m['col'] }}
    {% endfor %}
  from u
  where period in ('ALL','1ST','2ND')
  group by match_id, team_id, team_name, period
)
select
  {{ dbt_utils.generate_surrogate_key(['match_id','team_id','period']) }} as match_team_period_key,
  match_id,
  team_id,
  team_name,
  period
  {% for m in metrics %}
  , {{ m['col'] }}
  {% endfor %}
from pivoted