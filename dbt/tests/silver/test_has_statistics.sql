{{ config(severity='warn') }}

-- Test: Check matches that should have statistics but don't

select
    fm.match_id,
    fm.season_name,
    fm.home_team_name,
    fm.away_team_name,
    fm.match_date,
    count(distinct ftm.team_id) as teams_with_stats
from {{ ref('fact_match') }} fm
left join {{ ref('fact_team_match') }} ftm 
    on fm.match_id = ftm.match_id 
    and ftm.period = 'ALL'
where fm.status_type = 'finished'
group by fm.match_id, fm.season_name, fm.home_team_name, fm.away_team_name, fm.match_date
having count(distinct ftm.team_id) = 0  -- No statistics at all