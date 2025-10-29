
  
    

  create  table "dwh"."gold"."mart_team_possession__dbt_tmp"
  
  
    as
  
  (
    

with all_matches as (
    -- Wszystkie mecze
    select
        fm.season_id,
        fm.season_name,
        fm.season_year,
        fm.home_team_id as team_id,
        fm.home_team_name as team_name,
        fm.match_id
    from "dwh"."silver"."fact_match" fm
    where fm.status_type = 'finished'
    
    union all
    
    select
        fm.season_id,
        fm.season_name,
        fm.season_year,
        fm.away_team_id as team_id,
        fm.away_team_name as team_name,
        fm.match_id
    from "dwh"."silver"."fact_match" fm
    where fm.status_type = 'finished'
),
team_stats as (
    -- Statystyki posiadania (tylko gdzie dostępne)
    select
        ftm.team_id,
        ftm.team_name,
        fm.season_id,
        fm.season_name,
        fm.season_year,
        ftm.match_id,
        coalesce(ftm.stat_ballpossession, 0) as possession_pct,
        coalesce(ftm.stat_accuratepasses, 0) as accurate_passes,
        coalesce(ftm.stat_passes, 0) as total_passes,
        coalesce(ftm.stat_accuratelongballs, 0) as accurate_long_balls,
        coalesce(ftm.stat_accuratecross, 0) as accurate_crosses,
        coalesce(ftm.stat_finalthirdentries, 0) as final_third_entries,
        coalesce(ftm.stat_touchesinoppbox, 0) as touches_in_box,
        coalesce(ftm.stat_dispossessed, 0) as dispossessed,
        coalesce(ftm.stat_throwins, 0) as throw_ins,
        coalesce(ftm.stat_goalkicks, 0) as goal_kicks
    from "dwh"."silver"."fact_team_match" ftm
    join "dwh"."silver"."fact_match" fm on ftm.match_id = fm.match_id
    where ftm.period = 'ALL'
        and fm.status_type = 'finished'
),
combined as (
    select
        am.season_id,
        am.season_name,
        am.season_year,
        am.team_id,
        am.team_name,
        am.match_id,
        coalesce(ts.possession_pct, 0) as possession_pct,
        coalesce(ts.accurate_passes, 0) as accurate_passes,
        coalesce(ts.total_passes, 0) as total_passes,
        coalesce(ts.accurate_long_balls, 0) as accurate_long_balls,
        coalesce(ts.accurate_crosses, 0) as accurate_crosses,
        coalesce(ts.final_third_entries, 0) as final_third_entries,
        coalesce(ts.touches_in_box, 0) as touches_in_box,
        coalesce(ts.dispossessed, 0) as dispossessed,
        coalesce(ts.throw_ins, 0) as throw_ins,
        coalesce(ts.goal_kicks, 0) as goal_kicks
    from all_matches am
    left join team_stats ts 
        on am.match_id = ts.match_id 
        and am.team_id = ts.team_id
        and am.season_id = ts.season_id
)
select
    team_id,
    team_name,
    season_id,
    season_name,
    season_year,
    count(distinct match_id) as matches_played,
    round(avg(nullif(possession_pct, 0)), 1) as avg_possession_pct,
    sum(accurate_passes) as total_accurate_passes,
    sum(total_passes) as total_passes,
    round(avg(accurate_passes), 1) as accurate_passes_per_game,
    round(avg(total_passes), 1) as total_passes_per_game,
    round(sum(accurate_passes)::numeric / nullif(sum(total_passes), 0)::numeric * 100, 1) as pass_accuracy_pct,
    sum(accurate_long_balls) as total_accurate_long_balls,
    round(avg(accurate_long_balls), 2) as accurate_long_balls_per_game,
    sum(accurate_crosses) as total_accurate_crosses,
    round(avg(accurate_crosses), 2) as accurate_crosses_per_game,
    sum(final_third_entries) as total_final_third_entries,
    round(avg(final_third_entries), 2) as final_third_entries_per_game,
    sum(touches_in_box) as total_touches_in_box,
    round(avg(touches_in_box), 2) as touches_in_box_per_game,
    sum(dispossessed) as total_dispossessed,
    round(avg(dispossessed), 2) as dispossessed_per_game,
    sum(throw_ins) as total_throw_ins,
    round(avg(throw_ins), 2) as throw_ins_per_game,
    sum(goal_kicks) as total_goal_kicks,
    round(avg(goal_kicks), 2) as goal_kicks_per_game
from combined
group by team_id, team_name, season_id, season_name, season_year
order by season_year desc, avg_possession_pct desc
  );
  