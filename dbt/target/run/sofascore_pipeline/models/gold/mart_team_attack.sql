
  
    

  create  table "dwh"."gold"."mart_team_attack__dbt_tmp"
  
  
    as
  
  (
    

with all_matches as (
    -- Wszystkie mecze z golami (niezależnie od dostępności statystyk)
    select
        fm.season_id,
        fm.season_name,
        fm.season_year,
        fm.home_team_id as team_id,
        fm.home_team_name as team_name,
        fm.match_id,
        fm.home_score as goals_scored
    from "dwh"."silver"."fact_match" fm
    where fm.status_type = 'finished'
    
    union all
    
    select
        fm.season_id,
        fm.season_name,
        fm.season_year,
        fm.away_team_id as team_id,
        fm.away_team_name as team_name,
        fm.match_id,
        fm.away_score as goals_scored
    from "dwh"."silver"."fact_match" fm
    where fm.status_type = 'finished'
),
team_stats as (
    -- Statystyki szczegółowe (tylko dla meczów, które je mają)
    select
        ftm.team_id,
        ftm.team_name,
        fm.season_id,
        fm.season_name,
        fm.season_year,
        ftm.match_id,
        coalesce(ftm.stat_expectedgoals, 0) as xg,
        coalesce(ftm.stat_bigchancecreated, 0) as big_chances_created,
        coalesce(ftm.stat_bigchancemissed, 0) as big_chances_missed,
        coalesce(ftm.stat_bigchancescored, 0) as big_chances_scored,
        coalesce(ftm.stat_shotsongoal, 0) as shots_on_target,
        coalesce(ftm.stat_shotsoffgoal, 0) as shots_off_target,
        coalesce(ftm.stat_blockedscoringattempt, 0) as blocked_shots,
        coalesce(ftm.stat_totalshotsinsidebox, 0) as shots_inside_box,
        coalesce(ftm.stat_totalshotsoutsidebox, 0) as shots_outside_box,
        coalesce(ftm.stat_hitwoodwork, 0) as hit_woodwork,
        coalesce(ftm.stat_cornerkicks, 0) as corners,
        coalesce(ftm.stat_dribblespercentage, 0) as dribbles_success_pct,
        coalesce(ftm.stat_touchesinoppbox, 0) as touches_in_box
    from "dwh"."silver"."fact_team_match" ftm
    join "dwh"."silver"."fact_match" fm on ftm.match_id = fm.match_id
    where ftm.period = 'ALL'
        and fm.status_type = 'finished'
),
combined as (
    -- Połącz gole ze wszystkich meczów + statystyki gdzie dostępne
    select
        am.season_id,
        am.season_name,
        am.season_year,
        am.team_id,
        am.team_name,
        am.match_id,
        am.goals_scored,
        coalesce(ts.xg, 0) as xg,
        coalesce(ts.big_chances_created, 0) as big_chances_created,
        coalesce(ts.big_chances_missed, 0) as big_chances_missed,
        coalesce(ts.big_chances_scored, 0) as big_chances_scored,
        coalesce(ts.shots_on_target, 0) as shots_on_target,
        coalesce(ts.shots_off_target, 0) as shots_off_target,
        coalesce(ts.blocked_shots, 0) as blocked_shots,
        coalesce(ts.shots_on_target, 0) + coalesce(ts.shots_off_target, 0) + coalesce(ts.blocked_shots, 0) as total_shots,
        coalesce(ts.shots_inside_box, 0) as shots_inside_box,
        coalesce(ts.shots_outside_box, 0) as shots_outside_box,
        coalesce(ts.hit_woodwork, 0) as hit_woodwork,
        coalesce(ts.corners, 0) as corners,
        coalesce(ts.dribbles_success_pct, 0) as dribbles_success_pct,
        coalesce(ts.touches_in_box, 0) as touches_in_box
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
    sum(goals_scored) as total_goals,
    round(avg(goals_scored), 2) as goals_per_game,
    sum(xg) as total_xg,
    round(avg(xg), 2) as xg_per_game,
    round(sum(goals_scored) - sum(xg), 2) as xg_difference,
    round((sum(goals_scored) - sum(xg)) / nullif(count(distinct match_id), 0), 2) as xg_diff_per_game,
    sum(big_chances_created) as total_big_chances_created,
    round(avg(big_chances_created), 2) as big_chances_created_per_game,
    sum(big_chances_missed) as total_big_chances_missed,
    round(avg(big_chances_missed), 2) as big_chances_missed_per_game,
    sum(big_chances_scored) as total_big_chances_scored,
    round(avg(big_chances_scored), 2) as big_chances_scored_per_game,
    sum(shots_on_target) as total_shots_on_target,
    round(avg(shots_on_target), 2) as shots_on_target_per_game,
    sum(shots_off_target) as total_shots_off_target,
    round(avg(shots_off_target), 2) as shots_off_target_per_game,
    sum(blocked_shots) as total_blocked_shots,
    round(avg(blocked_shots), 2) as blocked_shots_per_game,
    sum(total_shots) as total_shots,
    round(avg(total_shots), 2) as shots_per_game,
    sum(shots_inside_box) as total_shots_inside_box,
    round(avg(shots_inside_box), 2) as shots_inside_box_per_game,
    sum(shots_outside_box) as total_shots_outside_box,
    round(avg(shots_outside_box), 2) as shots_outside_box_per_game,
    sum(hit_woodwork) as total_hit_woodwork,
    sum(corners) as total_corners,
    round(avg(corners), 2) as corners_per_game,
    round(avg(nullif(dribbles_success_pct, 0)), 1) as avg_dribbles_success_pct,
    sum(touches_in_box) as total_touches_in_box,
    round(avg(touches_in_box), 2) as touches_in_box_per_game
from combined
group by team_id, team_name, season_id, season_name, season_year
order by season_year desc, xg_per_game desc
  );
  