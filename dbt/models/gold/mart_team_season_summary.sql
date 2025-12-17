{{ config(materialized='table') }}

with match_data as (
    select
        fm.season_id,
        fm.season_name,
        fm.season_year,
        fm.match_id,
        fm.match_date,
        fm.home_team_id,
        fm.home_team_name,
        fm.away_team_id,
        fm.away_team_name,
        fm.home_score,
        fm.away_score,
        fm.home_score_period1,
        fm.away_score_period1,
        (fm.home_score - fm.home_score_period1) as home_score_period2,
        (fm.away_score - fm.away_score_period1) as away_score_period2,
        fm.winner_code,
        case when fm.home_score > 0 and fm.away_score > 0 then 1 else 0 end as btts,
        case when fm.away_score = 0 then 1 else 0 end as home_clean_sheet,
        case when fm.home_score = 0 then 1 else 0 end as away_clean_sheet,
        coalesce(ftm_home.stat_expectedgoals, 0) as home_xg,
        coalesce(ftm_away.stat_expectedgoals, 0) as away_xg
    from {{ ref('fact_match') }} fm
    left join {{ ref('fact_team_match') }} ftm_home 
        on fm.match_id = ftm_home.match_id 
        and fm.home_team_id = ftm_home.team_id 
        and ftm_home.period = 'ALL'
    left join {{ ref('fact_team_match') }} ftm_away 
        on fm.match_id = ftm_away.match_id 
        and fm.away_team_id = ftm_away.team_id 
        and ftm_away.period = 'ALL'
    where fm.status_type = 'finished'
),
home_halftime_stats as (
    select
        home_team_id as team_id,
        season_id,
        
        -- Halftime scoring stats
        round(sum(case when home_score_period1 > 0 then 1 else 0 end)::numeric / nullif(count(*), 0) * 100, 1) as home_scored_1h_pct,
        round(sum(case when home_score_period2 > 0 then 1 else 0 end)::numeric / nullif(count(*), 0) * 100, 1) as home_scored_2h_pct,
        round(sum(case when home_score_period1 > 0 and home_score_period2 > 0 then 1 else 0 end)::numeric / nullif(count(*), 0) * 100, 1) as home_scored_both_halves_pct,
        round(avg(home_score_period1), 2) as home_avg_goals_1h,
        round(avg(home_score_period2), 2) as home_avg_goals_2h,
        
        -- Halftime conceding stats
        round(sum(case when away_score_period1 = 0 then 1 else 0 end)::numeric / nullif(count(*), 0) * 100, 1) as home_clean_sheet_1h_pct,
        round(sum(case when away_score_period2 = 0 then 1 else 0 end)::numeric / nullif(count(*), 0) * 100, 1) as home_clean_sheet_2h_pct,
        round(avg(away_score_period1), 2) as home_avg_conceded_1h,
        round(avg(away_score_period2), 2) as home_avg_conceded_2h
    from match_data
    group by home_team_id, season_id
),
away_halftime_stats as (
    select
        away_team_id as team_id,
        season_id,
        
        -- Halftime scoring stats
        round(sum(case when away_score_period1 > 0 then 1 else 0 end)::numeric / nullif(count(*), 0) * 100, 1) as away_scored_1h_pct,
        round(sum(case when away_score_period2 > 0 then 1 else 0 end)::numeric / nullif(count(*), 0) * 100, 1) as away_scored_2h_pct,
        round(sum(case when away_score_period1 > 0 and away_score_period2 > 0 then 1 else 0 end)::numeric / nullif(count(*), 0) * 100, 1) as away_scored_both_halves_pct,
        round(avg(away_score_period1), 2) as away_avg_goals_1h,
        round(avg(away_score_period2), 2) as away_avg_goals_2h,
        
        -- Halftime conceding stats
        round(sum(case when home_score_period1 = 0 then 1 else 0 end)::numeric / nullif(count(*), 0) * 100, 1) as away_clean_sheet_1h_pct,
        round(sum(case when home_score_period2 = 0 then 1 else 0 end)::numeric / nullif(count(*), 0) * 100, 1) as away_clean_sheet_2h_pct,
        round(avg(home_score_period1), 2) as away_avg_conceded_1h,
        round(avg(home_score_period2), 2) as away_avg_conceded_2h
    from match_data
    group by away_team_id, season_id
),
home_away_record as (
    select
        home_team_id as team_id,
        season_id,
        count(*) as home_matches,
        sum(case when winner_code = 1 then 1 else 0 end) as home_wins,
        sum(case when winner_code = 3 then 1 else 0 end) as home_draws,
        sum(case when winner_code = 2 then 1 else 0 end) as home_losses
    from match_data
    group by home_team_id, season_id
),
away_record as (
    select
        away_team_id as team_id,
        season_id,
        count(*) as away_matches,
        sum(case when winner_code = 2 then 1 else 0 end) as away_wins,
        sum(case when winner_code = 3 then 1 else 0 end) as away_draws,
        sum(case when winner_code = 1 then 1 else 0 end) as away_losses
    from match_data
    group by away_team_id, season_id
)
select
    ov.season_id,
    ov.season_name,
    ov.season_year,
    ov.team_id,
    ov.team_name,
    
    -- Overall stats
    ov.matches_played,
    ov.wins,
    ov.draws,
    ov.losses,
    ov.total_points,
    ov.points_per_game,
    ov.goals_for,
    ov.goals_against,
    ov.goal_difference,
    ov.goals_per_game,
    ov.goals_conceded_per_game,
    ov.clean_sheets,
    ov.clean_sheet_percentage,
    
    -- Home/Away record
    coalesce(har.home_wins, 0) as home_wins,
    coalesce(har.home_draws, 0) as home_draws,
    coalesce(har.home_losses, 0) as home_losses,
    coalesce(awr.away_wins, 0) as away_wins,
    coalesce(awr.away_draws, 0) as away_draws,
    coalesce(awr.away_losses, 0) as away_losses,
    
    -- Attack stats
    att.total_xg,
    att.xg_per_game,
    att.xg_difference,
    att.xg_diff_per_game,
    att.total_big_chances_created,
    att.big_chances_created_per_game,
    att.total_big_chances_missed,
    att.big_chances_missed_per_game,
    att.total_shots,
    att.shots_per_game,
    att.total_shots_on_target,
    att.shots_on_target_per_game,
    att.total_shots_inside_box,
    att.shots_inside_box_per_game,
    att.total_shots_outside_box,
    att.shots_outside_box_per_game,
    att.total_corners,
    att.corners_per_game,
    att.avg_dribbles_success_pct,
    att.total_hit_woodwork,
    
    -- Defense stats
    def.total_saves,
    def.saves_per_game,
    def.total_tackles,
    def.tackles_per_game,
    def.avg_tackles_won_pct,
    def.total_interceptions,
    def.interceptions_per_game,
    def.total_clearances,
    def.clearances_per_game,
    def.total_blocked_shots,
    def.blocked_shots_per_game,
    def.total_ball_recoveries,
    def.ball_recoveries_per_game,
    def.avg_aerial_duels_pct,
    def.avg_ground_duels_pct,
    def.avg_duels_won_pct,
    def.total_errors_lead_to_goal,
    def.total_errors_lead_to_shot,
    
    -- Possession stats
    pos.avg_possession_pct,
    pos.total_accurate_passes,
    pos.total_passes,
    pos.pass_accuracy_pct,
    pos.accurate_passes_per_game,
    pos.total_passes_per_game,
    pos.total_accurate_long_balls,
    pos.accurate_long_balls_per_game,
    pos.total_accurate_crosses,
    pos.accurate_crosses_per_game,
    pos.total_final_third_entries,
    pos.final_third_entries_per_game,
    pos.total_touches_in_box as total_touches_in_box_possession,
    pos.touches_in_box_per_game as touches_in_box_per_game_possession,
    pos.total_dispossessed,
    pos.dispossessed_per_game,
    
    -- Discipline stats
    dis.total_yellow_cards,
    dis.yellow_cards_per_game,
    dis.total_red_cards,
    dis.total_fouls,
    dis.fouls_per_game,
    dis.total_offsides,
    dis.offsides_per_game,
    dis.total_free_kicks,
    dis.free_kicks_per_game,
    
    -- Form stats
    frm.last_5_results,
    frm.points_last_5,
    frm.wins_last_5,
    frm.draws_last_5,
    frm.losses_last_5,
    frm.goals_for_last_5,
    frm.goals_against_last_5,
    frm.halftime_leading_count,
    frm.halftime_leading_wins,
    frm.halftime_leading_win_pct,
    frm.conceded_first_count,
    frm.points_after_conceding_first,
    frm.points_pct_after_conceding_first,
    
    -- Home halftime stats
    coalesce(hhs.home_scored_1h_pct, 0) as home_scored_1h_pct,
    coalesce(hhs.home_scored_2h_pct, 0) as home_scored_2h_pct,
    coalesce(hhs.home_scored_both_halves_pct, 0) as home_scored_both_halves_pct,
    coalesce(hhs.home_avg_goals_1h, 0) as home_avg_goals_1h,
    coalesce(hhs.home_avg_goals_2h, 0) as home_avg_goals_2h,
    coalesce(hhs.home_clean_sheet_1h_pct, 0) as home_clean_sheet_1h_pct,
    coalesce(hhs.home_clean_sheet_2h_pct, 0) as home_clean_sheet_2h_pct,
    coalesce(hhs.home_avg_conceded_1h, 0) as home_avg_conceded_1h,
    coalesce(hhs.home_avg_conceded_2h, 0) as home_avg_conceded_2h,
    
    -- Away halftime stats
    coalesce(ahs.away_scored_1h_pct, 0) as away_scored_1h_pct,
    coalesce(ahs.away_scored_2h_pct, 0) as away_scored_2h_pct,
    coalesce(ahs.away_scored_both_halves_pct, 0) as away_scored_both_halves_pct,
    coalesce(ahs.away_avg_goals_1h, 0) as away_avg_goals_1h,
    coalesce(ahs.away_avg_goals_2h, 0) as away_avg_goals_2h,
    coalesce(ahs.away_clean_sheet_1h_pct, 0) as away_clean_sheet_1h_pct,
    coalesce(ahs.away_clean_sheet_2h_pct, 0) as away_clean_sheet_2h_pct,
    coalesce(ahs.away_avg_conceded_1h, 0) as away_avg_conceded_1h,
    coalesce(ahs.away_avg_conceded_2h, 0) as away_avg_conceded_2h

from {{ ref('mart_team_overview') }} ov
left join {{ ref('mart_team_attack') }} att 
    on ov.team_id = att.team_id 
    and ov.season_id = att.season_id
left join {{ ref('mart_team_defense') }} def 
    on ov.team_id = def.team_id 
    and ov.season_id = def.season_id
left join {{ ref('mart_team_possession') }} pos 
    on ov.team_id = pos.team_id 
    and ov.season_id = pos.season_id
left join {{ ref('mart_team_discipline') }} dis 
    on ov.team_id = dis.team_id 
    and ov.season_id = dis.season_id
left join {{ ref('mart_team_form') }} frm 
    on ov.team_id = frm.team_id 
    and ov.season_id = frm.season_id
left join home_halftime_stats hhs
    on ov.team_id = hhs.team_id
    and ov.season_id = hhs.season_id
left join away_halftime_stats ahs
    on ov.team_id = ahs.team_id
    and ov.season_id = ahs.season_id
left join home_away_record har
    on ov.team_id = har.team_id
    and ov.season_id = har.season_id
left join away_record awr
    on ov.team_id = awr.team_id
    and ov.season_id = awr.season_id

order by ov.season_year desc, ov.total_points desc