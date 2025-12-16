{{ config(materialized='table') }}

with league_match_stats as (
    -- Basic match statistics from fact_match - aggregate ALL matches for a season (including playoffs/relegation)
    select
        fm.season_id,
        fm.season_name,
        fm.season_year,
        -- Use the main tournament_id (you might need to adjust this logic based on your data)
        min(fm.tournament_id) as tournament_id,
        min(fm.tournament_name) as tournament_name,
        count(distinct fm.match_id) as total_matches,
        
        -- Goals
        round(avg(fm.home_score + fm.away_score), 2) as avg_goals_per_match,
        round(avg(fm.home_score), 2) as avg_home_goals,
        round(avg(fm.away_score), 2) as avg_away_goals,
        sum(fm.home_score + fm.away_score) as total_goals_in_season,
        
        -- Win/Draw percentages
        round(sum(case when fm.winner_code = 1 then 1 else 0 end)::numeric / nullif(count(*), 0) * 100, 1) as home_win_pct,
        round(sum(case when fm.winner_code = 2 then 1 else 0 end)::numeric / nullif(count(*), 0) * 100, 1) as away_win_pct,
        round(sum(case when fm.winner_code = 3 then 1 else 0 end)::numeric / nullif(count(*), 0) * 100, 1) as draw_pct,
        
        -- BTTS
        round(sum(case when fm.home_score > 0 and fm.away_score > 0 then 1 else 0 end)::numeric / nullif(count(*), 0) * 100, 1) as btts_pct,
        
        -- Clean sheets
        round(sum(case when fm.home_score = 0 or fm.away_score = 0 then 1 else 0 end)::numeric / nullif(count(*), 0) * 100, 1) as clean_sheet_pct
        
    from {{ ref('fact_match') }} fm
    where fm.status_type = 'finished'
    group by fm.season_id, fm.season_name, fm.season_year
),
league_attack_stats as (
    -- Attack statistics from mart_team_attack aggregated per league/season
    select
        season_id,
        season_name,
        season_year,
        count(distinct team_id) as teams_count,
        
        -- Goals and xG
        round(avg(coalesce(goals_per_game, 0)), 2) as avg_goals_per_game_per_team,
        round(avg(coalesce(total_xg, 0)) / nullif(avg(coalesce(matches_played, 0)), 0), 2) as avg_xg_per_game,
        round(avg(coalesce(xg_difference, 0)), 2) as avg_xg_difference,
        round(avg(coalesce(xg_diff_per_game, 0)), 2) as avg_xg_diff_per_game,
        sum(coalesce(total_xg, 0)) as total_xg_in_season,
        
        -- Big chances
        round(avg(coalesce(big_chances_created_per_game, 0)), 2) as avg_big_chances_created_per_game,
        round(avg(coalesce(big_chances_missed_per_game, 0)), 2) as avg_big_chances_missed_per_game,
        round(avg(coalesce(big_chances_scored_per_game, 0)), 2) as avg_big_chances_scored_per_game,
        sum(coalesce(total_big_chances_created, 0)) as total_big_chances_created,
        sum(coalesce(total_big_chances_missed, 0)) as total_big_chances_missed,
        sum(coalesce(total_big_chances_scored, 0)) as total_big_chances_scored,
        
        -- Shots
        round(avg(coalesce(shots_per_game, 0)), 2) as avg_shots_per_game,
        round(avg(coalesce(shots_on_target_per_game, 0)), 2) as avg_shots_on_target_per_game,
        round(avg(coalesce(shots_off_target_per_game, 0)), 2) as avg_shots_off_target_per_game,
        round(avg(coalesce(blocked_shots_per_game, 0)), 2) as avg_blocked_shots_per_game,
        round(avg(coalesce(shots_inside_box_per_game, 0)), 2) as avg_shots_inside_box_per_game,
        round(avg(coalesce(shots_outside_box_per_game, 0)), 2) as avg_shots_outside_box_per_game,
        sum(coalesce(total_shots, 0)) as total_shots_in_season,
        sum(coalesce(total_shots_on_target, 0)) as total_shots_on_target_in_season,
        sum(coalesce(total_hit_woodwork, 0)) as total_hit_woodwork_in_season,
        
        -- Other attack metrics
        round(avg(coalesce(corners_per_game, 0)), 2) as avg_corners_per_game,
        round(avg(coalesce(avg_dribbles_success_pct, 0)), 1) as avg_dribbles_success_pct,
        round(avg(coalesce(touches_in_box_per_game, 0)), 2) as avg_touches_in_box_per_game,
        sum(coalesce(total_corners, 0)) as total_corners_in_season
        
    from {{ ref('mart_team_attack') }}
    group by season_id, season_name, season_year
),
league_defense_stats as (
    -- Defense statistics from mart_team_defense aggregated per league/season
    select
        season_id,
        season_name,
        season_year,
        
        -- Goals conceded and xGA
        round(avg(coalesce(goals_conceded_per_game, 0)), 2) as avg_goals_conceded_per_game,
        round(avg(coalesce(xga_per_game, 0)), 2) as avg_xga_per_game,
        round(avg(coalesce(xga_difference, 0)), 2) as avg_xga_difference,
        round(avg(coalesce(xga_difference_per_game, 0)), 2) as avg_xga_difference_per_game,
        sum(coalesce(total_xga, 0)) as total_xga_in_season,
        
        -- Clean sheets
        round(avg(coalesce(clean_sheet_pct, 0)), 1) as avg_clean_sheet_pct,
        
        -- Defensive actions
        round(avg(coalesce(saves_per_game, 0)), 2) as avg_saves_per_game,
        round(avg(coalesce(tackles_per_game, 0)), 2) as avg_tackles_per_game,
        round(avg(coalesce(avg_tackles_won_pct, 0)), 1) as avg_tackles_won_pct,
        round(avg(coalesce(interceptions_per_game, 0)), 2) as avg_interceptions_per_game,
        round(avg(coalesce(clearances_per_game, 0)), 2) as avg_clearances_per_game,
        round(avg(coalesce(blocked_shots_per_game, 0)), 2) as avg_blocked_shots_per_game_def,
        round(avg(coalesce(ball_recoveries_per_game, 0)), 2) as avg_ball_recoveries_per_game,
        sum(coalesce(total_saves, 0)) as total_saves_in_season,
        sum(coalesce(total_tackles, 0)) as total_tackles_in_season,
        sum(coalesce(total_interceptions, 0)) as total_interceptions_in_season,
        
        -- Duels
        round(avg(coalesce(avg_aerial_duels_pct, 0)), 1) as avg_aerial_duels_pct,
        round(avg(coalesce(avg_ground_duels_pct, 0)), 1) as avg_ground_duels_pct,
        round(avg(coalesce(avg_duels_won_pct, 0)), 1) as avg_duels_won_pct,
        
        -- Errors
        sum(coalesce(total_errors_lead_to_goal, 0)) as total_errors_lead_to_goal_in_season,
        sum(coalesce(total_errors_lead_to_shot, 0)) as total_errors_lead_to_shot_in_season
        
    from {{ ref('mart_team_defense') }}
    group by season_id, season_name, season_year
),
league_possession_stats as (
    -- Possession statistics from mart_team_possession aggregated per league/season
    select
        season_id,
        season_name,
        season_year,
        
        -- Possession
        round(avg(coalesce(avg_possession_pct, 0)), 1) as avg_possession_pct,
        
        -- Passing
        round(avg(coalesce(pass_accuracy_pct, 0)), 1) as avg_pass_accuracy_pct,
        round(avg(coalesce(accurate_passes_per_game, 0)), 1) as avg_accurate_passes_per_game,
        round(avg(coalesce(total_passes_per_game, 0)), 1) as avg_total_passes_per_game,
        sum(coalesce(total_accurate_passes, 0)) as total_accurate_passes_in_season,
        sum(coalesce(total_passes, 0)) as total_passes_in_season,
        
        -- Long balls and crosses
        round(avg(coalesce(accurate_long_balls_per_game, 0)), 2) as avg_accurate_long_balls_per_game,
        round(avg(coalesce(accurate_crosses_per_game, 0)), 2) as avg_accurate_crosses_per_game,
        sum(coalesce(total_accurate_long_balls, 0)) as total_accurate_long_balls_in_season,
        sum(coalesce(total_accurate_crosses, 0)) as total_accurate_crosses_in_season,
        
        -- Final third and box entries
        round(avg(coalesce(final_third_entries_per_game, 0)), 2) as avg_final_third_entries_per_game,
        round(avg(coalesce(touches_in_box_per_game, 0)), 2) as avg_touches_in_box_per_game_poss,
        sum(coalesce(total_final_third_entries, 0)) as total_final_third_entries_in_season,
        
        -- Other possession metrics
        round(avg(coalesce(dispossessed_per_game, 0)), 2) as avg_dispossessed_per_game,
        round(avg(coalesce(throw_ins_per_game, 0)), 2) as avg_throw_ins_per_game,
        round(avg(coalesce(goal_kicks_per_game, 0)), 2) as avg_goal_kicks_per_game
        
    from {{ ref('mart_team_possession') }}
    group by season_id, season_name, season_year
),
league_discipline_stats as (
    -- Discipline statistics from mart_team_discipline aggregated per league/season
    select
        season_id,
        season_name,
        season_year,
        
        -- Cards
        round(avg(coalesce(yellow_cards_per_game, 0)), 2) as avg_yellow_cards_per_game,
        sum(coalesce(total_yellow_cards, 0)) as total_yellow_cards_in_season,
        sum(coalesce(total_red_cards, 0)) as total_red_cards_in_season,
        
        -- Fouls and free kicks
        round(avg(coalesce(fouls_per_game, 0)), 2) as avg_fouls_per_game,
        round(avg(coalesce(offsides_per_game, 0)), 2) as avg_offsides_per_game,
        round(avg(coalesce(free_kicks_per_game, 0)), 2) as avg_free_kicks_per_game,
        sum(coalesce(total_fouls, 0)) as total_fouls_in_season,
        sum(coalesce(total_offsides, 0)) as total_offsides_in_season,
        sum(coalesce(total_free_kicks, 0)) as total_free_kicks_in_season
        
    from {{ ref('mart_team_discipline') }}
    group by season_id, season_name, season_year
)
select
    lms.season_id,
    lms.season_name,
    lms.season_year,
    lms.tournament_id,
    lms.tournament_name,
    lms.total_matches,
    coalesce(las.teams_count, 0) as teams_count,
    
    -- Match outcome statistics
    coalesce(lms.avg_goals_per_match, 0) as avg_goals_per_match,
    coalesce(lms.avg_home_goals, 0) as avg_home_goals,
    coalesce(lms.avg_away_goals, 0) as avg_away_goals,
    coalesce(lms.home_win_pct, 0) as home_win_pct,
    coalesce(lms.away_win_pct, 0) as away_win_pct,
    coalesce(lms.draw_pct, 0) as draw_pct,
    coalesce(lms.btts_pct, 0) as btts_pct,
    coalesce(lms.clean_sheet_pct, 0) as clean_sheet_pct,
    
    -- Attack statistics
    coalesce(las.avg_goals_per_game_per_team, 0) as avg_goals_per_game_per_team,
    coalesce(las.avg_xg_per_game, 0) as avg_xg_per_game,
    coalesce(las.avg_xg_difference, 0) as avg_xg_difference,
    coalesce(las.avg_xg_diff_per_game, 0) as avg_xg_diff_per_game,
    coalesce(las.avg_big_chances_created_per_game, 0) as avg_big_chances_created_per_game,
    coalesce(las.avg_big_chances_missed_per_game, 0) as avg_big_chances_missed_per_game,
    coalesce(las.avg_big_chances_scored_per_game, 0) as avg_big_chances_scored_per_game,
    coalesce(las.avg_shots_per_game, 0) as avg_shots_per_game,
    coalesce(las.avg_shots_on_target_per_game, 0) as avg_shots_on_target_per_game,
    coalesce(las.avg_shots_off_target_per_game, 0) as avg_shots_off_target_per_game,
    coalesce(las.avg_blocked_shots_per_game, 0) as avg_blocked_shots_per_game,
    coalesce(las.avg_shots_inside_box_per_game, 0) as avg_shots_inside_box_per_game,
    coalesce(las.avg_shots_outside_box_per_game, 0) as avg_shots_outside_box_per_game,
    coalesce(las.avg_corners_per_game, 0) as avg_corners_per_game,
    coalesce(las.avg_dribbles_success_pct, 0) as avg_dribbles_success_pct,
    coalesce(las.avg_touches_in_box_per_game, 0) as avg_touches_in_box_per_game,
    
    -- Defense statistics
    coalesce(lds.avg_goals_conceded_per_game, 0) as avg_goals_conceded_per_game,
    coalesce(lds.avg_xga_per_game, 0) as avg_xga_per_game,
    coalesce(lds.avg_xga_difference, 0) as avg_xga_difference,
    coalesce(lds.avg_xga_difference_per_game, 0) as avg_xga_difference_per_game,
    coalesce(lds.avg_clean_sheet_pct, 0) as avg_clean_sheet_pct,
    coalesce(lds.avg_saves_per_game, 0) as avg_saves_per_game,
    coalesce(lds.avg_tackles_per_game, 0) as avg_tackles_per_game,
    coalesce(lds.avg_tackles_won_pct, 0) as avg_tackles_won_pct,
    coalesce(lds.avg_interceptions_per_game, 0) as avg_interceptions_per_game,
    coalesce(lds.avg_clearances_per_game, 0) as avg_clearances_per_game,
    coalesce(lds.avg_blocked_shots_per_game_def, 0) as avg_blocked_shots_per_game_def,
    coalesce(lds.avg_ball_recoveries_per_game, 0) as avg_ball_recoveries_per_game,
    coalesce(lds.avg_aerial_duels_pct, 0) as avg_aerial_duels_pct,
    coalesce(lds.avg_ground_duels_pct, 0) as avg_ground_duels_pct,
    coalesce(lds.avg_duels_won_pct, 0) as avg_duels_won_pct,
    
    -- Possession statistics
    coalesce(lps.avg_possession_pct, 0) as avg_possession_pct,
    coalesce(lps.avg_pass_accuracy_pct, 0) as avg_pass_accuracy_pct,
    coalesce(lps.avg_accurate_passes_per_game, 0) as avg_accurate_passes_per_game,
    coalesce(lps.avg_total_passes_per_game, 0) as avg_total_passes_per_game,
    coalesce(lps.avg_accurate_long_balls_per_game, 0) as avg_accurate_long_balls_per_game,
    coalesce(lps.avg_accurate_crosses_per_game, 0) as avg_accurate_crosses_per_game,
    coalesce(lps.avg_final_third_entries_per_game, 0) as avg_final_third_entries_per_game,
    coalesce(lps.avg_touches_in_box_per_game_poss, 0) as avg_touches_in_box_per_game_poss,
    coalesce(lps.avg_dispossessed_per_game, 0) as avg_dispossessed_per_game,
    coalesce(lps.avg_throw_ins_per_game, 0) as avg_throw_ins_per_game,
    coalesce(lps.avg_goal_kicks_per_game, 0) as avg_goal_kicks_per_game,
    
    -- Discipline statistics
    coalesce(ldi.avg_yellow_cards_per_game, 0) as avg_yellow_cards_per_game,
    coalesce(ldi.avg_fouls_per_game, 0) as avg_fouls_per_game,
    coalesce(ldi.avg_offsides_per_game, 0) as avg_offsides_per_game,
    coalesce(ldi.avg_free_kicks_per_game, 0) as avg_free_kicks_per_game,
    
    -- Season totals
    coalesce(lms.total_goals_in_season, 0) as total_goals_in_season,
    coalesce(las.total_xg_in_season, 0) as total_xg_in_season,
    coalesce(las.total_shots_in_season, 0) as total_shots_in_season,
    coalesce(las.total_shots_on_target_in_season, 0) as total_shots_on_target_in_season,
    coalesce(las.total_hit_woodwork_in_season, 0) as total_hit_woodwork_in_season,
    coalesce(las.total_big_chances_created, 0) as total_big_chances_created,
    coalesce(las.total_big_chances_missed, 0) as total_big_chances_missed,
    coalesce(las.total_big_chances_scored, 0) as total_big_chances_scored,
    coalesce(las.total_corners_in_season, 0) as total_corners_in_season,
    coalesce(lds.total_xga_in_season, 0) as total_xga_in_season,
    coalesce(lds.total_saves_in_season, 0) as total_saves_in_season,
    coalesce(lds.total_tackles_in_season, 0) as total_tackles_in_season,
    coalesce(lds.total_interceptions_in_season, 0) as total_interceptions_in_season,
    coalesce(lds.total_errors_lead_to_goal_in_season, 0) as total_errors_lead_to_goal_in_season,
    coalesce(lds.total_errors_lead_to_shot_in_season, 0) as total_errors_lead_to_shot_in_season,
    coalesce(lps.total_accurate_passes_in_season, 0) as total_accurate_passes_in_season,
    coalesce(lps.total_passes_in_season, 0) as total_passes_in_season,
    coalesce(lps.total_accurate_long_balls_in_season, 0) as total_accurate_long_balls_in_season,
    coalesce(lps.total_accurate_crosses_in_season, 0) as total_accurate_crosses_in_season,
    coalesce(lps.total_final_third_entries_in_season, 0) as total_final_third_entries_in_season,
    coalesce(ldi.total_yellow_cards_in_season, 0) as total_yellow_cards_in_season,
    coalesce(ldi.total_red_cards_in_season, 0) as total_red_cards_in_season,
    coalesce(ldi.total_fouls_in_season, 0) as total_fouls_in_season,
    coalesce(ldi.total_offsides_in_season, 0) as total_offsides_in_season,
    coalesce(ldi.total_free_kicks_in_season, 0) as total_free_kicks_in_season

from league_match_stats lms
left join league_attack_stats las
    on lms.season_id = las.season_id
left join league_defense_stats lds
    on lms.season_id = lds.season_id
left join league_possession_stats lps
    on lms.season_id = lps.season_id
left join league_discipline_stats ldi
    on lms.season_id = ldi.season_id
order by lms.season_year desc, lms.tournament_id