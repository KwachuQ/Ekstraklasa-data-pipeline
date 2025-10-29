

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
    frm.points_pct_after_conceding_first

from "dwh"."gold"."mart_team_overview" ov
left join "dwh"."gold"."mart_team_attack" att 
    on ov.team_id = att.team_id 
    and ov.season_id = att.season_id
left join "dwh"."gold"."mart_team_defense" def 
    on ov.team_id = def.team_id 
    and ov.season_id = def.season_id
left join "dwh"."gold"."mart_team_possession" pos 
    on ov.team_id = pos.team_id 
    and ov.season_id = pos.season_id
left join "dwh"."gold"."mart_team_discipline" dis 
    on ov.team_id = dis.team_id 
    and ov.season_id = dis.season_id
left join "dwh"."gold"."mart_team_form" frm 
    on ov.team_id = frm.team_id 
    and ov.season_id = frm.season_id

order by ov.season_year desc, ov.total_points desc