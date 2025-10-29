
  
    

  create  table "dwh"."gold"."mart_team_defense__dbt_tmp"
  
  
    as
  
  (
    

with all_matches as (
    -- Wszystkie mecze z bramkami straconymi
    select
        fm.season_id,
        fm.season_name,
        fm.season_year,
        fm.home_team_id as team_id,
        fm.home_team_name as team_name,
        fm.match_id,
        fm.away_score as goals_conceded,
        case when fm.away_score = 0 then 1 else 0 end as clean_sheet
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
        fm.home_score as goals_conceded,
        case when fm.home_score = 0 then 1 else 0 end as clean_sheet
    from "dwh"."silver"."fact_match" fm
    where fm.status_type = 'finished'
),
team_stats as (
    -- Statystyki obronne (tylko gdzie dostępne)
    select
        ftm.team_id,
        ftm.team_name,
        fm.season_id,
        fm.season_name,
        fm.season_year,
        ftm.match_id,
        coalesce(ftm.stat_goalkeepersaves, 0) as goalkeeper_saves,
        coalesce(ftm.stat_totaltackle, 0) as tackles,
        coalesce(ftm.stat_wontacklepercent, 0) as tackles_won_pct,
        coalesce(ftm.stat_interceptionwon, 0) as interceptions,
        coalesce(ftm.stat_totalclearance, 0) as clearances,
        coalesce(ftm.stat_blockedscoringattempt, 0) as blocked_shots,
        coalesce(ftm.stat_ballrecovery, 0) as ball_recoveries,
        coalesce(ftm.stat_aerialduelspercentage, 0) as aerial_duels_pct,
        coalesce(ftm.stat_groundduelspercentage, 0) as ground_duels_pct,
        coalesce(ftm.stat_duelwonpercent, 0) as duels_won_pct,
        coalesce(ftm.stat_errorsleadtogoal, 0) as errors_lead_to_goal,
        coalesce(ftm.stat_errorsleadtoshot, 0) as errors_lead_to_shot
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
        am.goals_conceded,
        am.clean_sheet,
        coalesce(ts.goalkeeper_saves, 0) as goalkeeper_saves,
        coalesce(ts.tackles, 0) as tackles,
        coalesce(ts.tackles_won_pct, 0) as tackles_won_pct,
        coalesce(ts.interceptions, 0) as interceptions,
        coalesce(ts.clearances, 0) as clearances,
        coalesce(ts.blocked_shots, 0) as blocked_shots,
        coalesce(ts.ball_recoveries, 0) as ball_recoveries,
        coalesce(ts.aerial_duels_pct, 0) as aerial_duels_pct,
        coalesce(ts.ground_duels_pct, 0) as ground_duels_pct,
        coalesce(ts.duels_won_pct, 0) as duels_won_pct,
        coalesce(ts.errors_lead_to_goal, 0) as errors_lead_to_goal,
        coalesce(ts.errors_lead_to_shot, 0) as errors_lead_to_shot
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
    sum(goals_conceded) as total_goals_conceded,
    round(avg(goals_conceded), 2) as goals_conceded_per_game,
    sum(clean_sheet) as clean_sheets,
    round(sum(clean_sheet)::numeric / count(distinct match_id)::numeric * 100, 1) as clean_sheet_pct,
    sum(goalkeeper_saves) as total_saves,
    round(avg(goalkeeper_saves), 2) as saves_per_game,
    sum(tackles) as total_tackles,
    round(avg(tackles), 2) as tackles_per_game,
    round(avg(nullif(tackles_won_pct, 0)), 1) as avg_tackles_won_pct,
    sum(interceptions) as total_interceptions,
    round(avg(interceptions), 2) as interceptions_per_game,
    sum(clearances) as total_clearances,
    round(avg(clearances), 2) as clearances_per_game,
    sum(blocked_shots) as total_blocked_shots,
    round(avg(blocked_shots), 2) as blocked_shots_per_game,
    sum(ball_recoveries) as total_ball_recoveries,
    round(avg(ball_recoveries), 2) as ball_recoveries_per_game,
    round(avg(nullif(aerial_duels_pct, 0)), 1) as avg_aerial_duels_pct,
    round(avg(nullif(ground_duels_pct, 0)), 1) as avg_ground_duels_pct,
    round(avg(nullif(duels_won_pct, 0)), 1) as avg_duels_won_pct,
    sum(errors_lead_to_goal) as total_errors_lead_to_goal,
    sum(errors_lead_to_shot) as total_errors_lead_to_shot
from combined
group by team_id, team_name, season_id, season_name, season_year
order by season_year desc, goals_conceded_per_game asc
  );
  