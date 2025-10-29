
  
    

  create  table "dwh"."silver"."fact_team_match__dbt_tmp"
  
  
    as
  
  (
    



with s as (
  select
    match_id,
    
case
  when lower(period) in ('all','match','full time','ft','0','overall') then 'ALL'
  when lower(period) in ('1st','first half','1h','period1','1') then '1ST'
  when lower(period) in ('2nd','second half','2h','period2','2') then '2ND'
  else 'ALL'
end
 as period,
    coalesce(nullif(statistic_key,''), statistic_name) as metric_key_raw,
    value_type,
    home_value_numeric, away_value_numeric,
    home_percentage,    away_percentage
  from "dwh"."silver"."staging_stats"
),
teams as (
  select match_id, home_team_id, home_team_name, away_team_id, away_team_name
  from "dwh"."silver"."staging_matches"
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
    
    , max(case when metric_key_raw = 'accurateCross' then value_num end)::numeric as stat_accuratecross
    
    , max(case when metric_key_raw = 'accurateLongBalls' then value_num end)::numeric as stat_accuratelongballs
    
    , max(case when metric_key_raw = 'accuratePasses' then value_num end)::numeric as stat_accuratepasses
    
    , max(case when metric_key_raw = 'accurateThroughBall' then value_num end)::numeric as stat_accuratethroughball
    
    , max(case when metric_key_raw = 'aerialDuelsPercentage' then value_num end)::numeric as stat_aerialduelspercentage
    
    , max(case when metric_key_raw = 'ballPossession' then value_num end)::numeric as stat_ballpossession
    
    , max(case when metric_key_raw = 'ballRecovery' then value_num end)::numeric as stat_ballrecovery
    
    , max(case when metric_key_raw = 'bigChanceCreated' then value_num end)::numeric as stat_bigchancecreated
    
    , max(case when metric_key_raw = 'bigChanceMissed' then value_num end)::numeric as stat_bigchancemissed
    
    , max(case when metric_key_raw = 'bigChanceScored' then value_num end)::numeric as stat_bigchancescored
    
    , max(case when metric_key_raw = 'blockedScoringAttempt' then value_num end)::numeric as stat_blockedscoringattempt
    
    , max(case when metric_key_raw = 'cornerKicks' then value_num end)::numeric as stat_cornerkicks
    
    , max(case when metric_key_raw = 'dispossessed' then value_num end)::numeric as stat_dispossessed
    
    , max(case when metric_key_raw = 'diveSaves' then value_num end)::numeric as stat_divesaves
    
    , max(case when metric_key_raw = 'dribblesPercentage' then value_num end)::numeric as stat_dribblespercentage
    
    , max(case when metric_key_raw = 'duelWonPercent' then value_num end)::numeric as stat_duelwonpercent
    
    , max(case when metric_key_raw = 'errorsLeadToGoal' then value_num end)::numeric as stat_errorsleadtogoal
    
    , max(case when metric_key_raw = 'errorsLeadToShot' then value_num end)::numeric as stat_errorsleadtoshot
    
    , max(case when metric_key_raw = 'expectedGoals' then value_num end)::numeric as stat_expectedgoals
    
    , max(case when metric_key_raw = 'finalThirdEntries' then value_num end)::numeric as stat_finalthirdentries
    
    , max(case when metric_key_raw = 'finalThirdPhaseStatistic' then value_num end)::numeric as stat_finalthirdphasestatistic
    
    , max(case when metric_key_raw = 'fouledFinalThird' then value_num end)::numeric as stat_fouledfinalthird
    
    , max(case when metric_key_raw = 'fouls' then value_num end)::numeric as stat_fouls
    
    , max(case when metric_key_raw = 'freeKicks' then value_num end)::numeric as stat_freekicks
    
    , max(case when metric_key_raw = 'goalkeeperSaves' then value_num end)::numeric as stat_goalkeepersaves
    
    , max(case when metric_key_raw = 'goalKicks' then value_num end)::numeric as stat_goalkicks
    
    , max(case when metric_key_raw = 'groundDuelsPercentage' then value_num end)::numeric as stat_groundduelspercentage
    
    , max(case when metric_key_raw = 'highClaims' then value_num end)::numeric as stat_highclaims
    
    , max(case when metric_key_raw = 'hitWoodwork' then value_num end)::numeric as stat_hitwoodwork
    
    , max(case when metric_key_raw = 'interceptionWon' then value_num end)::numeric as stat_interceptionwon
    
    , max(case when metric_key_raw = 'offsides' then value_num end)::numeric as stat_offsides
    
    , max(case when metric_key_raw = 'passes' then value_num end)::numeric as stat_passes
    
    , max(case when metric_key_raw = 'penaltySaves' then value_num end)::numeric as stat_penaltysaves
    
    , max(case when metric_key_raw = 'punches' then value_num end)::numeric as stat_punches
    
    , max(case when metric_key_raw = 'redCards' then value_num end)::numeric as stat_redcards
    
    , max(case when metric_key_raw = 'shotsOffGoal' then value_num end)::numeric as stat_shotsoffgoal
    
    , max(case when metric_key_raw = 'shotsOnGoal' then value_num end)::numeric as stat_shotsongoal
    
    , max(case when metric_key_raw = 'throwIns' then value_num end)::numeric as stat_throwins
    
    , max(case when metric_key_raw = 'totalClearance' then value_num end)::numeric as stat_totalclearance
    
    , max(case when metric_key_raw = 'totalShotsInsideBox' then value_num end)::numeric as stat_totalshotsinsidebox
    
    , max(case when metric_key_raw = 'totalShotsOnGoal' then value_num end)::numeric as stat_totalshotsongoal
    
    , max(case when metric_key_raw = 'totalShotsOutsideBox' then value_num end)::numeric as stat_totalshotsoutsidebox
    
    , max(case when metric_key_raw = 'totalTackle' then value_num end)::numeric as stat_totaltackle
    
    , max(case when metric_key_raw = 'touchesInOppBox' then value_num end)::numeric as stat_touchesinoppbox
    
    , max(case when metric_key_raw = 'wonTacklePercent' then value_num end)::numeric as stat_wontacklepercent
    
    , max(case when metric_key_raw = 'yellowCards' then value_num end)::numeric as stat_yellowcards
    
  from u
  where period in ('ALL','1ST','2ND')
  group by match_id, team_id, team_name, period
)
select
  md5(cast(coalesce(cast(match_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(team_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(period as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as match_team_period_key,
  match_id,
  team_id,
  team_name,
  period
  
  , stat_accuratecross
  
  , stat_accuratelongballs
  
  , stat_accuratepasses
  
  , stat_accuratethroughball
  
  , stat_aerialduelspercentage
  
  , stat_ballpossession
  
  , stat_ballrecovery
  
  , stat_bigchancecreated
  
  , stat_bigchancemissed
  
  , stat_bigchancescored
  
  , stat_blockedscoringattempt
  
  , stat_cornerkicks
  
  , stat_dispossessed
  
  , stat_divesaves
  
  , stat_dribblespercentage
  
  , stat_duelwonpercent
  
  , stat_errorsleadtogoal
  
  , stat_errorsleadtoshot
  
  , stat_expectedgoals
  
  , stat_finalthirdentries
  
  , stat_finalthirdphasestatistic
  
  , stat_fouledfinalthird
  
  , stat_fouls
  
  , stat_freekicks
  
  , stat_goalkeepersaves
  
  , stat_goalkicks
  
  , stat_groundduelspercentage
  
  , stat_highclaims
  
  , stat_hitwoodwork
  
  , stat_interceptionwon
  
  , stat_offsides
  
  , stat_passes
  
  , stat_penaltysaves
  
  , stat_punches
  
  , stat_redcards
  
  , stat_shotsoffgoal
  
  , stat_shotsongoal
  
  , stat_throwins
  
  , stat_totalclearance
  
  , stat_totalshotsinsidebox
  
  , stat_totalshotsongoal
  
  , stat_totalshotsoutsidebox
  
  , stat_totaltackle
  
  , stat_touchesinoppbox
  
  , stat_wontacklepercent
  
  , stat_yellowcards
  
from pivoted
  );
  