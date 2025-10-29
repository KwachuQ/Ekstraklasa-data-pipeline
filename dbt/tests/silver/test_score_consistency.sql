-- Test: Verify score consistency between periods and full match

with period_scores as (
    select
        ftm.match_id,
        ftm.team_id,
        fm.home_team_id,
        fm.away_team_id,
        fm.home_score_period1 as home_goals_1st,
        fm.away_score_period1 as away_goals_1st,
        fm.home_score_period2 as home_goals_2nd,
        fm.away_score_period2 as away_goals_2nd,
        fm.home_score as home_goals_all,
        fm.away_score as away_goals_all
    from {{ ref('fact_team_match') }} ftm
    join {{ ref('fact_match') }} fm on ftm.match_id = fm.match_id
    where ftm.period = 'ALL'
)
select
    match_id,
    team_id,
    case 
        when team_id = home_team_id then home_goals_1st
        else away_goals_1st
    end as goals_1st,
    case 
        when team_id = home_team_id then home_goals_2nd
        else away_goals_2nd
    end as goals_2nd,
    case 
        when team_id = home_team_id then home_goals_all
        else away_goals_all
    end as goals_all
from period_scores
where coalesce(
        case when team_id = home_team_id then home_goals_1st else away_goals_1st end, 0) +
      coalesce(
        case when team_id = home_team_id then home_goals_2nd else away_goals_2nd end, 0) !=
      case when team_id = home_team_id then home_goals_all else away_goals_all end