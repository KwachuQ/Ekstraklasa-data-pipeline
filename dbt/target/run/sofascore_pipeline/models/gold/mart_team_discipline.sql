
  
    

  create  table "dwh"."gold"."mart_team_discipline__dbt_tmp"
  
  
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
    -- Statystyki dyscyplinarne (tylko gdzie dostępne)
    select
        ftm.team_id,
        ftm.team_name,
        fm.season_id,
        fm.season_name,
        fm.season_year,
        ftm.match_id,
        coalesce(ftm.stat_yellowcards, 0) as yellow_cards,
        coalesce(ftm.stat_redcards, 0) as red_cards,
        coalesce(ftm.stat_fouls, 0) as fouls,
        coalesce(ftm.stat_offsides, 0) as offsides,
        coalesce(ftm.stat_freekicks, 0) as free_kicks
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
        coalesce(ts.yellow_cards, 0) as yellow_cards,
        coalesce(ts.red_cards, 0) as red_cards,
        coalesce(ts.fouls, 0) as fouls,
        coalesce(ts.offsides, 0) as offsides,
        coalesce(ts.free_kicks, 0) as free_kicks
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
    sum(yellow_cards) as total_yellow_cards,
    round(avg(yellow_cards), 2) as yellow_cards_per_game,
    sum(red_cards) as total_red_cards,
    sum(fouls) as total_fouls,
    round(avg(fouls), 2) as fouls_per_game,
    sum(offsides) as total_offsides,
    round(avg(offsides), 2) as offsides_per_game,
    sum(free_kicks) as total_free_kicks,
    round(avg(free_kicks), 2) as free_kicks_per_game
from combined
group by team_id, team_name, season_id, season_name, season_year
order by season_year desc, total_yellow_cards desc
  );
  