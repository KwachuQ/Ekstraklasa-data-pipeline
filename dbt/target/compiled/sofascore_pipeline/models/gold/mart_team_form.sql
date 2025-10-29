

with matches as (
    select
        season_id,
        season_name,
        season_year,
        home_team_id as team_id,
        home_team_name as team_name,
        match_id,
        match_date,
        case 
            when winner_code = 1 then 'W'
            when winner_code = 2 then 'D'
            when winner_code = 3 then 'L'
        end as result,
        case 
            when winner_code = 1 then 3
            when winner_code = 2 then 1
            else 0
        end as points,
        home_score as goals_for,
        away_score as goals_against,
        home_score_period1 as first_half_goals_for,
        away_score_period1 as first_half_goals_against
    from "dwh"."silver"."fact_match"
    where status_type = 'finished'
    
    union all
    
    select
        season_id,
        season_name,
        season_year,
        away_team_id as team_id,
        away_team_name as team_name,
        match_id,
        match_date,
        case 
            when winner_code = 3 then 'W'
            when winner_code = 2 then 'D'
            when winner_code = 1 then 'L'
        end as result,
        case 
            when winner_code = 3 then 3
            when winner_code = 2 then 1
            else 0
        end as points,
        away_score as goals_for,
        home_score as goals_against,
        away_score_period1 as first_half_goals_for,
        home_score_period1 as first_half_goals_against
    from "dwh"."silver"."fact_match"
    where status_type = 'finished'
),
last_5_matches as (
    select
        m.*,
        row_number() over (partition by team_id, season_id order by match_date desc) as match_rank
    from matches m
),
form_last_5 as (
    select
        team_id,
        team_name,
        season_id,
        season_name,
        season_year,
        string_agg(result, '' order by match_date desc) as last_5_results,
        sum(points) as points_last_5,
        sum(case when result = 'W' then 1 else 0 end) as wins_last_5,
        sum(case when result = 'D' then 1 else 0 end) as draws_last_5,
        sum(case when result = 'L' then 1 else 0 end) as losses_last_5,
        sum(goals_for) as goals_for_last_5,
        sum(goals_against) as goals_against_last_5
    from last_5_matches
    where match_rank <= 5
    group by team_id, team_name, season_id, season_name, season_year
),
halftime_leading as (
    select
        team_id,
        season_id,
        count(*) as halftime_leading_count,
        sum(case when result = 'W' then 1 else 0 end) as halftime_leading_wins,
        sum(case when result = 'D' then 1 else 0 end) as halftime_leading_draws,
        sum(case when result = 'L' then 1 else 0 end) as halftime_leading_losses
    from matches
    where first_half_goals_for > first_half_goals_against
    group by team_id, season_id
),
conceded_first as (
    select
        team_id,
        season_id,
        count(*) as conceded_first_count,
        sum(points) as points_after_conceding_first
    from matches
    where goals_against > 0
        and match_id in (
            select match_id
            from matches m2
            where m2.team_id = matches.team_id
                and m2.season_id = matches.season_id
        )
    group by team_id, season_id
)
select
    f.team_id,
    f.team_name,
    f.season_id,
    f.season_name,
    f.season_year,
    f.last_5_results,
    f.points_last_5,
    f.wins_last_5,
    f.draws_last_5,
    f.losses_last_5,
    f.goals_for_last_5,
    f.goals_against_last_5,
    h.halftime_leading_count,
    h.halftime_leading_wins,
    h.halftime_leading_draws,
    h.halftime_leading_losses,
    round(h.halftime_leading_wins::numeric / nullif(h.halftime_leading_count, 0)::numeric * 100, 1) as halftime_leading_win_pct,
    c.conceded_first_count,
    c.points_after_conceding_first,
    round(c.points_after_conceding_first::numeric / nullif(c.conceded_first_count, 0)::numeric * 100 / 3, 1) as points_pct_after_conceding_first
from form_last_5 f
left join halftime_leading h on f.team_id = h.team_id and f.season_id = h.season_id
left join conceded_first c on f.team_id = c.team_id and f.season_id = c.season_id
order by f.season_year desc, f.points_last_5 desc