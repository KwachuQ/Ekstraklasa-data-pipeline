-- Test: Verify all statistics have corresponding match records

select
    s.match_id,
    s.period
from {{ source('silver', 'staging_stats') }} s
left join {{ source('silver', 'staging_matches') }} m on s.match_id = m.match_id
where m.match_id is null