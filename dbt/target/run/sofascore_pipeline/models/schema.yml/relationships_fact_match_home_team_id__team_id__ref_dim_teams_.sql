
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select home_team_id as from_field
    from "dwh"."silver"."fact_match"
    where home_team_id is not null
),

parent as (
    select team_id as to_field
    from "dwh"."silver"."dim_teams"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test