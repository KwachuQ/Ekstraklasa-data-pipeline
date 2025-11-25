
      -- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
                
                
            
                
                
            
        
    

    

    merge into "dwh"."bronze"."full_stats_data" as DBT_INTERNAL_DEST
        using "full_stats_data__dbt_tmp105109535787" as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.match_id = DBT_INTERNAL_DEST.match_id
                ) and (
                    DBT_INTERNAL_SOURCE.period = DBT_INTERNAL_DEST.period
                ) and (
                    DBT_INTERNAL_SOURCE.group_name = DBT_INTERNAL_DEST.group_name
                ) and (
                    DBT_INTERNAL_SOURCE.statistic_key = DBT_INTERNAL_DEST.statistic_key
                )

    
    when matched then update set
        "match_id" = DBT_INTERNAL_SOURCE."match_id","period" = DBT_INTERNAL_SOURCE."period","group_name" = DBT_INTERNAL_SOURCE."group_name","statistic_key" = DBT_INTERNAL_SOURCE."statistic_key","tournament_id" = DBT_INTERNAL_SOURCE."tournament_id","season_id" = DBT_INTERNAL_SOURCE."season_id","statistic_name" = DBT_INTERNAL_SOURCE."statistic_name","statistics_type" = DBT_INTERNAL_SOURCE."statistics_type","value_type" = DBT_INTERNAL_SOURCE."value_type","render_type" = DBT_INTERNAL_SOURCE."render_type","compare_code" = DBT_INTERNAL_SOURCE."compare_code","home_value_text" = DBT_INTERNAL_SOURCE."home_value_text","away_value_text" = DBT_INTERNAL_SOURCE."away_value_text","home_value_numeric" = DBT_INTERNAL_SOURCE."home_value_numeric","away_value_numeric" = DBT_INTERNAL_SOURCE."away_value_numeric","advantage" = DBT_INTERNAL_SOURCE."advantage","home_percentage" = DBT_INTERNAL_SOURCE."home_percentage","away_percentage" = DBT_INTERNAL_SOURCE."away_percentage","value_difference" = DBT_INTERNAL_SOURCE."value_difference","stat_order_in_group" = DBT_INTERNAL_SOURCE."stat_order_in_group","stats_count_in_group" = DBT_INTERNAL_SOURCE."stats_count_in_group","total_statistics_count" = DBT_INTERNAL_SOURCE."total_statistics_count","periods_count" = DBT_INTERNAL_SOURCE."periods_count","groups_count" = DBT_INTERNAL_SOURCE."groups_count","statistics_in_period" = DBT_INTERNAL_SOURCE."statistics_in_period","has_numeric_values" = DBT_INTERNAL_SOURCE."has_numeric_values","is_complete_statistic" = DBT_INTERNAL_SOURCE."is_complete_statistic","file_path" = DBT_INTERNAL_SOURCE."file_path","batch_id" = DBT_INTERNAL_SOURCE."batch_id","original_data" = DBT_INTERNAL_SOURCE."original_data","ingestion_timestamp" = DBT_INTERNAL_SOURCE."ingestion_timestamp","created_at" = DBT_INTERNAL_SOURCE."created_at","updated_at" = DBT_INTERNAL_SOURCE."updated_at"
    

    when not matched then insert
        ("match_id", "period", "group_name", "statistic_key", "tournament_id", "season_id", "statistic_name", "statistics_type", "value_type", "render_type", "compare_code", "home_value_text", "away_value_text", "home_value_numeric", "away_value_numeric", "advantage", "home_percentage", "away_percentage", "value_difference", "stat_order_in_group", "stats_count_in_group", "total_statistics_count", "periods_count", "groups_count", "statistics_in_period", "has_numeric_values", "is_complete_statistic", "file_path", "batch_id", "original_data", "ingestion_timestamp", "created_at", "updated_at")
    values
        ("match_id", "period", "group_name", "statistic_key", "tournament_id", "season_id", "statistic_name", "statistics_type", "value_type", "render_type", "compare_code", "home_value_text", "away_value_text", "home_value_numeric", "away_value_numeric", "advantage", "home_percentage", "away_percentage", "value_difference", "stat_order_in_group", "stats_count_in_group", "total_statistics_count", "periods_count", "groups_count", "statistics_in_period", "has_numeric_values", "is_complete_statistic", "file_path", "batch_id", "original_data", "ingestion_timestamp", "created_at", "updated_at")


  