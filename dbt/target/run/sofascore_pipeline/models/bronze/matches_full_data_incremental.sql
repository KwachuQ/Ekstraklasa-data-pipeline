
      -- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into "dwh"."bronze"."full_matches_data" as DBT_INTERNAL_DEST
        using "full_matches_data__dbt_tmp122308229962" as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.match_id = DBT_INTERNAL_DEST.match_id))

    
    when matched then update set
        "match_id" = DBT_INTERNAL_SOURCE."match_id","match_slug" = DBT_INTERNAL_SOURCE."match_slug","custom_id" = DBT_INTERNAL_SOURCE."custom_id","start_timestamp" = DBT_INTERNAL_SOURCE."start_timestamp","status_code" = DBT_INTERNAL_SOURCE."status_code","status_type" = DBT_INTERNAL_SOURCE."status_type","status_description" = DBT_INTERNAL_SOURCE."status_description","winner_code" = DBT_INTERNAL_SOURCE."winner_code","home_score_current" = DBT_INTERNAL_SOURCE."home_score_current","away_score_current" = DBT_INTERNAL_SOURCE."away_score_current","home_score_period1" = DBT_INTERNAL_SOURCE."home_score_period1","home_score_period2" = DBT_INTERNAL_SOURCE."home_score_period2","away_score_period1" = DBT_INTERNAL_SOURCE."away_score_period1","away_score_period2" = DBT_INTERNAL_SOURCE."away_score_period2","home_team_id" = DBT_INTERNAL_SOURCE."home_team_id","home_team_name" = DBT_INTERNAL_SOURCE."home_team_name","home_team_slug" = DBT_INTERNAL_SOURCE."home_team_slug","home_team_short_name" = DBT_INTERNAL_SOURCE."home_team_short_name","home_team_name_code" = DBT_INTERNAL_SOURCE."home_team_name_code","away_team_id" = DBT_INTERNAL_SOURCE."away_team_id","away_team_name" = DBT_INTERNAL_SOURCE."away_team_name","away_team_slug" = DBT_INTERNAL_SOURCE."away_team_slug","away_team_short_name" = DBT_INTERNAL_SOURCE."away_team_short_name","away_team_name_code" = DBT_INTERNAL_SOURCE."away_team_name_code","tournament_id" = DBT_INTERNAL_SOURCE."tournament_id","tournament_name" = DBT_INTERNAL_SOURCE."tournament_name","tournament_slug" = DBT_INTERNAL_SOURCE."tournament_slug","unique_tournament_id" = DBT_INTERNAL_SOURCE."unique_tournament_id","season_id" = DBT_INTERNAL_SOURCE."season_id","season_name" = DBT_INTERNAL_SOURCE."season_name","season_year" = DBT_INTERNAL_SOURCE."season_year","country_name" = DBT_INTERNAL_SOURCE."country_name","country_alpha2" = DBT_INTERNAL_SOURCE."country_alpha2","country_alpha3" = DBT_INTERNAL_SOURCE."country_alpha3","round_number" = DBT_INTERNAL_SOURCE."round_number","injury_time_2" = DBT_INTERNAL_SOURCE."injury_time_2","current_period_start_timestamp" = DBT_INTERNAL_SOURCE."current_period_start_timestamp","is_editor" = DBT_INTERNAL_SOURCE."is_editor","feed_locked" = DBT_INTERNAL_SOURCE."feed_locked","final_result_only" = DBT_INTERNAL_SOURCE."final_result_only","has_global_highlights" = DBT_INTERNAL_SOURCE."has_global_highlights","source" = DBT_INTERNAL_SOURCE."source","batch_id" = DBT_INTERNAL_SOURCE."batch_id","ingestion_timestamp" = DBT_INTERNAL_SOURCE."ingestion_timestamp","created_at" = DBT_INTERNAL_SOURCE."created_at","updated_at" = DBT_INTERNAL_SOURCE."updated_at"
    

    when not matched then insert
        ("match_id", "match_slug", "custom_id", "start_timestamp", "status_code", "status_type", "status_description", "winner_code", "home_score_current", "away_score_current", "home_score_period1", "home_score_period2", "away_score_period1", "away_score_period2", "home_team_id", "home_team_name", "home_team_slug", "home_team_short_name", "home_team_name_code", "away_team_id", "away_team_name", "away_team_slug", "away_team_short_name", "away_team_name_code", "tournament_id", "tournament_name", "tournament_slug", "unique_tournament_id", "season_id", "season_name", "season_year", "country_name", "country_alpha2", "country_alpha3", "round_number", "injury_time_2", "current_period_start_timestamp", "is_editor", "feed_locked", "final_result_only", "has_global_highlights", "source", "batch_id", "ingestion_timestamp", "created_at", "updated_at")
    values
        ("match_id", "match_slug", "custom_id", "start_timestamp", "status_code", "status_type", "status_description", "winner_code", "home_score_current", "away_score_current", "home_score_period1", "home_score_period2", "away_score_period1", "away_score_period2", "home_team_id", "home_team_name", "home_team_slug", "home_team_short_name", "home_team_name_code", "away_team_id", "away_team_name", "away_team_slug", "away_team_short_name", "away_team_name_code", "tournament_id", "tournament_name", "tournament_slug", "unique_tournament_id", "season_id", "season_name", "season_year", "country_name", "country_alpha2", "country_alpha3", "round_number", "injury_time_2", "current_period_start_timestamp", "is_editor", "feed_locked", "final_result_only", "has_global_highlights", "source", "batch_id", "ingestion_timestamp", "created_at", "updated_at")


  