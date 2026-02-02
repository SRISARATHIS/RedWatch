
      
        
            delete from "redset_db"."analytics"."kpi_leaderboard_15m"
            where (
                lb_key) in (
                select (lb_key)
                from "kpi_leaderboard_15m__dbt_tmp232527993368"
            );

        
    

    insert into "redset_db"."analytics"."kpi_leaderboard_15m" ("lb_key", "window_start", "window_end", "dimension_type", "dimension_value", "rank_position", "queries_count", "heavy_units_sum")
    (
        select "lb_key", "window_start", "window_end", "dimension_type", "dimension_value", "rank_position", "queries_count", "heavy_units_sum"
        from "kpi_leaderboard_15m__dbt_tmp232527993368"
    )
  