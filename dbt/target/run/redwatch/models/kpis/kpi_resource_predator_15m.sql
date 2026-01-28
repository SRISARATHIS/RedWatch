
      
        
            delete from "redset_db"."analytics_analytics"."kpi_resource_predator_15m"
            where (
                pred_key) in (
                select (pred_key)
                from "kpi_resource_predator_15m__dbt_tmp114339915384"
            );

        
    

    insert into "redset_db"."analytics_analytics"."kpi_resource_predator_15m" ("pred_key", "window_start", "window_end", "predator_type", "predator_value", "rank_position", "queries_count", "heavy_units_sum", "total_heavy_units", "cost_share_pct")
    (
        select "pred_key", "window_start", "window_end", "predator_type", "predator_value", "rank_position", "queries_count", "heavy_units_sum", "total_heavy_units", "cost_share_pct"
        from "kpi_resource_predator_15m__dbt_tmp114339915384"
    )
  