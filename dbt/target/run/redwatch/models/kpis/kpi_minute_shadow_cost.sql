
      
        
            delete from "redset_db"."analytics"."kpi_minute_shadow_cost"
            where (
                kpi_key) in (
                select (kpi_key)
                from "kpi_minute_shadow_cost__dbt_tmp232528558191"
            );

        
    

    insert into "redset_db"."analytics"."kpi_minute_shadow_cost" ("kpi_key", "minute_ts", "instance_id", "queries_count", "heavy_units_sum", "cluster_size_clean_max", "cost_units_per_min", "cost_units_per_min_per_node")
    (
        select "kpi_key", "minute_ts", "instance_id", "queries_count", "heavy_units_sum", "cluster_size_clean_max", "cost_units_per_min", "cost_units_per_min_per_node"
        from "kpi_minute_shadow_cost__dbt_tmp232528558191"
    )
  