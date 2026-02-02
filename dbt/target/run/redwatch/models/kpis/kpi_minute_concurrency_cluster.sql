
      
        
            delete from "redset_db"."analytics"."kpi_minute_concurrency_cluster"
            where (
                kpi_key) in (
                select (kpi_key)
                from "kpi_minute_concurrency_cluster__dbt_tmp232528172468"
            );

        
    

    insert into "redset_db"."analytics"."kpi_minute_concurrency_cluster" ("kpi_key", "minute_ts", "instance_id", "active_queries", "started_queries", "ended_queries", "cluster_size_clean_max", "query_pressure")
    (
        select "kpi_key", "minute_ts", "instance_id", "active_queries", "started_queries", "ended_queries", "cluster_size_clean_max", "query_pressure"
        from "kpi_minute_concurrency_cluster__dbt_tmp232528172468"
    )
  