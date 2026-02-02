
      
        
            delete from "redset_db"."analytics"."kpi_minute_cluster_workload"
            where (
                kpi_key) in (
                select (kpi_key)
                from "kpi_minute_cluster_workload__dbt_tmp232527991784"
            );

        
    

    insert into "redset_db"."analytics"."kpi_minute_cluster_workload" ("kpi_key", "minute_ts", "instance_id", "workload_classification", "access_scope", "query_type", "queries_count", "heavy_units_sum", "scanned_mb_sum", "spilled_mb_sum", "exec_ms_sum", "queue_ms_sum", "cluster_size_clean_max", "source_max_raw_id")
    (
        select "kpi_key", "minute_ts", "instance_id", "workload_classification", "access_scope", "query_type", "queries_count", "heavy_units_sum", "scanned_mb_sum", "spilled_mb_sum", "exec_ms_sum", "queue_ms_sum", "cluster_size_clean_max", "source_max_raw_id"
        from "kpi_minute_cluster_workload__dbt_tmp232527991784"
    )
  