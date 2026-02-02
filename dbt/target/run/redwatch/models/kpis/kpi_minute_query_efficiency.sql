
      
        
            delete from "redset_db"."analytics"."kpi_minute_query_efficiency"
            where (
                kpi_key) in (
                select (kpi_key)
                from "kpi_minute_query_efficiency__dbt_tmp232528207616"
            );

        
    

    insert into "redset_db"."analytics"."kpi_minute_query_efficiency" ("kpi_key", "minute_ts", "instance_id", "queries_count", "exec_ms_sum", "exec_ms_avg", "queue_ms_sum", "queue_ms_avg", "compile_ms_sum", "scanned_mb_sum", "spilled_mb_sum", "heavy_units_sum", "heavy_unit_avg", "cached_queries", "aborted_queries", "spill_to_scan_ratio")
    (
        select "kpi_key", "minute_ts", "instance_id", "queries_count", "exec_ms_sum", "exec_ms_avg", "queue_ms_sum", "queue_ms_avg", "compile_ms_sum", "scanned_mb_sum", "spilled_mb_sum", "heavy_units_sum", "heavy_unit_avg", "cached_queries", "aborted_queries", "spill_to_scan_ratio"
        from "kpi_minute_query_efficiency__dbt_tmp232528207616"
    )
  