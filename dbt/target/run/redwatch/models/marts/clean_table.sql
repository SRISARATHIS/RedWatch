
      
        
            delete from "redset_db"."analytics_analytics"."clean_table"
            where (
                raw_id) in (
                select (raw_id)
                from "clean_table__dbt_tmp114339383447"
            );

        
    

    insert into "redset_db"."analytics_analytics"."clean_table" ("raw_id", "instance_id", "user_id", "database_id", "query_id", "arrival_timestamp", "minute_ts", "compile_duration_ms", "queue_duration_ms", "execution_duration_ms", "mbytes_scanned", "mbytes_spilled", "feature_fingerprint", "is_unknown_fingerprint", "was_aborted", "was_cached", "query_type", "num_permanent_tables_accessed", "num_external_tables_accessed", "num_system_tables_accessed", "cluster_size_clean", "has_read", "has_write", "query_end_timestamp", "access_scope", "workload_is_system_metadata", "workload_classification", "heavy_unit")
    (
        select "raw_id", "instance_id", "user_id", "database_id", "query_id", "arrival_timestamp", "minute_ts", "compile_duration_ms", "queue_duration_ms", "execution_duration_ms", "mbytes_scanned", "mbytes_spilled", "feature_fingerprint", "is_unknown_fingerprint", "was_aborted", "was_cached", "query_type", "num_permanent_tables_accessed", "num_external_tables_accessed", "num_system_tables_accessed", "cluster_size_clean", "has_read", "has_write", "query_end_timestamp", "access_scope", "workload_is_system_metadata", "workload_classification", "heavy_unit"
        from "clean_table__dbt_tmp114339383447"
    )
  