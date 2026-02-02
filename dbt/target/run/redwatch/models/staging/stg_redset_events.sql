
  create view "redset_db"."analytics"."stg_redset_events__dbt_tmp"
    
    
  as (
    

SELECT
  id AS raw_id,

  NULLIF(event->>'instance_id','')::int AS instance_id,
  NULLIF(event->>'user_id','')::int     AS user_id,
  NULLIF(event->>'database_id','')::int AS database_id,
  NULLIF(event->>'query_id','')::bigint AS query_id,

  NULLIF(event->>'cluster_size','')::numeric AS cluster_size,

  NULLIF(event->>'arrival_timestamp','')::timestamptz AS arrival_timestamp,

  COALESCE(NULLIF(event->>'compile_duration_ms','')::numeric, 0)   AS compile_duration_ms,
  COALESCE(NULLIF(event->>'queue_duration_ms','')::numeric, 0)     AS queue_duration_ms,
  COALESCE(NULLIF(event->>'execution_duration_ms','')::numeric, 0) AS execution_duration_ms,

  COALESCE(NULLIF(event->>'feature_fingerprint',''), 'UNKNOWN_FINGERPRINT') AS feature_fingerprint,

  COALESCE(NULLIF(event->>'was_aborted','')::int, 0) AS was_aborted,
  COALESCE(NULLIF(event->>'was_cached','')::int, 0)  AS was_cached,

  NULLIF(event->>'query_type','') AS query_type,

  COALESCE(NULLIF(event->>'num_permanent_tables_accessed','')::numeric, 0) AS num_permanent_tables_accessed,
  COALESCE(NULLIF(event->>'num_external_tables_accessed','')::numeric, 0)  AS num_external_tables_accessed,
  COALESCE(NULLIF(event->>'num_system_tables_accessed','')::numeric, 0)    AS num_system_tables_accessed,

  COALESCE(NULLIF(event->>'mbytes_scanned','')::numeric, 0) AS mbytes_scanned,
  COALESCE(NULLIF(event->>'mbytes_spilled','')::numeric, 0) AS mbytes_spilled,

  COALESCE(NULLIF(event->>'num_joins','')::numeric, 0)        AS num_joins,
  COALESCE(NULLIF(event->>'num_scans','')::numeric, 0)        AS num_scans,
  COALESCE(NULLIF(event->>'num_aggregations','')::numeric, 0) AS num_aggregations,

  (NULLIF(btrim(event->>'read_table_ids'), '') IS NOT NULL)  AS has_read,
  (NULLIF(btrim(event->>'write_table_ids'), '') IS NOT NULL) AS has_write

FROM public.redset_events
WHERE event IS NOT NULL
  );