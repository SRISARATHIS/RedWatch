{{ config(
    materialized='incremental',
    unique_key='kpi_key',
    incremental_strategy='delete+insert'
) }}

WITH bounds AS (

  {% if is_incremental() %}
    SELECT COALESCE(MAX(minute_ts), TIMESTAMP '1970-01-01') AS max_minute_ts
    FROM {{ this }}
  {% else %}
    SELECT TIMESTAMP '1970-01-01' AS max_minute_ts
  {% endif %}

),

watermark AS (
  SELECT COALESCE(MAX(minute_ts), TIMESTAMP '1970-01-01') AS max_clean_minute
  FROM {{ ref('clean_table') }}
),

base AS (
  SELECT
    minute_ts,
    instance_id,
    user_id,
    query_type,
    workload_classification,
    access_scope,
    feature_fingerprint,
    was_cached,
    was_aborted,
    execution_duration_ms,
    queue_duration_ms,
    compile_duration_ms,
    mbytes_scanned,
    mbytes_spilled,
    raw_id
  FROM {{ ref('clean_table') }}
  WHERE minute_ts <= (SELECT max_clean_minute FROM watermark) - INTERVAL '10 minutes'

  {% if is_incremental() %}
    AND minute_ts >= (SELECT max_minute_ts FROM bounds) - INTERVAL '30 minutes'
  {% endif %}
),

row_metrics AS (
  SELECT
    minute_ts,
    instance_id,
    user_id,
    query_type,
    workload_classification,
    access_scope,
    feature_fingerprint,
    raw_id,
    CASE
      WHEN was_cached = 1 THEN 0.05
      WHEN was_cached <> 1 AND was_aborted = 1 THEN 0.10
      ELSE
        LEAST(
          1.0,
          GREATEST(
            0.05,
            (
              0.4 * LEAST(COALESCE(execution_duration_ms, 0) / 300000.0, 1.0)
            + 0.4 * LEAST(COALESCE(mbytes_scanned, 0) / 10240.0, 1.0)
            + 0.2 * LEAST(COALESCE(mbytes_spilled, 0) / 1024.0, 1.0)
            )
          )
        )
    END AS heavy_unit
  FROM base
),

agg AS (
  SELECT
    minute_ts,
    instance_id,

    COUNT(*) AS queries_count,
    SUM(COALESCE(execution_duration_ms, 0)) AS exec_ms_sum,
    AVG(COALESCE(execution_duration_ms, 0)) AS exec_ms_avg,
    SUM(COALESCE(queue_duration_ms, 0))     AS queue_ms_sum,
    AVG(COALESCE(queue_duration_ms, 0))     AS queue_ms_avg,
    SUM(COALESCE(compile_duration_ms, 0))   AS compile_ms_sum,
    SUM(COALESCE(mbytes_scanned, 0)) AS scanned_mb_sum,
    SUM(COALESCE(mbytes_spilled, 0)) AS spilled_mb_sum,
    SUM(heavy_unit) AS heavy_units_sum,
    AVG(heavy_unit) AS heavy_unit_avg,
    SUM(CASE WHEN COALESCE(was_cached, 0) = 1 THEN 1 ELSE 0 END) AS cached_queries,
    SUM(CASE WHEN COALESCE(was_aborted, 0) = 1 THEN 1 ELSE 0 END) AS aborted_queries

  FROM (
    SELECT
      b.minute_ts,
      b.instance_id,
      b.execution_duration_ms,
      b.queue_duration_ms,
      b.compile_duration_ms,
      b.mbytes_scanned,
      b.mbytes_spilled,
      b.was_cached,
      b.was_aborted,
      rm.heavy_unit
    FROM base b
    JOIN row_metrics rm
      ON b.raw_id = rm.raw_id
  ) x
  GROUP BY 1, 2
)

SELECT
  MD5(CAST(minute_ts AS VARCHAR) || '-' || CAST(instance_id AS VARCHAR)) AS kpi_key,
  minute_ts,
  instance_id,
  queries_count,
  exec_ms_sum,
  exec_ms_avg,
  queue_ms_sum,
  queue_ms_avg,
  compile_ms_sum,
  scanned_mb_sum,
  spilled_mb_sum,
  heavy_units_sum,
  heavy_unit_avg,
  cached_queries,
  aborted_queries,
  (spilled_mb_sum::numeric / NULLIF(scanned_mb_sum, 0)) AS spill_to_scan_ratio
FROM agg

