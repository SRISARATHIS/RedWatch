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
  SELECT
    COALESCE(MAX(minute_ts), TIMESTAMP '1970-01-01') AS max_clean_minute
  FROM {{ ref('clean_table') }}
),

base AS (
  SELECT
    minute_ts,
    instance_id,
    workload_classification,
    access_scope,
    query_type,

    heavy_unit,
    mbytes_scanned,
    mbytes_spilled,
    execution_duration_ms,
    queue_duration_ms,
    cluster_size_clean,
    raw_id

  FROM {{ ref('clean_table') }}

  WHERE minute_ts <= (SELECT max_clean_minute FROM watermark) - INTERVAL '10 minutes'

  {% if is_incremental() %}
    AND minute_ts >= (SELECT max_minute_ts FROM bounds) - INTERVAL '30 minutes'
  {% endif %}
),

agg AS (
  SELECT
    minute_ts,
    instance_id,
    workload_classification,
    access_scope,
    query_type,

    COUNT(*) AS queries_count,
    SUM(heavy_unit) AS heavy_units_sum,

    SUM(COALESCE(mbytes_scanned, 0)) AS scanned_mb_sum,
    SUM(COALESCE(mbytes_spilled, 0)) AS spilled_mb_sum,

    SUM(COALESCE(execution_duration_ms, 0)) AS exec_ms_sum,
    SUM(COALESCE(queue_duration_ms, 0)) AS queue_ms_sum,

    MAX(cluster_size_clean) AS cluster_size_clean_max,
    MAX(raw_id) AS source_max_raw_id

  FROM base
  GROUP BY 1,2,3,4,5
)

SELECT
  MD5(
    CAST(minute_ts AS VARCHAR) || '-' ||
    CAST(instance_id AS VARCHAR) || '-' ||
    COALESCE(workload_classification, 'UNKNOWN') || '-' ||
    COALESCE(access_scope, 'UNKNOWN') || '-' ||
    COALESCE(query_type, 'UNKNOWN')
  ) AS kpi_key,

  minute_ts,
  instance_id,
  workload_classification,
  access_scope,
  query_type,

  queries_count,
  heavy_units_sum,

  scanned_mb_sum,
  spilled_mb_sum,

  exec_ms_sum,
  queue_ms_sum,

  cluster_size_clean_max,
  source_max_raw_id

FROM agg

