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
    raw_id,
    instance_id,
    minute_ts AS start_minute,
    DATE_TRUNC('minute', query_end_timestamp) AS end_minute,
    cluster_size_clean
  FROM {{ ref('clean_table') }}
  WHERE minute_ts <= (SELECT max_clean_minute FROM watermark) - INTERVAL '10 minutes'

  {% if is_incremental() %}
    AND minute_ts >= (SELECT max_minute_ts FROM bounds) - INTERVAL '30 minutes'
  {% endif %}
),

expanded AS (
  SELECT
    b.instance_id,
    gs.minute_ts,
    b.raw_id,
    b.start_minute,
    b.end_minute
  FROM base b
  JOIN LATERAL generate_series(b.start_minute, b.end_minute, INTERVAL '1 minute') AS gs(minute_ts)
    ON TRUE
),

agg AS (
  SELECT
    minute_ts,
    instance_id,

    COUNT(DISTINCT raw_id) AS active_queries,
    COUNT(DISTINCT CASE WHEN start_minute = minute_ts THEN raw_id END) AS started_queries,
    COUNT(DISTINCT CASE WHEN end_minute = minute_ts THEN raw_id END) AS ended_queries
  FROM expanded
  GROUP BY 1, 2
),

cluster_size AS (
  SELECT
    minute_ts,
    instance_id,
    MAX(COALESCE(cluster_size_clean, 0)) AS cluster_size_clean_max
  FROM {{ ref('clean_table') }}
  WHERE minute_ts <= (SELECT max_clean_minute FROM watermark) - INTERVAL '10 minutes'

  {% if is_incremental() %}
    AND minute_ts >= (SELECT max_minute_ts FROM bounds) - INTERVAL '30 minutes'
  {% endif %}

  GROUP BY 1, 2
)

SELECT
  MD5(CAST(a.minute_ts AS VARCHAR) || '-' || CAST(a.instance_id AS VARCHAR)) AS kpi_key,

  a.minute_ts,
  a.instance_id,

  a.active_queries,
  a.started_queries,
  a.ended_queries,

  cs.cluster_size_clean_max,

  (a.active_queries::numeric / NULLIF(cs.cluster_size_clean_max, 0)) AS query_pressure

FROM agg a
LEFT JOIN cluster_size cs
  ON a.minute_ts = cs.minute_ts
 AND a.instance_id = cs.instance_id

