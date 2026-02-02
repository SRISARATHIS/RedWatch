{{ config(
    materialized='incremental',
    unique_key='lb_key',
    incremental_strategy='delete+insert'
) }}
WITH bounds AS (
  {% if is_incremental() %}
    SELECT COALESCE(MAX(window_start), TIMESTAMP '1970-01-01') AS max_window_start
    FROM {{ this }}
  {% else %}
    SELECT TIMESTAMP '1970-01-01' AS max_window_start
  {% endif %}
),

base AS (
  SELECT
    arrival_timestamp,
    user_id,
    query_type,
    workload_classification,
    access_scope,
    feature_fingerprint,
    was_cached,
    was_aborted,
    execution_duration_ms,
    mbytes_scanned,
    mbytes_spilled,
    raw_id
  FROM {{ ref('clean_table') }}
  {% if is_incremental() %}
    WHERE arrival_timestamp >= (SELECT max_window_start FROM bounds) - INTERVAL '2 hours'
  {% endif %}
),
with_window AS (
  SELECT
    (
      date_trunc('hour', arrival_timestamp)
      + (floor(extract(minute from arrival_timestamp)::numeric / 15) * interval '15 minutes')
    ) AS window_start,

    (
      date_trunc('hour', arrival_timestamp)
      + (floor(extract(minute from arrival_timestamp)::numeric / 15) * interval '15 minutes')
      + interval '15 minutes'
    ) AS window_end,
    user_id,
    query_type,
    workload_classification,
    access_scope,
    feature_fingerprint,
    CASE
      WHEN was_cached = 1 THEN 0.05
      WHEN was_cached <> 1 AND was_aborted = 1 THEN 0.10
      ELSE LEAST(
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

unioned AS (
  SELECT window_start, window_end, 'user' AS dimension_type, CAST(user_id AS varchar) AS dimension_value, heavy_unit
  FROM with_window
  WHERE user_id IS NOT NULL
  UNION ALL
  SELECT window_start, window_end, 'query_type', COALESCE(query_type, 'UNKNOWN'), heavy_unit
  FROM with_window
  UNION ALL
  SELECT window_start, window_end, 'workload', COALESCE(workload_classification, 'UNKNOWN'), heavy_unit
  FROM with_window
  UNION ALL
  SELECT window_start, window_end, 'access_scope', COALESCE(access_scope, 'UNKNOWN'), heavy_unit
  FROM with_window
  UNION ALL
  SELECT window_start, window_end, 'fingerprint', COALESCE(feature_fingerprint, 'UNKNOWN'), heavy_unit
  FROM with_window
),

agg AS (
  SELECT
    window_start,
    window_end,
    dimension_type,
    dimension_value,
    COUNT(*) AS queries_count,
    SUM(heavy_unit) AS heavy_units_sum
  FROM unioned
  GROUP BY 1,2,3,4
),

ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY window_start, dimension_type
      ORDER BY heavy_units_sum DESC
    ) AS rank_position
  FROM agg
)

SELECT
  md5(
    CAST(window_start AS varchar)
    || '-' || dimension_type
    || '-' || dimension_value
  ) AS lb_key,
  window_start,
  window_end,
  dimension_type,
  dimension_value,
  rank_position,
  queries_count,
  heavy_units_sum
FROM ranked
WHERE rank_position <= 10

