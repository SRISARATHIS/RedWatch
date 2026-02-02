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

-- ✅ closed-minute gate (same as other KPIs)
watermark AS (
  SELECT COALESCE(MAX(minute_ts), TIMESTAMP '1970-01-01') AS max_clean_minute
  FROM {{ ref('clean_table') }}
),

-- ✅ heavy units come from KPI 3
eff AS (
  SELECT
    minute_ts,
    instance_id,
    heavy_units_sum,
    queries_count
  FROM {{ ref('kpi_minute_query_efficiency') }}
  WHERE minute_ts <= (SELECT max_clean_minute FROM watermark) - INTERVAL '10 minutes'

  {% if is_incremental() %}
    AND minute_ts >= (SELECT max_minute_ts FROM bounds) - INTERVAL '30 minutes'
  {% endif %}
),

-- ✅ cluster size comes from clean_table (max seen that minute)
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
),

joined AS (
  SELECT
    e.minute_ts,
    e.instance_id,
    COALESCE(e.heavy_units_sum, 0) AS heavy_units_sum,
    COALESCE(e.queries_count, 0)   AS queries_count,
    COALESCE(cs.cluster_size_clean_max, 0) AS cluster_size_clean_max
  FROM eff e
  LEFT JOIN cluster_size cs
    ON e.minute_ts = cs.minute_ts
   AND e.instance_id = cs.instance_id
),

scored AS (
  SELECT
    *,
    -- ✅ simple “shadow cost units per minute”
    -- We keep this as “cost_units_per_min” (not dollars yet).
    (heavy_units_sum::numeric) AS cost_units_per_min,

    -- ✅ normalize for cluster size if you want a fairness metric
    (heavy_units_sum::numeric / NULLIF(cluster_size_clean_max, 0)) AS cost_units_per_min_per_node
  FROM joined
)

SELECT
  MD5(CAST(minute_ts AS VARCHAR) || '-' || CAST(instance_id AS VARCHAR)) AS kpi_key,
  minute_ts,
  instance_id,

  queries_count,
  heavy_units_sum,
  cluster_size_clean_max,

  cost_units_per_min,
  cost_units_per_min_per_node

FROM scored
