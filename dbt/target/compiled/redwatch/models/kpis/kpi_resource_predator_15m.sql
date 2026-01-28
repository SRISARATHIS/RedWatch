

WITH bounds AS (

  
    SELECT COALESCE(MAX(window_start), TIMESTAMP '1970-01-01') AS max_window_start
    FROM "redset_db"."analytics_analytics"."kpi_resource_predator_15m"
  

),

base AS (
  SELECT
    window_start,
    window_end,
    dimension_type,
    dimension_value,
    heavy_units_sum,
    queries_count
  FROM "redset_db"."analytics_analytics"."kpi_leaderboard_15m"
  WHERE dimension_type IN ('user', 'fingerprint')
  
    AND window_start >= (SELECT max_window_start FROM bounds) - INTERVAL '2 hours'
  
),

totals AS (
  SELECT
    window_start,
    dimension_type,
    SUM(heavy_units_sum) AS total_heavy_units
  FROM base
  GROUP BY 1,2
),

scored AS (
  SELECT
    b.window_start,
    b.window_end,
    b.dimension_type AS predator_type,
    b.dimension_value AS predator_value,
    b.queries_count,
    b.heavy_units_sum,
    t.total_heavy_units,
    (b.heavy_units_sum::numeric / NULLIF(t.total_heavy_units, 0)) AS cost_share_pct
  FROM base b
  JOIN totals t
    ON b.window_start = t.window_start
   AND b.dimension_type = t.dimension_type
),

ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY window_start, predator_type
      ORDER BY cost_share_pct DESC, heavy_units_sum DESC
    ) AS rank_position
  FROM scored
)

SELECT
  md5(CAST(window_start AS varchar) || '-' || predator_type || '-' || predator_value) AS pred_key,
  window_start,
  window_end,
  predator_type,
  predator_value,
  rank_position,
  queries_count,
  heavy_units_sum,
  total_heavy_units,
  cost_share_pct
FROM ranked
WHERE rank_position <= 10