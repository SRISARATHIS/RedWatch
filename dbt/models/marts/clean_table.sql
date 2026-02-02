{{ config(
    materialized='incremental',
    unique_key='raw_id'
) }}
WITH src AS (

  SELECT
    *
  FROM {{ ref('stg_redset_events') }}

  {% if is_incremental() %}
    WHERE raw_id > (SELECT COALESCE(MAX(raw_id), 0) FROM {{ this }})
  {% endif %}

),
enriched AS (

  SELECT
    raw_id,
    instance_id,
    user_id,
    database_id,
    query_id,
    arrival_timestamp,
    DATE_TRUNC('minute', arrival_timestamp) AS minute_ts,
    COALESCE(compile_duration_ms, 0)   AS compile_duration_ms,
    COALESCE(queue_duration_ms, 0)     AS queue_duration_ms,
    COALESCE(execution_duration_ms, 0) AS execution_duration_ms,
    COALESCE(mbytes_scanned, 0)        AS mbytes_scanned,
    COALESCE(mbytes_spilled, 0)        AS mbytes_spilled,
    feature_fingerprint,
    (
      feature_fingerprint IS NULL
      OR feature_fingerprint = ''
      OR UPPER(feature_fingerprint) IN ('UNKNOWN', 'UNKNOWN_FINGERPRINT')
    ) AS is_unknown_fingerprint,
    was_aborted,
    was_cached,
    query_type,
    num_permanent_tables_accessed,
    num_external_tables_accessed,
    num_system_tables_accessed,
    cluster_size AS cluster_size_raw,
    has_read,
    has_write,
    arrival_timestamp
      + (COALESCE(execution_duration_ms, 0)::double precision * INTERVAL '1 millisecond')
      AS query_end_timestamp,
    CASE
      WHEN COALESCE(num_external_tables_accessed, 0) > 0 THEN 'External (Data Lake)'
      WHEN COALESCE(num_system_tables_accessed, 0) > 0 THEN 'System Internal'
      WHEN COALESCE(num_permanent_tables_accessed, 0) > 0 THEN 'Core Warehousing'
      ELSE 'Metadata only'
    END AS access_scope,
    CASE
      WHEN COALESCE(num_system_tables_accessed, 0) > 0 THEN TRUE
      WHEN query_type IS NOT NULL AND LOWER(query_type) IN (
        'analyze','vacuum','commit','rollback','begin','set','show','describe','explain'
      ) THEN TRUE
      ELSE FALSE
    END AS workload_is_system_metadata

  FROM src
),
instance_median AS (
  SELECT
    instance_id,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY cluster_size_raw) AS median_cluster_size
  FROM enriched
  WHERE cluster_size_raw IS NOT NULL
  GROUP BY 1
),

filled AS (
  SELECT
    e.*,
    m.median_cluster_size,
    COUNT(e.cluster_size_raw) FILTER (WHERE e.cluster_size_raw IS NOT NULL)
      OVER (PARTITION BY e.instance_id ORDER BY e.arrival_timestamp, e.raw_id) AS ff_grp,
    COUNT(e.cluster_size_raw) FILTER (WHERE e.cluster_size_raw IS NOT NULL)
      OVER (PARTITION BY e.instance_id ORDER BY e.arrival_timestamp DESC, e.raw_id DESC) AS bf_grp
  FROM enriched e
  LEFT JOIN instance_median m
    ON e.instance_id = m.instance_id
),

cluster_imputed AS (
  SELECT
    f.*,
    MAX(f.cluster_size_raw) OVER (PARTITION BY f.instance_id, f.ff_grp) AS cluster_size_ff,
    MAX(f.cluster_size_raw) OVER (PARTITION BY f.instance_id, f.bf_grp) AS cluster_size_bf
  FROM filled f
),
final AS (

  SELECT
    ci.raw_id,
    ci.instance_id,
    ci.user_id,
    ci.database_id,
    ci.query_id,
    ci.arrival_timestamp,
    ci.minute_ts,
    ci.compile_duration_ms,
    ci.queue_duration_ms,
    ci.execution_duration_ms,
    ci.mbytes_scanned,
    ci.mbytes_spilled,
    ci.feature_fingerprint,
    ci.is_unknown_fingerprint,
    ci.was_aborted,
    ci.was_cached,
    ci.query_type,
    ci.num_permanent_tables_accessed,
    ci.num_external_tables_accessed,
    ci.num_system_tables_accessed,
    COALESCE(ci.cluster_size_ff, ci.cluster_size_bf, ci.median_cluster_size, 1)::numeric
      AS cluster_size_clean,
    ci.has_read,
    ci.has_write,
    ci.query_end_timestamp,
    ci.access_scope,
    ci.workload_is_system_metadata,
    CASE
      WHEN ci.workload_is_system_metadata THEN 'System/Metadata'
      WHEN COALESCE(ci.has_read, FALSE) = TRUE AND COALESCE(ci.has_write, FALSE) = TRUE  THEN 'Read/Write'
      WHEN COALESCE(ci.has_read, FALSE) = TRUE AND COALESCE(ci.has_write, FALSE) = FALSE THEN 'Read only'
      WHEN COALESCE(ci.has_read, FALSE) = FALSE AND COALESCE(ci.has_write, FALSE) = TRUE THEN 'Write only'
      ELSE 'Unknown/Other'
    END AS workload_classification,
    CASE
      WHEN ci.was_cached = 1 THEN 0.05
      WHEN ci.was_cached <> 1 AND ci.was_aborted = 1 THEN 0.10
      ELSE
        LEAST(
          1.0,
          GREATEST(
            0.05,
            (
              0.4 * LEAST(COALESCE(ci.execution_duration_ms, 0) / 300000.0, 1.0)
            + 0.4 * LEAST(COALESCE(ci.mbytes_scanned, 0) / 10240.0, 1.0)
            + 0.2 * LEAST(COALESCE(ci.mbytes_spilled, 0) / 1024.0, 1.0)
            )
          )
        )
    END AS heavy_unit

  FROM cluster_imputed ci
)

SELECT * FROM final

