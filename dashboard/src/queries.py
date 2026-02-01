import os

def schema() -> str:
    return os.getenv("DBT_SCHEMA", "analytics_analytics")

def T(name: str) -> str:
    return f"{schema()}.{name}"

def _window_from_latest(table: str, tcol: str, win: int) -> str:
    win = int(win)
    return f"""
    WITH latest AS (
      SELECT MAX({tcol}) AS max_ts
      FROM {table}
    )
    SELECT *
    FROM {table}
    WHERE {tcol} >= (SELECT max_ts FROM latest) - (INTERVAL '1 minute' * {win})
    ORDER BY {tcol} ASC;
    """

def shadow_cost_window(tcol: str, win: int) -> str:
    return _window_from_latest(T("kpi_minute_shadow_cost"), tcol, win)

def cluster_workload_window(tcol: str, win: int) -> str:
    return _window_from_latest(T("kpi_minute_cluster_workload"), tcol, win)

def cluster_concurrency_window(tcol: str, win: int) -> str:
    return _window_from_latest(T("kpi_minute_concurrency_cluster"), tcol, win)

def leaderboard_15m() -> str:
    return f"SELECT * FROM {T('kpi_leaderboard_15m')};"

def predator_15m() -> str:
    return f"SELECT * FROM {T('kpi_resource_predator_15m')};"


# ---------------------------------------------------------
# Query Efficiency (UPDATED to use kpi_minute_query_efficiency)
# ---------------------------------------------------------
def efficiency_window(tcol: str, win: int) -> str:
    """
    Returns per-minute aggregated query efficiency metrics.

    Source table: kpi_minute_query_efficiency (from your screenshot)
    We aggregate across instance_id (and kpi_key) so the UI is stable.

    Output columns (per minute):
      - minute_ts
      - queries_count
      - exec_ms_avg, queue_ms_avg, compile_ms_avg  (weighted by queries_count)
      - scanned_mb_sum, spilled_mb_sum
      - cached_queries, aborted_queries
      - spill_to_scan_ratio
      - heavy_units_sum, heavy_unit_avg (weighted)
    """
    win = int(win)
    table = T("kpi_minute_query_efficiency")

    return f"""
    WITH latest AS (
      SELECT MAX({tcol}) AS max_ts
      FROM {table}
      WHERE {tcol} IS NOT NULL
    ),
    base AS (
      SELECT *
      FROM {table}
      WHERE {tcol} >= (SELECT max_ts FROM latest) - (INTERVAL '1 minute' * {win})
        AND {tcol} IS NOT NULL
    )
    SELECT
      {tcol} AS minute_ts,

      -- volume
      SUM(COALESCE(queries_count, 0))::bigint AS queries_count,

      -- Weighted averages (avg * count) / total_count
      CASE
        WHEN SUM(COALESCE(queries_count, 0)) > 0
        THEN (SUM(COALESCE(exec_ms_avg, 0) * COALESCE(queries_count, 0))
              / NULLIF(SUM(COALESCE(queries_count, 0)), 0))::double precision
        ELSE 0
      END AS exec_ms_avg,

      CASE
        WHEN SUM(COALESCE(queries_count, 0)) > 0
        THEN (SUM(COALESCE(queue_ms_avg, 0) * COALESCE(queries_count, 0))
              / NULLIF(SUM(COALESCE(queries_count, 0)), 0))::double precision
        ELSE 0
      END AS queue_ms_avg,

      CASE
        WHEN SUM(COALESCE(queries_count, 0)) > 0
        THEN (SUM(COALESCE(compile_ms_sum, 0))
              / NULLIF(SUM(COALESCE(queries_count, 0)), 0))::double precision
        ELSE 0
      END AS compile_ms_avg,

      -- data movement
      SUM(COALESCE(scanned_mb_sum, 0))::double precision AS scanned_mb_sum,
      SUM(COALESCE(spilled_mb_sum, 0))::double precision AS spilled_mb_sum,

      CASE
        WHEN SUM(COALESCE(scanned_mb_sum, 0)) > 0
        THEN (SUM(COALESCE(spilled_mb_sum, 0)) / NULLIF(SUM(COALESCE(scanned_mb_sum, 0)), 0))::double precision
        ELSE 0
      END AS spill_to_scan_ratio,

      -- outcomes
      SUM(COALESCE(cached_queries, 0))::bigint  AS cached_queries,
      SUM(COALESCE(aborted_queries, 0))::bigint AS aborted_queries,

      -- compute pressure
      SUM(COALESCE(heavy_units_sum, 0))::double precision AS heavy_units_sum,
      CASE
        WHEN SUM(COALESCE(queries_count, 0)) > 0
        THEN (SUM(COALESCE(heavy_unit_avg, 0) * COALESCE(queries_count, 0))
              / NULLIF(SUM(COALESCE(queries_count, 0)), 0))::double precision
        ELSE 0
      END AS heavy_unit_avg

    FROM base
    GROUP BY 1
    ORDER BY 1 ASC;
    """
