import os

def schema() -> str:
    return os.getenv("DBT_SCHEMA", "analytics_analytics")

def T(name: str) -> str:
    return f"{schema()}.{name}"

def _window(table: str, tcol: str, win: int) -> str:
    win = int(win)
    return f"""
    SELECT *
    FROM {table}
    WHERE {tcol} >= NOW() - (INTERVAL '1 minute' * {win})
    ORDER BY {tcol} ASC;
    """


def shadow_cost_window(tcol: str, win: int) -> str:
    win = int(win)
    table = T("kpi_minute_shadow_cost")
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



def efficiency_window(tcol: str, win: int) -> str:
    win = int(win)
    table = T("kpi_minute_query_efficiency")

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


def cluster_workload_window(tcol: str, win: int) -> str:
    return _window_from_latest(T("kpi_minute_cluster_workload"), tcol, win)

def cluster_concurrency_window(tcol: str, win: int) -> str:
    return _window_from_latest(T("kpi_minute_concurrency_cluster"), tcol, win)

def leaderboard_15m() -> str:
    return f"SELECT * FROM {T('kpi_leaderboard_15m')};"

def predator_15m() -> str:
    return f"SELECT * FROM {T('kpi_resource_predator_15m')};"

def clean_efficiency_window(tcol: str, win: int) -> str:
    """
    Aggregated query efficiency metrics over a rolling window.

    clean_table columns assumed:
      - minute_ts
      - mbytes_spilled
      - mbytes_scanned
      - was_cached  (0/1)
      - was_aborted (0/1)
    """
    win = int(win)
    table = T("clean_table")

    return f"""
    SELECT
      {tcol} AS minute_ts,

      SUM(COALESCE(mbytes_spilled, 0))::double precision AS spilled_mb_sum,
      SUM(COALESCE(mbytes_scanned, 0))::double precision AS scanned_mb_sum,

      CASE
        WHEN SUM(COALESCE(mbytes_scanned, 0)) > 0
        THEN SUM(COALESCE(mbytes_spilled, 0))::double precision
             / NULLIF(SUM(COALESCE(mbytes_scanned, 0)), 0)
        ELSE 0
      END AS spill_to_scan_ratio,

      SUM(COALESCE(was_cached, 0))::bigint  AS cached_count,
      SUM(COALESCE(was_aborted, 0))::bigint AS aborted_count,

      COUNT(*)::bigint AS total_rows
    FROM {table}
    WHERE {tcol} >= NOW() - (INTERVAL '1 minute' * {win})
      AND {tcol} IS NOT NULL
    GROUP BY 1
    ORDER BY 1 ASC;
    """
