import os
import time
import json
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

# ---------- ENV ----------
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DATABASE = os.getenv("POSTGRES_DATABASE", "redset_db")
PG_USER = os.getenv("POSTGRES_USER", "redset")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "redset")

PG_RAW_TABLE = os.getenv("PG_RAW_TABLE", "public.redset_events")
PG_STATE_TABLE = os.getenv("PG_STATE_TABLE", "cleaner_state")
PG_CLEAN_TABLE = os.getenv("PG_CLEAN_TABLE", "redset_clean_tables")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "2"))
CLEANER_NAME = os.getenv("CLEANER_NAME", "cleaner_v1")


PG_DSN = os.getenv("PG_DSN")

def get_conn():
    if PG_DSN:
        return psycopg2.connect(PG_DSN)
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
    )

def ensure_tables():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {PG_STATE_TABLE} (
              cleaner_name TEXT PRIMARY KEY,
              last_raw_id  BIGINT NOT NULL DEFAULT 0,
              updated_ts  TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """)

            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {PG_CLEAN_TABLE} (
              raw_id BIGINT PRIMARY KEY,

              instance_id BIGINT,
              user_id BIGINT,
              database_id BIGINT,
              query_id BIGINT,

              arrival_timestamp TIMESTAMPTZ,

              compile_duration_ms NUMERIC NOT NULL DEFAULT 0,
              queue_duration_ms   NUMERIC NOT NULL DEFAULT 0,
              execution_duration_ms NUMERIC NOT NULL DEFAULT 0,

              feature_fingerprint TEXT,
              was_aborted BOOLEAN,
              was_cached  BOOLEAN,

              query_type TEXT,

              num_permanent_tables_accessed NUMERIC NOT NULL DEFAULT 0,
              num_external_tables_accessed  NUMERIC NOT NULL DEFAULT 0,
              num_system_tables_accessed    NUMERIC NOT NULL DEFAULT 0,

              mbytes_scanned NUMERIC NOT NULL DEFAULT 0,
              mbytes_spilled NUMERIC NOT NULL DEFAULT 0,

              num_joins NUMERIC NOT NULL DEFAULT 0,
              num_scans NUMERIC NOT NULL DEFAULT 0,
              num_aggregations NUMERIC NOT NULL DEFAULT 0,

              cluster_size_clean NUMERIC,

              workload_type TEXT,
              total_tables_accessed NUMERIC NOT NULL DEFAULT 0,
              access_scope TEXT,

              fingerprint_frequency BIGINT,
              traffic_source TEXT,

              cleaned_ts TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """)
        conn.commit()


def get_watermark() -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
            INSERT INTO {PG_STATE_TABLE} (cleaner_name, last_raw_id)
            VALUES (%s, 0)
            ON CONFLICT (cleaner_name) DO NOTHING;
            """, (CLEANER_NAME,))
            conn.commit()

            cur.execute(f"SELECT last_raw_id FROM {PG_STATE_TABLE} WHERE cleaner_name=%s;", (CLEANER_NAME,))
            row = cur.fetchone()
            return int(row[0]) if row else 0


def set_watermark(last_raw_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
            UPDATE {PG_STATE_TABLE}
            SET last_raw_id=%s, updated_ts=now()
            WHERE cleaner_name=%s;
            """, (last_raw_id, CLEANER_NAME))
        conn.commit()

def fetch_raw_batch(last_id: int, limit: int):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(f"""
                  SELECT id, event
                  FROM {PG_RAW_TABLE}
                  WHERE id > %s
                  ORDER BY id ASC
                  LIMIT %s;
                """, (last_id, limit))
                return cur.fetchall()

            except psycopg2.errors.UndefinedTable:
                # Raw table not created yet (startup ordering). Treat as "no data".
                conn.rollback()
                return []


# ---------------- SQL BUILDING: small blocks ----------------
def cte_batch_ids() -> str:
    return """
    batch_ids AS (
      SELECT raw_id FROM tmp_clean_stage
    )
    """


def cte_batch_parsed() -> str:
    # Basic parsing (assumes most keys exist; uses NULLIF to avoid '' casts)
    return """
    batch AS (
      SELECT
        s.raw_id,
        s.event,

        NULLIF(s.event->>'instance_id','')::bigint AS instance_id,
        NULLIF(s.event->>'user_id','')::bigint     AS user_id,
        NULLIF(s.event->>'database_id','')::bigint AS database_id,
        NULLIF(s.event->>'query_id','')::bigint    AS query_id,

        NULLIF(s.event->>'arrival_timestamp','')::timestamptz AS arrival_timestamp,

        COALESCE(NULLIF(s.event->>'compile_duration_ms','')::numeric, 0)   AS compile_duration_ms,
        COALESCE(NULLIF(s.event->>'queue_duration_ms','')::numeric, 0)     AS queue_duration_ms,
        COALESCE(NULLIF(s.event->>'execution_duration_ms','')::numeric, 0) AS execution_duration_ms,

        COALESCE(NULLIF(s.event->>'feature_fingerprint',''), 'UNKNOWN_FINGERPRINT') AS feature_fingerprint,

        NULLIF(s.event->>'was_aborted','')::boolean AS was_aborted,
        NULLIF(s.event->>'was_cached','')::boolean  AS was_cached,

        NULLIF(s.event->>'query_type','') AS query_type,

        COALESCE(NULLIF(s.event->>'num_permanent_tables_accessed','')::numeric, 0) AS num_permanent_tables_accessed,
        COALESCE(NULLIF(s.event->>'num_external_tables_accessed','')::numeric, 0)  AS num_external_tables_accessed,
        COALESCE(NULLIF(s.event->>'num_system_tables_accessed','')::numeric, 0)    AS num_system_tables_accessed,

        COALESCE(NULLIF(s.event->>'mbytes_scanned','')::numeric, 0) AS mbytes_scanned,
        COALESCE(NULLIF(s.event->>'mbytes_spilled','')::numeric, 0) AS mbytes_spilled,

        COALESCE(NULLIF(s.event->>'num_joins','')::numeric, 0)        AS num_joins,
        COALESCE(NULLIF(s.event->>'num_scans','')::numeric, 0)        AS num_scans,
        COALESCE(NULLIF(s.event->>'num_aggregations','')::numeric, 0) AS num_aggregations,

        NULLIF(s.event->>'cluster_size','')::numeric AS cluster_size,
        
        NULLIF(btrim(s.event->>'read_table_ids'), '') IS NOT NULL AS has_read,
        NULLIF(btrim(s.event->>'write_table_ids'), '') IS NOT NULL AS has_write



      FROM tmp_clean_stage s
    )
    """


def cte_bounds() -> str:
    return """
    bounds AS (
      SELECT instance_id, MIN(arrival_timestamp) AS min_ts, MAX(arrival_timestamp) AS max_ts
      FROM batch
      WHERE instance_id IS NOT NULL AND arrival_timestamp IS NOT NULL
      GROUP BY instance_id
    )
    """


def cte_raw_cluster_view(raw_table: str) -> str:
    # Small "view" CTE so we don't repeat JSON extraction
    return f"""
    raw_cluster AS (
      SELECT
        NULLIF(event->>'instance_id','')::bigint AS instance_id,
        NULLIF(event->>'arrival_timestamp','')::timestamptz AS arrival_timestamp,
        NULLIF(event->>'cluster_size','')::numeric AS cluster_size
      FROM {raw_table}
    )
    """


def cte_lookback() -> str:
    return """
    lookback AS (
      SELECT DISTINCT ON (r.instance_id)
        r.instance_id, r.arrival_timestamp, r.cluster_size
      FROM raw_cluster r
      JOIN bounds b ON b.instance_id = r.instance_id
      WHERE r.cluster_size IS NOT NULL AND r.arrival_timestamp < b.min_ts
      ORDER BY r.instance_id, r.arrival_timestamp DESC
    )
    """


def cte_lookahead() -> str:
    return """
    lookahead AS (
      SELECT DISTINCT ON (r.instance_id)
        r.instance_id, r.arrival_timestamp, r.cluster_size
      FROM raw_cluster r
      JOIN bounds b ON b.instance_id = r.instance_id
      WHERE r.cluster_size IS NOT NULL AND r.arrival_timestamp > b.max_ts
      ORDER BY r.instance_id, r.arrival_timestamp ASC
    )
    """


def cte_unioned_for_fill() -> str:
    return """
    unioned AS (
      SELECT raw_id, instance_id, arrival_timestamp, cluster_size FROM batch
      UNION ALL
      SELECT NULL::bigint, instance_id, arrival_timestamp, cluster_size FROM lookback
      UNION ALL
      SELECT NULL::bigint, instance_id, arrival_timestamp, cluster_size FROM lookahead
    )
    """


def cte_ffill() -> str:
    return """
    ffill AS (
      SELECT
        u.*,
        COUNT(cluster_size) OVER (
          PARTITION BY instance_id
          ORDER BY arrival_timestamp
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS grp_fwd
      FROM unioned u
    ),
    ffilled AS (
      SELECT
        f.*,
        MAX(cluster_size) OVER (PARTITION BY instance_id, grp_fwd) AS cluster_size_ffill
      FROM ffill f
    )
    """


def cte_bfill() -> str:
    return """
    bfill AS (
      SELECT
        x.*,
        COUNT(cluster_size_ffill) OVER (
          PARTITION BY instance_id
          ORDER BY arrival_timestamp DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS grp_bwd
      FROM ffilled x
    ),
    bfilled AS (
      SELECT
        y.*,
        MAX(cluster_size_ffill) OVER (PARTITION BY instance_id, grp_bwd) AS cluster_size_bfill
      FROM bfill y
    )
    """


def cte_global_median(raw_table: str) -> str:
    return f"""
    global_median AS (
      SELECT percentile_cont(0.5) WITHIN GROUP (
        ORDER BY NULLIF(event->>'cluster_size','')::numeric
      ) AS med
      FROM {raw_table}
    )
    """


def cte_cluster_clean() -> str:
    return """
    cluster_clean AS (
      SELECT
        b.raw_id,
        COALESCE(z.cluster_size_ffill, z.cluster_size_bfill, gm.med) AS cluster_size_clean
      FROM batch b
      JOIN bfilled z ON z.raw_id = b.raw_id
      CROSS JOIN global_median gm
    )
    """


def cte_fp_counts(raw_table: str) -> str:
    return f"""
    fp_counts AS (
      SELECT
        COALESCE(NULLIF(event->>'feature_fingerprint',''), 'UNKNOWN_FINGERPRINT') AS fp,
        COUNT(*)::bigint AS freq
      FROM {raw_table}
      GROUP BY 1
    )
    """


def cte_workload_access_traffic() -> str:
    # Keep derivations in one place, but readable/basic CASE statements
    return """
    derived AS (
      SELECT
        b.*,
        cc.cluster_size_clean,

        CASE
          WHEN b.has_read AND b.has_write THEN 'Read/Write'
          WHEN b.has_read THEN 'Read Only'
          WHEN b.has_write THEN 'Write Only'
          ELSE 'System/Metadata'
        END AS workload_type,

        (b.num_permanent_tables_accessed + b.num_external_tables_accessed + b.num_system_tables_accessed) AS total_tables_accessed,

        CASE
          WHEN b.num_external_tables_accessed > 0 THEN 'External (Data Lake)'
          WHEN b.num_system_tables_accessed > 0 THEN 'System Internal'
          WHEN b.num_permanent_tables_accessed > 0 THEN 'Core Warehousing'
          ELSE 'Metadata Only'
        END AS access_scope

      FROM batch b
      JOIN cluster_clean cc ON cc.raw_id = b.raw_id
    )
    """


def cte_final() -> str:
    return """
    final AS (
      SELECT
        d.raw_id,

        d.instance_id,
        d.user_id,
        d.database_id,
        d.query_id,

        d.arrival_timestamp,

        d.compile_duration_ms,
        d.queue_duration_ms,
        d.execution_duration_ms,

        d.feature_fingerprint,
        d.was_aborted,
        d.was_cached,
        d.query_type,

        d.num_permanent_tables_accessed,
        d.num_external_tables_accessed,
        d.num_system_tables_accessed,

        d.mbytes_scanned,
        d.mbytes_spilled,

        d.num_joins,
        d.num_scans,
        d.num_aggregations,

        d.cluster_size_clean,

        d.workload_type,
        d.total_tables_accessed,
        d.access_scope,

        fpc.freq AS fingerprint_frequency,

        CASE
          WHEN fpc.freq >= 100 THEN 'Automated / App'
          WHEN fpc.freq > 5 THEN 'Recurring Report'
          ELSE 'Ad-hoc / Human'
        END AS traffic_source

      FROM derived d
      LEFT JOIN fp_counts fpc ON fpc.fp = d.feature_fingerprint
    )
    """


def insert_clean(clean_table: str) -> str:
    return f"""
    INSERT INTO {clean_table} (
      raw_id,
      instance_id, user_id, database_id, query_id,
      arrival_timestamp,
      compile_duration_ms, queue_duration_ms, execution_duration_ms,
      feature_fingerprint, was_aborted, was_cached, query_type,
      num_permanent_tables_accessed, num_external_tables_accessed, num_system_tables_accessed,
      mbytes_scanned, mbytes_spilled,
      num_joins, num_scans, num_aggregations,
      cluster_size_clean,
      workload_type, total_tables_accessed, access_scope,
      fingerprint_frequency, traffic_source
    )
    SELECT
      raw_id,
      instance_id, user_id, database_id, query_id,
      arrival_timestamp,
      compile_duration_ms, queue_duration_ms, execution_duration_ms,
      feature_fingerprint, was_aborted, was_cached, query_type,
      num_permanent_tables_accessed, num_external_tables_accessed, num_system_tables_accessed,
      mbytes_scanned, mbytes_spilled,
      num_joins, num_scans, num_aggregations,
      cluster_size_clean,
      workload_type, total_tables_accessed, access_scope,
      fingerprint_frequency, traffic_source
    FROM final
    ON CONFLICT (raw_id) DO NOTHING;
    """


def build_sql(raw_table: str, clean_table: str) -> str:
    # Order matters (dependencies)
    ctes = [
        cte_batch_ids().strip(),  # not used now but kept if you later need batch-only filters
        cte_batch_parsed().strip(),
        cte_bounds().strip(),
        cte_raw_cluster_view(raw_table).strip(),
        cte_lookback().strip(),
        cte_lookahead().strip(),
        cte_unioned_for_fill().strip(),
        cte_ffill().strip(),
        cte_bfill().strip(),
        cte_global_median(raw_table).strip(),
        cte_cluster_clean().strip(),
        cte_fp_counts(raw_table).strip(),
        cte_workload_access_traffic().strip(),
        cte_final().strip(),
    ]
    return "WITH\n" + ",\n".join(ctes) + "\n" + insert_clean(clean_table)


# ---------------- CLEANER ----------------
def write_clean(rows):
    if not rows:
        return

    staged = []
    for r in rows:
        raw_id = int(r["id"])
        event = r["event"]
        if isinstance(event, str):
            event = json.loads(event)
        staged.append((raw_id, json.dumps(event)))

    sql = build_sql(PG_RAW_TABLE, PG_CLEAN_TABLE)

    with get_conn() as conn:
        with conn.cursor() as cur:
            # stage batch
            cur.execute("""
            CREATE TEMP TABLE IF NOT EXISTS tmp_clean_stage (
              raw_id BIGINT PRIMARY KEY,
              event  JSONB NOT NULL
            ) ON COMMIT DROP;
            """)
            cur.execute("TRUNCATE tmp_clean_stage;")
            cur.executemany(
                "INSERT INTO tmp_clean_stage (raw_id, event) VALUES (%s, %s);",
                staged
            )

            # transform + insert
            cur.execute(sql)

        conn.commit()


def main():
    ensure_tables()
    watermark = get_watermark()
    print(f"[{CLEANER_NAME}] starting. watermark id={watermark}")

    while True:
        rows = fetch_raw_batch(watermark, BATCH_SIZE)

        if not rows:
            time.sleep(POLL_SECONDS)
            continue

        write_clean(rows)

        watermark = int(rows[-1]["id"])
        set_watermark(watermark)
        print(f"[{CLEANER_NAME}] processed batch={len(rows)} up to id={watermark}")


if __name__ == "__main__":
    main()
