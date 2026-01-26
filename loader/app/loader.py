import os
import time
import json
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()

# ---------- ENV ----------
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DATABASE = os.getenv("POSTGRES_DATABASE", "redset_db")
PG_USER = os.getenv("POSTGRES_USER", "redset")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "redset")

PG_RAW_TABLE = os.getenv("PG_RAW_TABLE", "public.redset_events")
PG_STATE_TABLE = os.getenv("PG_STATE_TABLE", "cleaner_state")

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
            # watermark table (same idea as your cleaner_state)
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {PG_STATE_TABLE} (
              cleaner_name TEXT PRIMARY KEY,
              last_raw_id  BIGINT NOT NULL DEFAULT 0,
              updated_ts  TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """)

            # raw table: minimal schema
            # (Assumes consumer inserts here or loader inserts here depending on your setup)
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {PG_RAW_TABLE} (
              id BIGSERIAL PRIMARY KEY,
              event JSONB NOT NULL,
              inserted_ts TIMESTAMPTZ NOT NULL DEFAULT now()
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
    # This function depends on where “raw events” currently live.
    # If your consumer already writes into PG_RAW_TABLE, then loader does nothing.
    # If you have a separate "raw ingest table" already, point PG_RAW_TABLE to that and use this.
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"""
              SELECT id, event
              FROM {PG_RAW_TABLE}
              WHERE id > %s
              ORDER BY id ASC
              LIMIT %s;
            """, (last_id, limit))
            return cur.fetchall()


def main():
    ensure_tables()
    watermark = get_watermark()
    print(f"[{CLEANER_NAME}] starting. watermark id={watermark}")

    # Loader is now just “monitor raw arrival” and advance watermark.
    # dbt will transform from raw table -> clean tables.
    while True:
        rows = fetch_raw_batch(watermark, BATCH_SIZE)

        if not rows:
            time.sleep(POLL_SECONDS)
            continue

        watermark = int(rows[-1]["id"])
        set_watermark(watermark)
        print(f"[{CLEANER_NAME}] observed batch={len(rows)} up to id={watermark}")


if __name__ == "__main__":
    main()
