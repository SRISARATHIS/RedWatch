import os
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import math

from kafka import KafkaConsumer
import psycopg2


BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "REDSET")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "redset_postgres_sink")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "1000"))

# Postgres env vars
PG_DSN = os.getenv("PG_DSN", "postgresql://redset:redset@postgres:5432/redset_db")
PG_TABLE = os.getenv("PG_RAW_TABLE", "redset_events")


def sanitize_for_json(x: Any) -> Any:
    if isinstance(x, float):
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    if isinstance(x, dict):
        return {k: sanitize_for_json(v) for k, v in x.items()}
    if isinstance(x, list):
        return [sanitize_for_json(v) for v in x]
    return x

def safe_json_loads(b: Optional[bytes]) -> Optional[Dict[str, Any]]:
    if not b:
        return None
    try:
        return json.loads(b.decode("utf-8"))
    except Exception:
        return None


def make_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=False,  # commit only after Postgres insert succeeds
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        max_poll_records=BATCH_SIZE,
    )


def pg_connect():
    # autocommit False: we commit only after batch insert success
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = False
    return conn


def ensure_table(conn) -> None:
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {PG_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      event JSONB NOT NULL,
      ingest_ts TIMESTAMPTZ NOT NULL,
      kafka_topic TEXT NOT NULL,
      kafka_partition INT NOT NULL,
      kafka_offset BIGINT NOT NULL,
      kafka_key TEXT
    );
    """
    cur = conn.cursor()
    try:
        cur.execute(ddl)

        # add unique constraint if missing (safe to run repeatedly)
        cur.execute(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'uq_{PG_TABLE}_kafka'
            ) THEN
                EXECUTE 'ALTER TABLE {PG_TABLE}
                         ADD CONSTRAINT uq_{PG_TABLE}_kafka
                         UNIQUE (kafka_topic, kafka_partition, kafka_offset)';
            END IF;
        END $$;
        """)

        conn.commit()
    finally:
        cur.close()



def enrich_event(msg, payload: Dict[str, Any]) -> Dict[str, Any]:
    payload["_ingest_ts_utc"] = datetime.now(timezone.utc).isoformat()
    payload["_kafka_topic"] = msg.topic
    payload["_kafka_partition"] = msg.partition
    payload["_kafka_offset"] = msg.offset
    payload["_kafka_key"] = msg.key
    return payload


def to_rows(events: List[Dict[str, Any]]) -> List[Tuple[str, str, str, int, int, Optional[str]]]:
    """
    Returns rows for INSERT:
      event_json, ingest_ts, topic, partition, offset, key
    """
    rows = []
    for e in events:
        rows.append(
            (
                json.dumps(sanitize_for_json(e), separators=(",", ":"), allow_nan=False),  # compact JSON string
                e.get("_ingest_ts_utc"),
                e.get("_kafka_topic"),
                int(e.get("_kafka_partition", 0)),
                int(e.get("_kafka_offset", 0)),
                e.get("_kafka_key"),
            )
        )
    return rows


def flush_to_postgres(conn, events: List[Dict[str, Any]]) -> None:
    sql = f"""
        INSERT INTO {PG_TABLE}
            (event, ingest_ts, kafka_topic, kafka_partition, kafka_offset, kafka_key)
        VALUES
            (%s::jsonb, %s::timestamptz, %s, %s, %s, %s)
        ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
    """

    rows = to_rows(events)

    cur = conn.cursor()
    try:
        cur.executemany(sql, rows)
        conn.commit()
    finally:
        cur.close()


def main() -> None:
    print(f"[consumer_pg] bootstrap={BOOTSTRAP} topic={TOPIC} group_id={GROUP_ID}")
    print(f"[consumer_pg] target={PG_TABLE} dsn={PG_DSN}")

    consumer = make_consumer()
    conn = pg_connect()
    ensure_table(conn)

    buffer: List[Dict[str, Any]] = []
    backoff_seconds = 2

    FLUSH_SECONDS = float(os.getenv("FLUSH_SECONDS", "2"))
    last_flush = time.time()

    while True:
        records = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)
        got_any = False

        for _tp, msgs in records.items():
            for msg in msgs:
                got_any = True
                payload = safe_json_loads(msg.value)
                if payload is None:
                    continue
                buffer.append(enrich_event(msg, payload))

        # ✅ NEW: flush on size OR time
        if buffer and (
            len(buffer) >= BATCH_SIZE
            or (time.time() - last_flush) >= FLUSH_SECONDS
        ):
            try:
                flush_to_postgres(conn, buffer)

                # Commit Kafka offsets only after Postgres insert success
                consumer.commit()

                first = buffer[0]
                last = buffer[-1]
                print(
                    f"[consumer_pg] inserted {len(buffer)} events | "
                    f"offset range: {first['_kafka_partition']}:{first['_kafka_offset']} -> "
                    f"{last['_kafka_partition']}:{last['_kafka_offset']}"
                )

                buffer.clear()
                last_flush = time.time()
                backoff_seconds = 2

            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass

                print(f"[consumer_pg][ERROR] insert failed: {e}")
                print(f"[consumer_pg] retrying in {backoff_seconds}s (offsets not committed)")
                time.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 30)

                try:
                    conn.close()
                except Exception:
                    pass
                conn = pg_connect()
                ensure_table(conn)

        if not got_any:
            time.sleep(0.05)



if __name__ == "__main__":
    main()
