import os
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from kafka import KafkaConsumer


BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "REDSET")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "redset_snowflake_sink")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "1000"))

# Safety: avoid infinite memory growth if messages are huge / ingestion paused
MAX_BUFFER_SIZE = int(os.getenv("MAX_BUFFER_SIZE", str(BATCH_SIZE * 5)))


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
        enable_auto_commit=False,  # commit only when our "sink" succeeded
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        max_poll_records=BATCH_SIZE,
    )


def enrich_event(msg, payload: Dict[str, Any]) -> Dict[str, Any]:
    payload["_ingest_ts_utc"] = datetime.now(timezone.utc).isoformat()
    payload["_kafka_topic"] = msg.topic
    payload["_kafka_partition"] = msg.partition
    payload["_kafka_offset"] = msg.offset
    payload["_kafka_key"] = msg.key
    return payload


def main() -> None:
    consumer = make_consumer()
    buffer: List[Dict[str, Any]] = []

    print(f"[consumer_base] bootstrap={BOOTSTRAP} topic={TOPIC} group_id={GROUP_ID}")
    print("[consumer_base] buffering messages and committing offsets per batch...")

    while True:
        records = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)
        got_any = False

        for _tp, msgs in records.items():
            for msg in msgs:
                got_any = True
                payload = safe_json_loads(msg.value)
                if payload is None:
                    # In production: send to DLQ (dead-letter topic)
                    continue
                buffer.append(enrich_event(msg, payload))

        # Backpressure guard
        if len(buffer) > MAX_BUFFER_SIZE:
            print(f"[consumer_base][WARN] buffer exceeded MAX_BUFFER_SIZE={MAX_BUFFER_SIZE}. "
                  f"Dropping oldest {len(buffer) - MAX_BUFFER_SIZE} events to protect memory.")
            buffer = buffer[-MAX_BUFFER_SIZE:]

        if len(buffer) >= BATCH_SIZE:
            # In Step 2, this is where we flush to Snowflake.
            first = buffer[0]
            last = buffer[-1]
            print(
                f"[consumer_base] buffered {len(buffer)} events | "
                f"offset range: {first['_kafka_partition']}:{first['_kafka_offset']} -> "
                f"{last['_kafka_partition']}:{last['_kafka_offset']}"
            )

            # Simulate "sink success" then commit offsets
            consumer.commit()
            buffer.clear()

        if not got_any:
            time.sleep(0.05)


if __name__ == "__main__":
    main()