"""
Kafka Time-Warp Producer

This module replays a static Redset dataset (Parquet) into Kafka as a *simulated real-time*
event stream.
"""

import os
import json
import time
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "REDSET")
INPUT_PATH = os.getenv("INPUT_PATH", "/data/sample_0.01.parquet")
TIME_WARP_FACTOR = float(os.getenv("TIME_WARP_FACTOR", "60"))
TIMESTAMP_COL = os.getenv("TIMESTAMP_COL", "arrival_timestamp")
MAX_SLEEP_SECONDS = float(os.getenv("MAX_SLEEP_SECONDS", "0.5"))
MIN_SLEEP_SECONDS = float(os.getenv("MIN_SLEEP_SECONDS", "0.0"))

ACK_EVERY = int(os.getenv("ACK_EVERY", "500"))

def main() -> None:
    """
    Replay the dataset into Kafka in timestamp order using a configurable time warp.
    """
    print(f"[producer] bootstrap={BOOTSTRAP} topic={TOPIC}")
    print(f"[producer] input={INPUT_PATH} warp={TIME_WARP_FACTOR}x ts_col={TIMESTAMP_COL}")
    df = pd.read_parquet(INPUT_PATH)
    if TIMESTAMP_COL not in df.columns:
        raise ValueError(
            f"Timestamp column '{TIMESTAMP_COL}' not found. Available: {list(df.columns)}"
        )
    df[TIMESTAMP_COL] = pd.to_datetime(df[TIMESTAMP_COL], errors="coerce", utc=True)
    df = df.dropna(subset=[TIMESTAMP_COL]).sort_values(TIMESTAMP_COL).reset_index(drop=True)
    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=10,
    )
    prev_ts = None
    sleep_s = 0.0
    for i in range(len(df)):
        event = df.iloc[i].to_dict()
        cur_ts = event[TIMESTAMP_COL]
        sleep_s = 0.0
        if prev_ts is not None:
            real_delta = (cur_ts - prev_ts).total_seconds()
            sleep_s = max(real_delta / TIME_WARP_FACTOR, 0.0)
            sleep_s = min(max(sleep_s, MIN_SLEEP_SECONDS), MAX_SLEEP_SECONDS)
            if sleep_s > 0:
                time.sleep(sleep_s)
        key = event.get("query_id")
        key_bytes = str(key).encode("utf-8") if key is not None else None
        future = producer.send(TOPIC, key=key_bytes, value=event)
        if ACK_EVERY > 0 and i > 0 and i % ACK_EVERY == 0:
            try:
                future.get(timeout=10)
            except KafkaError as e:
                print(f"[producer] send failed at i={i}: {e}")
                raise
        prev_ts = cur_ts
        if i % 500 == 0:
            print(f"[producer] sent={i} replay_time={cur_ts} sleep={sleep_s:.3f}s")
    producer.flush()
    print("[producer] done")

if __name__ == "__main__":
    main()
