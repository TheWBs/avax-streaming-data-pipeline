import asyncio
import json
import os
import time
from typing import Optional

import websockets
from confluent_kafka import Producer


SYMBOL = os.getenv("SYMBOL", "avaxusdt").lower()
TOPIC = os.getenv("KAFKA_TOPIC", "binance_avaxusdt_aggtrades")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# Binance Spot WS endpoint for aggTrade
WS_URL = os.getenv(
    "BINANCE_WS_URL",
    f"wss://stream.binance.com:9443/ws/{SYMBOL}@aggTrade",
)

CLIENT_ID = os.getenv("CLIENT_ID", "producer-avax")


def make_kafka_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "client.id": CLIENT_ID,
            # good defaults for a learning project:
            "acks": "1",
            "enable.idempotence": False,  # keep simple
            "message.timeout.ms": 10000,
        }
    )


def delivery_report(err, msg) -> None:
    # Called once per message when Kafka confirms delivery (or fails)
    if err is not None:
        print(f"[kafka] delivery failed: {err}")
    # else: keep quiet to avoid spamming logs


async def run_forever() -> None:
    producer = make_kafka_producer()
    backoff_sec = 1

    while True:
        try:
            print(f"[ws] connecting: {WS_URL}")
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                print("[ws] connected")
                backoff_sec = 1

                while True:
                    raw = await ws.recv()  # string (JSON)
                    ingest_ts_ms = int(time.time() * 1000)

                    # Wrap the Binance message so we can add ingest timestamp
                    # Keep the original payload intact under "data"
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        # skip malformed messages
                        continue

                    out = {
                        "ingest_ts_ms": ingest_ts_ms,
                        "source": "binance_ws",
                        "stream": f"{SYMBOL}@aggTrade",
                        "data": data,
                    }

                    producer.produce(
                        TOPIC,
                        value=json.dumps(out).encode("utf-8"),
                        on_delivery=delivery_report,
                    )
                    producer.poll(0)  # let callbacks run
        except Exception as e:
            print(f"[ws] error: {e}")
            print(f"[ws] reconnecting in {backoff_sec}s...")
            await asyncio.sleep(backoff_sec)
            backoff_sec = min(backoff_sec * 2, 30)


if __name__ == "__main__":
    asyncio.run(run_forever())