"""
Binance WebSocket → Kafka producer.

Connects to the Binance combined stream for BTC/ETH/SOL trades,
parses each trade event, and publishes to the `price-events` topic.
"""

import asyncio
import json
import logging
import signal
import sys
import time
from typing import Optional

import websockets
from confluent_kafka import Producer, KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("binance_producer")

BINANCE_WS_URL = (
    "wss://stream.binance.com:9443/stream"
    "?streams=btcusdt@trade/ethusdt@trade/solusdt@trade"
)
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "price-events"
RECONNECT_BACKOFF_S = 5


def build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "acks": "all",
            "linger.ms": 5,
            "batch.size": 16384,
            "retries": 5,
            "retry.backoff.ms": 200,
            "compression.type": "lz4",
        }
    )


def delivery_callback(err, msg):
    if err:
        log.error("Delivery failed for %s: %s", msg.key(), err)


def parse_trade(raw: dict) -> Optional[dict]:
    """
    Extract trade fields from a Binance combined-stream envelope.

    Raw shape: {"stream": "btcusdt@trade", "data": {...trade fields...}}
    Trade fields we care about: s (symbol), p (price), q (qty), T (trade time ms).
    """
    data = raw.get("data", {})
    if data.get("e") != "trade":
        return None
    return {
        "symbol": data["s"],          # e.g. "BTCUSDT"
        "price": float(data["p"]),
        "qty": float(data["q"]),
        "ts": int(data["T"]),          # epoch ms
    }


async def consume_stream(producer: Producer):
    """Open WebSocket and forward trade events to Kafka indefinitely."""
    while True:
        try:
            log.info("Connecting to Binance stream …")
            async with websockets.connect(
                BINANCE_WS_URL,
                ping_interval=20,
                ping_timeout=10,
            ) as ws:
                log.info("Connected.")
                async for raw_msg in ws:
                    event = json.loads(raw_msg)
                    trade = parse_trade(event)
                    if trade is None:
                        continue

                    payload = json.dumps(trade).encode()
                    key = trade["symbol"].encode()
                    producer.produce(
                        KAFKA_TOPIC,
                        value=payload,
                        key=key,
                        callback=delivery_callback,
                    )
                    # Poll to trigger delivery callbacks without blocking.
                    producer.poll(0)

        except (websockets.ConnectionClosed, OSError) as exc:
            log.warning("WebSocket disconnected (%s). Reconnecting in %ds …",
                        exc, RECONNECT_BACKOFF_S)
            producer.flush()
            await asyncio.sleep(RECONNECT_BACKOFF_S)

        except Exception as exc:  # pylint: disable=broad-except
            log.error("Unexpected error: %s. Reconnecting in %ds …",
                      exc, RECONNECT_BACKOFF_S)
            producer.flush()
            await asyncio.sleep(RECONNECT_BACKOFF_S)


def main():
    producer = build_producer()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _shutdown(*_):
        log.info("Shutting down …")
        producer.flush(timeout=10)
        loop.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        loop.run_until_complete(consume_stream(producer))
    finally:
        producer.flush(timeout=10)
        loop.close()


if __name__ == "__main__":
    main()
