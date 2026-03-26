# Crypto Anomaly Detection Pipeline

Real-time pipeline that streams Binance trade data, detects price anomalies with PySpark Structured Streaming, and exposes results via a FastAPI dashboard.

## Architecture

Trade events from the Binance WebSocket (BTC, ETH, SOL) are published to a 6-partition Kafka topic (`price-events`) by a Python producer using the `confluent-kafka` library with `acks=all` for durability. A PySpark Structured Streaming job reads from that topic, computes a 5-minute sliding-window mean and standard deviation per symbol, and writes any event whose price deviates by more than 3 standard deviations (z-score > 3) into a TimescaleDB `anomalies` hypertable for efficient time-series querying. A FastAPI service exposes the stored anomalies and a health endpoint, with CORS enabled for browser-based dashboards.

## Setup

```bash
# Start Kafka, Zookeeper, and TimescaleDB
docker compose up -d

# Install Python dependencies
pip install -r requirements.txt

# Run the Binance producer (in a separate terminal)
python producer/binance_producer.py

# Submit the PySpark streaming job
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
  processor/spark_streaming.py

# Start the FastAPI backend
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /anomalies` | Last 100 anomaly events |
| `GET /health` | Service and database status |

## Throughput Benchmarks

> _To be filled in after load testing._

| Metric | Value |
|---|---|
| Messages / second (producer) | _TBD_ |
| End-to-end latency (p99) | _TBD_ |
| Anomaly detection lag | _TBD_ |
| TimescaleDB insert rate | _TBD_ |
