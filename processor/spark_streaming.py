"""
PySpark Structured Streaming job — crypto price anomaly detection.

Reads trade events from the `price-events` Kafka topic, computes a
5-minute rolling window (mean, stddev, count) per symbol, detects
price anomalies where |z-score| > 3, and writes them to TimescaleDB.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = "price-events"

TIMESCALE_URL = os.getenv(
    "TIMESCALE_URL",
    "jdbc:postgresql://timescaledb:5432/crypto_db",
)
TIMESCALE_USER = os.getenv("TIMESCALE_USER", "crypto")
TIMESCALE_PASS = os.getenv("TIMESCALE_PASS", "crypto_pass")
TIMESCALE_TABLE = "anomalies"

ZSCORE_THRESHOLD = 3.0
WINDOW_DURATION = "5 minutes"
SLIDE_DURATION = "1 minute"
WATERMARK_DELAY = "10 seconds"
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoint")

# ---------------------------------------------------------------------------
# Schema for incoming JSON messages
# ---------------------------------------------------------------------------
TRADE_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("price", DoubleType(), nullable=False),
        StructField("qty", DoubleType(), nullable=False),
        StructField("ts", LongType(), nullable=False),  # epoch ms
    ]
)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("CryptoAnomalyDetection")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        # Kafka connector jar must be on the classpath via spark-submit --packages
        .getOrCreate()
    )


def run(spark: SparkSession):
    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------------
    # 1. Read from Kafka
    # ------------------------------------------------------------------
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    trades = (
        raw.select(
            F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("d")
        )
        .select("d.*")
        .withColumn(
            "event_time",
            (F.col("ts") / 1000).cast(TimestampType()),
        )
        .withWatermark("event_time", WATERMARK_DELAY)
    )

    # ------------------------------------------------------------------
    # 2. Rolling window aggregation — mean, stddev, count per symbol
    # ------------------------------------------------------------------
    windowed = trades.groupBy(
        F.window("event_time", WINDOW_DURATION, SLIDE_DURATION),
        F.col("symbol"),
    ).agg(
        F.mean("price").alias("mean_price"),
        F.stddev("price").alias("std_price"),
        F.count("price").alias("event_count"),
    )

    # ------------------------------------------------------------------
    # 3. Join back to individual trades to compute z-score
    # ------------------------------------------------------------------
    trades_with_window = trades.join(
        windowed,
        on=[
            trades.symbol == windowed.symbol,
            trades.event_time >= windowed.window.start,
            trades.event_time < windowed.window.end,
        ],
        how="inner",
    ).select(
        trades.symbol,
        trades.price,
        trades.event_time,
        windowed.mean_price.alias("mean"),
        windowed.std_price.alias("stddev"),
        windowed.event_count,
    )

    anomalies = (
        trades_with_window
        .filter(F.col("stddev").isNotNull() & (F.col("stddev") > 0))
        .withColumn(
            "zscore",
            F.abs((F.col("price") - F.col("mean")) / F.col("stddev")),
        )
        .filter(F.col("zscore") > ZSCORE_THRESHOLD)
        .select(
            F.col("symbol"),
            F.col("price"),
            F.col("mean"),
            F.col("stddev"),
            F.col("zscore"),
            F.col("event_time").alias("detected_at"),
        )
    )

    # ------------------------------------------------------------------
    # 4. Write anomalies to TimescaleDB via JDBC foreachBatch
    # ------------------------------------------------------------------
    jdbc_props = {
        "user": TIMESCALE_USER,
        "password": TIMESCALE_PASS,
        "driver": "org.postgresql.Driver",
    }

    def write_to_timescale(batch_df, batch_id):
        if batch_df.count() == 0:
            return
        (
            batch_df.write.jdbc(
                url=TIMESCALE_URL,
                table=TIMESCALE_TABLE,
                mode="append",
                properties=jdbc_props,
            )
        )

    query = (
        anomalies.writeStream.outputMode("append")
        .foreachBatch(write_to_timescale)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/anomalies")
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    spark = build_spark()
    run(spark)
