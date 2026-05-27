#!/usr/bin/env python3
"""
spark_streaming_job.py

Spark Structured Streaming job that:
1. Reads raw transactions from Kafka topic 'raw-transactions'
2. Parses JSON and applies transaction schema
3. Computes windowed aggregation features (txn_count_1h, avg_amount_1h, etc.)
4. Computes per-transaction features (hour_of_day, is_weekend, is_round_amount)
5. Writes enriched transactions to Kafka topic 'enriched-transactions'

Usage:
    python spark_streaming_job.py

Requirements:
    - Java 17+ (you have Java 25 ✅)
    - PySpark 3.5.x
    - Kafka running on localhost:9092
    - winutils.exe configured (HADOOP_HOME set)
"""

import os
import sys
import json

# ── Import configs ────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from stream_processor.config import (
    SPARK_APP_NAME, SPARK_MASTER, SPARK_KAFKA_PACKAGE,
    WATERMARK_DELAY, WINDOW_1H, TRIGGER_INTERVAL, CHECKPOINT_DIR
)
from config.settings import (
    KAFKA_BROKER, TOPIC_RAW_TRANSACTIONS, TOPIC_ENRICHED_TRANSACTIONS,
    MODEL_SERVER_URL
)

# ── Spark imports ─────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, to_json, col, struct, window, count, avg, max as spark_max,
    stddev, lit, when, udf, current_timestamp, expr, hour, dayofweek
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    BooleanType, TimestampType
)


def create_spark_session():
    """Create and configure Spark session with Kafka support."""
    # Set HADOOP_HOME if not set (for Windows)
    if not os.environ.get('HADOOP_HOME'):
        hadoop_home = os.path.join(os.path.dirname(__file__), '..', 'hadoop')
        if os.path.exists(hadoop_home):
            os.environ['HADOOP_HOME'] = hadoop_home
            print(f"[Spark] Set HADOOP_HOME to: {hadoop_home}")

    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages", SPARK_KAFKA_PACKAGE) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print(f"[Spark] Session created — {SPARK_APP_NAME}")
    print(f"[Spark] Kafka broker: {KAFKA_BROKER}")
    return spark


# ── Transaction Schema ────────────────────────────────────────────────
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("Time", DoubleType(), True),
    StructField("Amount", DoubleType(), True),
    StructField("Class", DoubleType(), True),
] + [
    StructField(f"V{i}", DoubleType(), True) for i in range(1, 29)
])


# ── UDFs for per-transaction features ─────────────────────────────────
@udf(BooleanType())
def is_round_amount_udf(amount):
    """Check if amount is a round number."""
    try:
        if amount is None or amount <= 0:
            return False
        return amount == int(amount) or (amount * 2) == int(amount * 2)
    except:
        return False


def build_pipeline(spark):
    """Build the Spark Structured Streaming pipeline."""

    # ── 1. Read from Kafka ────────────────────────────────────────────
    print(f"[Spark] Reading from topic: {TOPIC_RAW_TRANSACTIONS}")
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_RAW_TRANSACTIONS) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # ── 2. Parse JSON → structured schema ─────────────────────────────
    parsed = raw_stream.select(
        from_json(
            col("value").cast("string"),
            TRANSACTION_SCHEMA
        ).alias("txn"),
        col("timestamp").alias("kafka_timestamp")
    ).select("txn.*", "kafka_timestamp")

    # Cast timestamp string to proper TimestampType
    parsed = parsed.withColumn(
        "event_time",
        col("kafka_timestamp")  # Use Kafka ingestion time as event time
    )

    # ── 3. Per-transaction features ───────────────────────────────────
    enriched = parsed \
        .withColumn("hour_of_day", hour(col("event_time"))) \
        .withColumn("day_of_week", dayofweek(col("event_time"))) \
        .withColumn("is_weekend", when(dayofweek(col("event_time")).isin(1, 7), True).otherwise(False)) \
        .withColumn("is_round_amount", is_round_amount_udf(col("Amount")))

    # ── 4. Windowed aggregations per user ─────────────────────────────
    # Watermark for late data handling
    windowed_base = enriched.withWatermark("event_time", WATERMARK_DELAY)

    # 1-hour window aggregations per user
    user_window_1h = windowed_base \
        .groupBy(
            window(col("event_time"), WINDOW_1H),
            col("user_id")
        ) \
        .agg(
            count("*").alias("txn_count_1h"),
            avg("Amount").alias("avg_amount_1h"),
            spark_max("Amount").alias("max_amount_1h"),
            stddev("Amount").alias("std_amount_1h")
        ) \
        .select(
            col("user_id").alias("agg_user_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "txn_count_1h",
            "avg_amount_1h",
            "max_amount_1h",
            "std_amount_1h"
        )

    # ── 5. Build output payload ───────────────────────────────────────
    # For the output, we write the enriched per-transaction stream
    # The windowed aggregations are available separately for joining

    # Select the columns we want in the output
    output_cols = [
        "transaction_id", "timestamp", "user_id", "merchant_id",
        "Time", "Amount", "Class",
        "hour_of_day", "day_of_week", "is_weekend", "is_round_amount"
    ] + [f"V{i}" for i in range(1, 29)]

    output = enriched.select(
        to_json(struct([col(c) for c in output_cols if c in enriched.columns])).alias("value")
    )

    return output, user_window_1h


def start_streaming(output, windowed_agg):
    """Start the streaming queries."""

    # Ensure checkpoint directory exists
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    enriched_checkpoint = os.path.join(CHECKPOINT_DIR, "enriched")
    windowed_checkpoint = os.path.join(CHECKPOINT_DIR, "windowed")

    # ── Write enriched transactions to Kafka ──────────────────────────
    print(f"[Spark] Writing enriched stream to topic: {TOPIC_ENRICHED_TRANSACTIONS}")
    enriched_query = output.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", TOPIC_ENRICHED_TRANSACTIONS) \
        .option("checkpointLocation", enriched_checkpoint) \
        .trigger(processingTime=TRIGGER_INTERVAL) \
        .outputMode("append") \
        .start()

    # ── Write windowed aggregations to console (for debugging) ────────
    windowed_query = windowed_agg.writeStream \
        .format("console") \
        .option("truncate", "false") \
        .option("checkpointLocation", windowed_checkpoint) \
        .trigger(processingTime=TRIGGER_INTERVAL) \
        .outputMode("update") \
        .start()

    print("[Spark] ✅ Streaming started!")
    print(f"[Spark] Enriched → Kafka topic '{TOPIC_ENRICHED_TRANSACTIONS}'")
    print(f"[Spark] Windowed aggregations → console")
    print("[Spark] Press Ctrl+C to stop")

    return enriched_query, windowed_query


def main():
    print("=" * 60)
    print("  Fraud Detection — Spark Structured Streaming")
    print("=" * 60)

    spark = create_spark_session()

    try:
        output, windowed_agg = build_pipeline(spark)
        enriched_query, windowed_query = start_streaming(output, windowed_agg)

        # Wait for termination
        enriched_query.awaitTermination()

    except KeyboardInterrupt:
        print("\n[Spark] Shutting down...")
    except Exception as e:
        print(f"[Spark] Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("[Spark] Session stopped.")


if __name__ == "__main__":
    main()
