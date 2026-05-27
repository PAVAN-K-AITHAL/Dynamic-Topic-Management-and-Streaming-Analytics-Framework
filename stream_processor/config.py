"""
Spark + Kafka configuration for the stream processor.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config.settings import (
    KAFKA_BROKER, REDIS_HOST, REDIS_PORT,
    TOPIC_RAW_TRANSACTIONS, TOPIC_ENRICHED_TRANSACTIONS
)

# ── Spark Config ──────────────────────────────────────────────────────
SPARK_APP_NAME = "FraudFeatureEngineering"
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

# Kafka connector for Spark — must match your PySpark version
SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6"

# ── Processing Config ────────────────────────────────────────────────
WATERMARK_DELAY = "1 hour"
WINDOW_1H = "1 hour"
WINDOW_24H = "24 hours"
TRIGGER_INTERVAL = "10 seconds"    # Micro-batch interval

# ── Checkpoint ────────────────────────────────────────────────────────
CHECKPOINT_DIR = os.getenv(
    "SPARK_CHECKPOINT_DIR",
    os.path.join(os.path.dirname(__file__), "..", "data", "spark_checkpoints")
)
