"""
Centralized configuration for the entire project.

All hardcoded ZeroTier IPs (10.147.19.93) and Ubuntu paths
(/home/pes1ug23cs420/...) are replaced by this single source of truth.

Everything defaults to localhost for single-system development.
Override via environment variables for Docker/cloud deployment.
"""

import os
from pathlib import Path

# ── Project root ──────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ── Kafka ─────────────────────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

# ── Admin Service (Flask) ─────────────────────────────────────────────
ADMIN_HOST = os.getenv("ADMIN_HOST", "0.0.0.0")
ADMIN_PORT = int(os.getenv("ADMIN_PORT", "5000"))
ADMIN_URL = os.getenv("ADMIN_URL", f"http://localhost:{ADMIN_PORT}")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "demo123")

# ── Model Server (FastAPI) ────────────────────────────────────────────
MODEL_SERVER_HOST = os.getenv("MODEL_SERVER_HOST", "0.0.0.0")
MODEL_SERVER_PORT = int(os.getenv("MODEL_SERVER_PORT", "8000"))
MODEL_SERVER_URL = os.getenv("MODEL_SERVER_URL", f"http://localhost:{MODEL_SERVER_PORT}")

# ── Redis (Feature Store) ────────────────────────────────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_URL = os.getenv("REDIS_URL", f"redis://{REDIS_HOST}:{REDIS_PORT}")

# ── MLflow ────────────────────────────────────────────────────────────
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")

# ── Paths ─────────────────────────────────────────────────────────────
DATA_DIR = Path(os.getenv("DATA_DIR", str(PROJECT_ROOT / "producer" / "data")))
DB_DIR = PROJECT_ROOT / "data"
DB_PATH = Path(os.getenv("DB_PATH", str(DB_DIR / "topics.db")))
TRAINED_MODELS_DIR = PROJECT_ROOT / "ml_models" / "trained_models"
LOG_DIR = PROJECT_ROOT / "Consumer" / "logs"

# ── Kafka Topics ──────────────────────────────────────────────────────
TOPIC_RAW_TRANSACTIONS = "raw-transactions"
TOPIC_ENRICHED_TRANSACTIONS = "enriched-transactions"
TOPIC_FRAUD_ALERTS = "fraud-alerts"
TOPIC_MODEL_METRICS = "model-metrics"

# ── Fraud Producer ────────────────────────────────────────────────────
REPLAY_SPEED = float(os.getenv("REPLAY_SPEED", "10"))  # 10x real-time default

# ── Consumer ──────────────────────────────────────────────────────────
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "dynamic-consumer-group")
CONSUMER_NAME = os.getenv("CONSUMER_NAME", "consumer-1")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "5"))  # seconds

# ── Ensure directories exist ─────────────────────────────────────────
DB_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)
TRAINED_MODELS_DIR.mkdir(parents=True, exist_ok=True)
