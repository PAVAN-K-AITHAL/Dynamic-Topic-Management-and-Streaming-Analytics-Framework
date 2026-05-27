#!/usr/bin/env python3
"""
model_server.py

FastAPI microservice for real-time fraud detection inference.

Features:
  - POST /predict — Ensemble scoring + SHAP explanation
  - GET /health — Health check with model status
  - GET /metrics — Prometheus metrics (auto-instrumented)
  - Tracks prediction latency (p50, p95, p99)
  - Publishes fraud alerts to Admin API

Usage:
    uvicorn ml_models.model_server:app --host 0.0.0.0 --port 8000 --reload
    # OR
    python ml_models/model_server.py
"""

import os
import sys
import time
import json
import asyncio
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

import numpy as np

# ── Import centralized config ────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config.settings import (
    MODEL_SERVER_HOST, MODEL_SERVER_PORT, ADMIN_URL,
    TRAINED_MODELS_DIR, KAFKA_BROKER
)

# ── FastAPI imports ───────────────────────────────────────────────────
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# ── Prometheus ────────────────────────────────────────────────────────
import prometheus_client as prom

# Custom Prometheus metrics
PREDICTIONS_TOTAL = prom.Counter(
    "fraud_predictions_total",
    "Total number of predictions made",
    ["result"]  # "fraud" or "legit"
)
PREDICTION_LATENCY = prom.Histogram(
    "fraud_prediction_latency_seconds",
    "Prediction latency in seconds",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]
)
FRAUD_SCORE_HISTOGRAM = prom.Histogram(
    "fraud_score_distribution",
    "Distribution of fraud scores",
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
)
MODELS_LOADED = prom.Gauge(
    "fraud_models_loaded",
    "Number of ML models currently loaded"
)

# ── Ensemble model ────────────────────────────────────────────────────
from ml_models.ensemble import FraudEnsemble

# Global ensemble instance
ensemble = FraudEnsemble(models_dir=str(TRAINED_MODELS_DIR))


# ── Lifespan ──────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load models on startup."""
    print("[ModelServer] Loading models...")
    loaded = ensemble.load_models()
    MODELS_LOADED.set(len(loaded))
    print(f"[ModelServer] Ready — {len(loaded)} models loaded")
    yield
    print("[ModelServer] Shutting down...")


# ── FastAPI App ───────────────────────────────────────────────────────
app = FastAPI(
    title="Fraud Detection Model Server",
    description="Real-time fraud detection with triple-layer ML ensemble + SHAP explainability",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Prometheus instrumentation
try:
    from prometheus_fastapi_instrumentator import Instrumentator
    Instrumentator().instrument(app).expose(app)
except ImportError:
    # Fallback: manual /metrics endpoint
    from fastapi.responses import PlainTextResponse
    
    @app.get("/metrics")
    async def metrics():
        return PlainTextResponse(
            prom.generate_latest().decode("utf-8"),
            media_type="text/plain"
        )


# ── Pydantic Models ──────────────────────────────────────────────────

class TransactionFeatures(BaseModel):
    """Input transaction features for prediction."""
    transaction_id: str = Field(default="unknown")
    user_id: Optional[str] = None
    merchant_id: Optional[str] = None
    timestamp: Optional[str] = None
    Time: float = 0.0
    V1: float = 0.0
    V2: float = 0.0
    V3: float = 0.0
    V4: float = 0.0
    V5: float = 0.0
    V6: float = 0.0
    V7: float = 0.0
    V8: float = 0.0
    V9: float = 0.0
    V10: float = 0.0
    V11: float = 0.0
    V12: float = 0.0
    V13: float = 0.0
    V14: float = 0.0
    V15: float = 0.0
    V16: float = 0.0
    V17: float = 0.0
    V18: float = 0.0
    V19: float = 0.0
    V20: float = 0.0
    V21: float = 0.0
    V22: float = 0.0
    V23: float = 0.0
    V24: float = 0.0
    V25: float = 0.0
    V26: float = 0.0
    V27: float = 0.0
    V28: float = 0.0
    Amount: float = 0.0
    Class: Optional[float] = None  # Ground truth (if available)

    def to_feature_array(self):
        """Convert to numpy array in the order expected by models."""
        return np.array([[
            self.V1, self.V2, self.V3, self.V4, self.V5,
            self.V6, self.V7, self.V8, self.V9, self.V10,
            self.V11, self.V12, self.V13, self.V14, self.V15,
            self.V16, self.V17, self.V18, self.V19, self.V20,
            self.V21, self.V22, self.V23, self.V24, self.V25,
            self.V26, self.V27, self.V28, self.Amount, self.Time
        ]])


class PredictionResponse(BaseModel):
    """Prediction result from the ensemble model."""
    transaction_id: str
    fraud_score: float
    is_fraud: bool
    model_scores: Dict[str, float]
    explanation: Optional[List[Dict[str, Any]]] = None
    latency_ms: float


# ── Endpoints ─────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """Health check with model status."""
    return {
        "status": "ok",
        "models_loaded": ensemble._loaded,
        "models": {
            "isolation_forest": ensemble.isolation_forest is not None,
            "xgboost": ensemble.xgboost_model is not None,
            "autoencoder": ensemble.autoencoder is not None,
            "scaler": ensemble.scaler is not None,
            "shap_explainer": ensemble.shap_explainer is not None,
        },
        "threshold": ensemble.threshold,
    }


@app.post("/predict", response_model=PredictionResponse)
async def predict(transaction: TransactionFeatures):
    """
    Run fraud detection ensemble on a transaction.
    Returns fraud score, binary decision, per-model scores, and SHAP explanation.
    """
    start = time.time()

    # Extract feature array
    features = transaction.to_feature_array()

    # Scale features
    scaled_features = ensemble.preprocess(features)

    # Run ensemble prediction
    result = ensemble.predict(scaled_features)

    # Generate SHAP explanation if suspicious (score > 0.3)
    explanation = None
    if result['fraud_score'] > 0.3:
        explanation = ensemble.explain(scaled_features, top_k=5)

    elapsed_ms = (time.time() - start) * 1000

    # Record Prometheus metrics
    PREDICTION_LATENCY.observe(elapsed_ms / 1000)
    FRAUD_SCORE_HISTOGRAM.observe(result['fraud_score'])
    PREDICTIONS_TOTAL.labels(result="fraud" if result['is_fraud'] else "legit").inc()

    # If fraud detected, report to admin API asynchronously
    if result['is_fraud']:
        asyncio.create_task(_report_fraud_alert(
            transaction, result, explanation
        ))

    return PredictionResponse(
        transaction_id=transaction.transaction_id,
        fraud_score=result['fraud_score'],
        is_fraud=result['is_fraud'],
        model_scores=result['model_scores'],
        explanation=explanation,
        latency_ms=round(elapsed_ms, 2),
    )


@app.post("/predict/batch")
async def predict_batch(transactions: List[TransactionFeatures]):
    """Batch prediction for multiple transactions."""
    results = []
    for txn in transactions:
        start = time.time()
        features = txn.to_feature_array()
        scaled = ensemble.preprocess(features)
        result = ensemble.predict(scaled)
        elapsed_ms = (time.time() - start) * 1000

        PREDICTION_LATENCY.observe(elapsed_ms / 1000)
        PREDICTIONS_TOTAL.labels(result="fraud" if result['is_fraud'] else "legit").inc()

        results.append({
            "transaction_id": txn.transaction_id,
            "fraud_score": result['fraud_score'],
            "is_fraud": result['is_fraud'],
            "model_scores": result['model_scores'],
            "latency_ms": round(elapsed_ms, 2),
        })
    return results


@app.post("/threshold")
async def update_threshold(threshold: float):
    """Update the fraud detection threshold."""
    if not 0 < threshold < 1:
        raise HTTPException(400, "Threshold must be between 0 and 1")
    ensemble.threshold = threshold
    return {"status": "updated", "threshold": threshold}


@app.post("/reload")
async def reload_models():
    """Reload models from disk (hot reload without restart)."""
    loaded = ensemble.load_models()
    MODELS_LOADED.set(len(loaded))
    return {"status": "reloaded", "models": loaded}


# ── Helper: report fraud to admin API ─────────────────────────────────
async def _report_fraud_alert(transaction, result, explanation):
    """Asynchronously report fraud alert to admin service."""
    try:
        import httpx
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{ADMIN_URL}/fraud/alerts",
                json={
                    "transaction_id": transaction.transaction_id,
                    "user_id": transaction.user_id,
                    "amount": transaction.Amount,
                    "fraud_score": result['fraud_score'],
                    "is_fraud": result['is_fraud'],
                    "model_scores": result['model_scores'],
                    "explanation": explanation,
                },
                timeout=5.0,
            )
    except Exception as e:
        # Non-critical — don't fail prediction if admin is down
        print(f"[ModelServer] Failed to report alert: {e}")


# ── Run ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    print(f"[ModelServer] Starting on {MODEL_SERVER_HOST}:{MODEL_SERVER_PORT}")
    uvicorn.run(
        "ml_models.model_server:app",
        host=MODEL_SERVER_HOST,
        port=MODEL_SERVER_PORT,
        reload=True,
    )
