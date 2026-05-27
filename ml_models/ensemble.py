"""
ensemble.py

Triple-layer ensemble model for fraud detection:
  Layer 1: Isolation Forest (unsupervised anomaly detection)
  Layer 2: XGBoost (supervised classification)
  Layer 3: Dense Autoencoder (reconstruction error)

Combined via weighted voting + SHAP explainability on XGBoost.
"""

import os
import sys
import numpy as np
import warnings

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config.settings import TRAINED_MODELS_DIR

warnings.filterwarnings('ignore')


class FraudEnsemble:
    """
    Ensemble fraud detector combining three models:
      - Isolation Forest (anomaly score)
      - XGBoost (fraud probability)
      - Dense Autoencoder (reconstruction error)
    
    Weighted scoring: 0.2 × IsoForest + 0.5 × XGBoost + 0.3 × Autoencoder
    """

    def __init__(self, models_dir=None, threshold=0.5):
        self.models_dir = models_dir or str(TRAINED_MODELS_DIR)
        self.threshold = threshold
        
        # Models (loaded lazily)
        self.isolation_forest = None
        self.xgboost_model = None
        self.autoencoder = None
        self.scaler = None
        self.shap_explainer = None
        
        # Ensemble weights
        self.weights = {
            'isolation_forest': 0.2,
            'xgboost': 0.5,
            'autoencoder': 0.3,
        }
        
        # Feature names (Kaggle Credit Card dataset)
        self.feature_names = (
            [f'V{i}' for i in range(1, 29)] + ['Amount', 'Time']
        )
        
        # Autoencoder threshold (set during training at 97th percentile)
        self.ae_threshold = None
        
        self._loaded = False

    def load_models(self):
        """Load all trained models from disk."""
        import joblib
        
        models_path = self.models_dir
        loaded = []
        
        # Load scaler
        scaler_path = os.path.join(models_path, 'scaler.joblib')
        if os.path.exists(scaler_path):
            self.scaler = joblib.load(scaler_path)
            loaded.append('scaler')
            print(f"[Ensemble] Loaded scaler from {scaler_path}")
        
        # Load Isolation Forest
        iso_path = os.path.join(models_path, 'isolation_forest.joblib')
        if os.path.exists(iso_path):
            self.isolation_forest = joblib.load(iso_path)
            loaded.append('isolation_forest')
            print(f"[Ensemble] Loaded Isolation Forest from {iso_path}")

        # Load XGBoost
        xgb_path = os.path.join(models_path, 'xgboost_fraud.joblib')
        if os.path.exists(xgb_path):
            self.xgboost_model = joblib.load(xgb_path)
            loaded.append('xgboost')
            print(f"[Ensemble] Loaded XGBoost from {xgb_path}")
            
            # Initialize SHAP explainer
            try:
                import shap
                self.shap_explainer = shap.TreeExplainer(self.xgboost_model)
                loaded.append('shap_explainer')
                print("[Ensemble] SHAP TreeExplainer initialized")
            except Exception as e:
                print(f"[Ensemble] SHAP init failed: {e}")

        # Load Autoencoder
        ae_path = os.path.join(models_path, 'autoencoder.pt')
        if os.path.exists(ae_path):
            try:
                import torch
                self.autoencoder = torch.load(ae_path, map_location='cpu', weights_only=False)
                self.autoencoder.eval()
                loaded.append('autoencoder')
                print(f"[Ensemble] Loaded Autoencoder from {ae_path}")
            except Exception as e:
                print(f"[Ensemble] Autoencoder load failed: {e}")
        
        # Load autoencoder threshold
        ae_thresh_path = os.path.join(models_path, 'ae_threshold.joblib')
        if os.path.exists(ae_thresh_path):
            self.ae_threshold = joblib.load(ae_thresh_path)
            print(f"[Ensemble] Loaded AE threshold: {self.ae_threshold}")
        
        self._loaded = True
        print(f"[Ensemble] Loaded models: {loaded}")
        
        if not loaded:
            print("[Ensemble] ⚠️ No trained models found! Run training notebooks first.")
            print(f"[Ensemble] Expected models in: {models_path}")
        
        return loaded

    def _normalize_iso_score(self, score):
        """Normalize Isolation Forest score from [-1, 1] to [0, 1]."""
        # decision_function: negative = anomaly, positive = normal
        # We want: 1 = anomalous (fraud), 0 = normal
        return max(0.0, min(1.0, 0.5 - score * 0.5))

    def _normalize_ae_error(self, error):
        """Normalize autoencoder reconstruction error to [0, 1]."""
        if self.ae_threshold is None or self.ae_threshold <= 0:
            return min(1.0, error / 10.0)  # Fallback normalization
        # Scale so threshold maps to ~0.5
        return min(1.0, error / (2 * self.ae_threshold))

    def predict(self, features_array):
        """
        Run ensemble prediction on a single transaction.
        
        Args:
            features_array: numpy array of shape (1, n_features) — scaled features
            
        Returns:
            dict with fraud_score, is_fraud, model_scores, explanation
        """
        result = {
            'fraud_score': 0.0,
            'is_fraud': False,
            'model_scores': {},
            'explanation': None,
        }
        
        features = np.array(features_array).reshape(1, -1)
        active_weights = {}
        scores = {}

        # ── Isolation Forest ──────────────────────────────────────────
        if self.isolation_forest is not None:
            try:
                raw_score = self.isolation_forest.decision_function(features)[0]
                scores['isolation_forest'] = self._normalize_iso_score(raw_score)
                active_weights['isolation_forest'] = self.weights['isolation_forest']
            except Exception as e:
                print(f"[Ensemble] IsoForest error: {e}")

        # ── XGBoost ───────────────────────────────────────────────────
        if self.xgboost_model is not None:
            try:
                prob = self.xgboost_model.predict_proba(features)[0, 1]
                scores['xgboost'] = float(prob)
                active_weights['xgboost'] = self.weights['xgboost']
            except Exception as e:
                print(f"[Ensemble] XGBoost error: {e}")

        # ── Autoencoder ───────────────────────────────────────────────
        if self.autoencoder is not None:
            try:
                import torch
                with torch.no_grad():
                    x = torch.FloatTensor(features)
                    reconstructed = self.autoencoder(x)
                    error = torch.mean((x - reconstructed) ** 2).item()
                scores['autoencoder'] = self._normalize_ae_error(error)
                active_weights['autoencoder'] = self.weights['autoencoder']
            except Exception as e:
                print(f"[Ensemble] Autoencoder error: {e}")

        # ── Weighted ensemble ─────────────────────────────────────────
        if active_weights:
            total_weight = sum(active_weights.values())
            fraud_score = sum(
                scores[k] * active_weights[k] / total_weight
                for k in scores
            )
            result['fraud_score'] = round(float(fraud_score), 6)
            result['is_fraud'] = fraud_score > self.threshold
            result['model_scores'] = {k: round(v, 6) for k, v in scores.items()}

        return result

    def explain(self, features_array, top_k=5):
        """
        Generate SHAP explanation for a prediction.
        
        Returns top-k features that contributed most to the fraud prediction.
        """
        if self.shap_explainer is None:
            return None

        try:
            features = np.array(features_array).reshape(1, -1)
            shap_values = self.shap_explainer.shap_values(features)
            
            # Get absolute SHAP values and sort
            if isinstance(shap_values, list):
                # Binary classification — use class 1 (fraud)
                sv = shap_values[1][0] if len(shap_values) > 1 else shap_values[0][0]
            else:
                sv = shap_values[0]
            
            # Build explanation
            feature_importance = []
            for i, (name, shap_val) in enumerate(zip(self.feature_names, sv)):
                feature_importance.append({
                    'feature': name,
                    'shap_value': round(float(shap_val), 6),
                    'abs_importance': abs(float(shap_val)),
                    'direction': 'increases fraud' if shap_val > 0 else 'decreases fraud',
                    'feature_value': float(features[0, i]) if i < features.shape[1] else None,
                })
            
            # Sort by absolute importance, return top-k
            feature_importance.sort(key=lambda x: x['abs_importance'], reverse=True)
            return feature_importance[:top_k]

        except Exception as e:
            print(f"[Ensemble] SHAP explanation error: {e}")
            return None

    def preprocess(self, raw_features):
        """Scale features using the saved scaler."""
        if self.scaler is not None:
            return self.scaler.transform(np.array(raw_features).reshape(1, -1))
        return np.array(raw_features).reshape(1, -1)
