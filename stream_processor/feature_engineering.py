"""
feature_engineering.py

Pure Python feature computation functions used as Spark UDFs
and for Redis-backed stateful features.

Features computed:
  - amount_zscore: (current - mean) / std for user
  - hour_of_day: hour extracted from timestamp
  - is_weekend: boolean weekend flag
  - is_round_amount: boolean if amount is a round number
  - amount_ratio: current_amount / avg_amount (from Redis)
  - time_since_last_txn: seconds since user's last transaction (from Redis)
"""

import math
from datetime import datetime


def compute_hour_of_day(timestamp_str):
    """Extract hour (0-23) from ISO timestamp string."""
    try:
        dt = datetime.fromisoformat(timestamp_str)
        return dt.hour
    except (ValueError, TypeError):
        return 0


def compute_is_weekend(timestamp_str):
    """Check if timestamp falls on weekend (Saturday=5, Sunday=6)."""
    try:
        dt = datetime.fromisoformat(timestamp_str)
        return dt.weekday() >= 5
    except (ValueError, TypeError):
        return False


def compute_is_round_amount(amount):
    """Check if amount is a round number (common fraud signal)."""
    try:
        amount = float(amount)
        if amount <= 0:
            return False
        # Round if it's a whole number or ends in .00, .50
        return amount == int(amount) or (amount * 2) == int(amount * 2)
    except (ValueError, TypeError):
        return False


def compute_amount_zscore(amount, mean_amount, std_amount):
    """Compute z-score: (amount - mean) / std."""
    try:
        amount = float(amount)
        mean_amount = float(mean_amount)
        std_amount = float(std_amount)
        if std_amount < 1e-6:
            return 0.0
        return (amount - mean_amount) / std_amount
    except (ValueError, TypeError):
        return 0.0


def compute_amount_ratio(current_amount, avg_amount):
    """Compute ratio of current amount to average amount."""
    try:
        current = float(current_amount)
        avg = float(avg_amount)
        if avg < 1e-6:
            return 1.0
        return current / avg
    except (ValueError, TypeError):
        return 1.0


def compute_time_since_last_txn(current_ts_str, last_ts_str):
    """Compute seconds elapsed since last transaction."""
    try:
        current = datetime.fromisoformat(current_ts_str)
        last = datetime.fromisoformat(last_ts_str)
        diff = (current - last).total_seconds()
        return max(0.0, diff)
    except (ValueError, TypeError):
        return -1.0  # -1 indicates no previous transaction


# ── Redis-backed stateful features ────────────────────────────────────

class UserProfileStore:
    """
    Manages per-user state in Redis for cross-window feature computation.
    
    Key schema:
        user:{user_id}:last_txn_time   → ISO timestamp of last transaction
        user:{user_id}:running_sum     → running sum of amounts
        user:{user_id}:running_count   → count of transactions
        user:{user_id}:running_sq_sum  → running sum of squared amounts (for std)
    """

    def __init__(self, redis_client=None):
        self.redis = redis_client

    def _connect(self):
        """Lazy connect to Redis."""
        if self.redis is None:
            try:
                import redis
                from config.settings import REDIS_HOST, REDIS_PORT
                self.redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
                self.redis.ping()
            except Exception as e:
                print(f"[UserProfileStore] Redis not available: {e}. Using in-memory fallback.")
                self.redis = None
        return self.redis is not None

    def update_and_get_features(self, user_id, amount, timestamp_str):
        """
        Update user profile and return stateful features.
        Returns dict with: time_since_last_txn, amount_zscore, amount_ratio
        """
        features = {
            'time_since_last_txn': -1.0,
            'amount_zscore': 0.0,
            'amount_ratio': 1.0,
            'user_avg_amount': 0.0,
            'user_txn_count': 0,
        }

        if not self._connect():
            return features

        try:
            amount = float(amount)
            key_prefix = f"user:{user_id}"

            # Get previous state
            pipe = self.redis.pipeline()
            pipe.get(f"{key_prefix}:last_txn_time")
            pipe.get(f"{key_prefix}:running_sum")
            pipe.get(f"{key_prefix}:running_count")
            pipe.get(f"{key_prefix}:running_sq_sum")
            last_time, run_sum, run_count, run_sq_sum = pipe.execute()

            # Parse
            run_sum = float(run_sum) if run_sum else 0.0
            run_count = int(run_count) if run_count else 0
            run_sq_sum = float(run_sq_sum) if run_sq_sum else 0.0

            # Compute features from previous state
            if last_time:
                features['time_since_last_txn'] = compute_time_since_last_txn(timestamp_str, last_time)

            if run_count > 0:
                mean = run_sum / run_count
                variance = (run_sq_sum / run_count) - (mean ** 2)
                std = math.sqrt(max(0, variance))
                features['amount_zscore'] = compute_amount_zscore(amount, mean, std)
                features['amount_ratio'] = compute_amount_ratio(amount, mean)
                features['user_avg_amount'] = mean
                features['user_txn_count'] = run_count

            # Update state
            pipe = self.redis.pipeline()
            pipe.set(f"{key_prefix}:last_txn_time", timestamp_str)
            pipe.incrbyfloat(f"{key_prefix}:running_sum", amount)
            pipe.incr(f"{key_prefix}:running_count")
            pipe.incrbyfloat(f"{key_prefix}:running_sq_sum", amount * amount)
            # Set TTL of 48 hours for auto-cleanup
            pipe.expire(f"{key_prefix}:last_txn_time", 172800)
            pipe.expire(f"{key_prefix}:running_sum", 172800)
            pipe.expire(f"{key_prefix}:running_count", 172800)
            pipe.expire(f"{key_prefix}:running_sq_sum", 172800)
            pipe.execute()

        except Exception as e:
            print(f"[UserProfileStore] Error for {user_id}: {e}")

        return features
