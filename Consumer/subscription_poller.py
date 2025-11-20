# subscription_poller.py

import threading
import time
import requests
import os
from config import ADMIN_BASE_URL, POLL_INTERVAL

# Get consumer name from environment or use default
CONSUMER_NAME = os.getenv('CONSUMER_NAME', 'consumer-1')

class SubscriptionPoller:
    """Background thread that polls admin node for this consumer's subscriptions."""

    def __init__(self, callback):
        self.callback = callback
        self.running = True
        self.last_topics = set()

    def start(self):
        threading.Thread(target=self._poll_loop, daemon=True).start()

    def _poll_loop(self):
        url = f"{ADMIN_BASE_URL}/subscriptions/{CONSUMER_NAME}"
        while self.running:
            try:
                r = requests.get(url, timeout=3)
                r.raise_for_status()
                # API returns list of { topic_name: <name>, subscribed_at: ... }
                topics = set(item.get("topic_name") for item in r.json() if item.get("topic_name"))
                if topics != self.last_topics:
                    self.last_topics = topics
                    self.callback(list(topics))
            except Exception as e:
                print(f"[SubscriptionPoller] Error fetching subscriptions: {e}")
            time.sleep(POLL_INTERVAL)

    def stop(self):
        self.running = False



