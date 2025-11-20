# topic_poller.py

import threading
import time
import requests
from config import ADMIN_BASE_URL, POLL_INTERVAL

class TopicPoller:
    """Background thread that polls admin node for active topics."""

    def __init__(self, callback):
        self.callback = callback
        self.running = True
        self.last_topics = set()

    def start(self):
        threading.Thread(target=self._poll_loop, daemon=True).start()

    def _poll_loop(self):
        url = f"{ADMIN_BASE_URL}/topics/active"
        while self.running:
            try:
                r = requests.get(url, timeout=3)
                r.raise_for_status()
                topics = set(t["name"] for t in r.json())
                if topics != self.last_topics:
                    self.last_topics = topics
                    self.callback(list(topics))
            except Exception as e:
                print(f"[TopicPoller] Error fetching active topics: {e}")
            time.sleep(POLL_INTERVAL)

    def stop(self):
        self.running = False