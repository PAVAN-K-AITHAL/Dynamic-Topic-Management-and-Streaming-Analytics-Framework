#!/usr/bin/env python3
import requests, time, threading
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

class TopicWatcherThread:
   
    def __init__(self, broker, admin_url, poll_interval=5):
        self.broker = broker
        self.admin_url = admin_url.rstrip("/")  
        self.poll_interval = poll_interval
        self.active_topics = set()
        self.lock = threading.Lock()
        self.running = True
        self.admin_client = KafkaAdminClient(bootstrap_servers=[self.broker])

    def create_pending_topic(self, topic_name, description="auto-created"):
        """Register a pending topic in Admin DB via POST /topics"""
        try:
            payload = {"name": topic_name, "description": description}
            r = requests.post(self.admin_url, json=payload, timeout=5)
            if r.status_code in (200, 201):
                print(f"[Watcher] Registered pending topic: {topic_name}")
            elif r.status_code in (400, 409):
                print(f"[Watcher] Topic already exists in Admin DB: {topic_name}")
            else:
                print(f"[Watcher] Failed pending topic ({r.status_code}) {topic_name} -> {r.text}")
        except Exception as e:
            print(f"[Watcher] Error creating pending topic {topic_name}: {e}")

    def _ensure_topic_in_kafka(self, topic):
        """Create topic in Kafka dynamically"""
        try:
            self.admin_client.create_topics(
                [NewTopic(name=topic, num_partitions=1, replication_factor=1)]
            )
            print(f"[Watcher] Created topic in Kafka: {topic}")
        except TopicAlreadyExistsError:
            print(f"[Watcher] Topic already exists in Kafka: {topic}")
        except Exception as e:
            print(f"[Watcher] Kafka topic creation error {topic}: {e}")

    def _activate_in_admin(self, topic):
        """Activate topic in Admin DB"""
        try:
            url = f"{self.admin_url}/{topic}/activate"
            r = requests.post(url, timeout=5)
            if r.status_code in (200, 201):
                print(f"[Watcher] Activated topic in Admin DB: {topic}")
            else:
                print(f"[Watcher] Activation failed ({r.status_code}) {topic} -> {r.text}")
        except Exception as e:
            print(f"[Watcher] Activation error {topic}: {e}")

    def run(self):
        print("[Watcher] Started -> polling Admin API for approved topics...")
        while self.running:
            try:
                r = requests.get(self.admin_url, timeout=5)
                r.raise_for_status()
                data = r.json()

                approved = {t["name"] for t in data if t.get("status") == "approved"}
                active   = {t["name"] for t in data if t.get("status") == "active"}
                to_promote = approved - active

                for topic in sorted(to_promote):
                    self._ensure_topic_in_kafka(topic)
                    self._activate_in_admin(topic)

                with self.lock:
                    self.active_topics = active

                if active:
                    print(f"[Watcher] Active topics -> {', '.join(sorted(active))}")
                else:
                    print("[Watcher] No active topics currently.")
            except Exception as e:
                print(f"[Watcher] Error polling Admin: {e}")

            time.sleep(self.poll_interval)

    def stop(self):
        self.running = False

