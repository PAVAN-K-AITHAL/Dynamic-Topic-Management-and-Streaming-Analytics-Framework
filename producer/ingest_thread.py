#!/usr/bin/env python3
import csv, os, time, json
from datetime import datetime

class IngestThread:
   

    def __init__(self, watcher, out_q,
                 datasets_dir=None,
                 poll_interval=3, send_interval=1, loop=True):
        if datasets_dir is None:
            datasets_dir = os.path.join(os.path.dirname(__file__), "data")
        self.watcher = watcher
        self.out_q = out_q
        self.datasets_dir = datasets_dir
        self.poll_interval = poll_interval
        self.send_interval = send_interval
        self.loop = loop
        self.running = True

        self.topic_rows = {}
        self.topic_fields = {}
        self.known_files = set()

        if not os.path.exists(self.datasets_dir):
            os.makedirs(self.datasets_dir, exist_ok=True)
            print(f"[Ingest] Created datasets directory: {self.datasets_dir}")

    
    def _discover_files(self):
        """Return all CSV file paths in dataset directory."""
        return {
            os.path.join(self.datasets_dir, f)
            for f in os.listdir(self.datasets_dir)
            if f.lower().endswith(".csv")
        }

    def _load_new_files(self):
        """Detect new CSV files and register topics from config."""
        new_files = self._discover_files() - self.known_files
        for csv_path in new_files:
            self._parse_csv_and_config(csv_path)

    def _parse_csv_and_config(self, csv_path):
        """Load data and topic mapping from config."""
        config_path = csv_path.replace(".csv", ".config.json")
        if not os.path.exists(config_path):
            print(f"[Ingest] No config for {os.path.basename(csv_path)}, skipping.")
            return

        try:
            with open(config_path, "r") as cf:
                cfg = json.load(cf)
            with open(csv_path, "r") as f:
                rows = list(csv.DictReader(f))
        except Exception as e:
            print(f"[Ingest] Error reading {csv_path}: {e}")
            return

        topics = cfg.get("topics", {})
        desc = cfg.get("description", f"Auto-created from {os.path.basename(csv_path)}")

        for topic, fields in topics.items():
            if topic not in self.topic_rows:
                self.watcher.create_pending_topic(topic, desc)
                self.topic_rows[topic] = rows
                self.topic_fields[topic] = fields
                print(f"[Ingest] Registered topic: {topic} with fields {fields}")
            else:
                print(f"[Ingest] Topic already registered: {topic}")

        self.known_files.add(csv_path)

    
    def _payload_for_topic(self, topic, row):
        """Build a payload for the given topic."""
        payload = {
            "ts": row.get("ts") or datetime.utcnow().isoformat(),
            "server_id": row.get("server_id", "producer-node")
        }

        fields = self.topic_fields.get(topic, [])
        for f in fields:
            if f in row:
                try:
                    payload[f] = float(row[f]) if row[f] else 0
                except ValueError:
                    payload[f] = row[f]
        return payload

    
    def run(self):
        print(f"[Ingest] Started dynamic ingestion from {self.datasets_dir}")
        self._load_new_files()

        rows_per_topic = {}

        while self.running:
            self._load_new_files()


            with self.watcher.lock:
                active_topics = set(self.watcher.active_topics)


            for topic in active_topics:
                if topic not in rows_per_topic and topic in self.topic_rows:
                    rows_per_topic[topic] = iter(self.topic_rows[topic])
                    print(f"[Ingest] 🟢 Started streaming topic: {topic}")


            for topic in list(rows_per_topic.keys()):
                if topic not in active_topics:
                    print(f"[Ingest] 🔴 Stopping topic (revoked): {topic}")
                    del rows_per_topic[topic]


            if not rows_per_topic:
                time.sleep(self.poll_interval)
                continue


            for topic in list(rows_per_topic.keys()):
                try:
                    row = next(rows_per_topic[topic])
                    payload = self._payload_for_topic(topic, row)
                    self.out_q.put((topic, payload))
                    print(f"[Ingest] queued -> {topic}: {payload}")
                except StopIteration:

                    if self.loop and topic in self.topic_rows:
                        rows_per_topic[topic] = iter(self.topic_rows[topic])
                    else:
                        print(f"[Ingest]  Completed dataset for {topic}")
                        del rows_per_topic[topic]
                except Exception as e:
                    print(f"[Ingest] Error sending {topic}: {e}")

                # Sleep briefly between sends for fairness
                time.sleep(self.send_interval / max(1, len(rows_per_topic)))

            # Short pause between cycles
            time.sleep(0.2)

    def stop(self):
        self.running = False

