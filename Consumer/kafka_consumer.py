# kafka_consumer.py

from kafka import KafkaConsumer
import json
import threading
import time
import os
from config import BROKER_URL

class DynamicKafkaConsumer:
    """Kafka consumer supporting live subscribe/unsubscribe and file logging."""

    def __init__(self, log_dir="logs", group_id=None):
        self.consumer = None
        self.running = False
        self.lock = threading.Lock()
        self.current_topics = set()
        self.log_dir = log_dir
        self.consume_thread = None
        # Use provided group_id or get from config
        if group_id is None:
            from config import GROUP_ID
            self.group_id = GROUP_ID
        else:
            self.group_id = group_id
        # Ensure log directory exists
        os.makedirs(self.log_dir, exist_ok=True)

    def start_consumer(self, topics):
        """Subscribe dynamically to new active topics."""
        with self.lock:
            new_topics = set(topics)
            if new_topics == self.current_topics:
                return

            self.current_topics = new_topics
            print(f"[Consumer] Updating subscriptions → {list(new_topics)}")
            self._stop_existing_consumer()

            if not new_topics:
                print("[Consumer] No active topics currently subscribed.")
                return

            self.consumer = KafkaConsumer(
                *list(new_topics),
                bootstrap_servers=BROKER_URL,
                group_id=self.group_id,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else v
            )

            self.running = True
            self.consume_thread = threading.Thread(target=self._consume_loop, daemon=True)
            self.consume_thread.start()
            print(f"[Consumer] Subscribed successfully to: {list(new_topics)}")

    def _consume_loop(self):
        """Continuously read messages and log to file."""
        while self.running and self.consumer:
            try:
                for msg in self.consumer:
                    if not self.running:
                        break
                    formatted = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg.value}\n"
                    # print(formatted.strip())  # disabled for silent mode
                    self._log_to_file(msg.topic, formatted)
            except Exception as e:
                print(f"[Consumer] Error: {e}")
                break

    def _stop_existing_consumer(self):
        """Gracefully stop current consumer thread."""
        if self.consumer:
            try:
                self.running = False
                self.consumer.close()
                self.consumer = None
                if self.consume_thread and self.consume_thread.is_alive():
                    self.consume_thread.join(timeout=2)
                    if self.consume_thread.is_alive():
                        print("[Consumer] Warning: consume thread did not stop gracefully")
            except Exception as e:
                print(f"[Consumer] Stop error: {e}")

    def _log_to_file(self, topic, line):
        """Append received message to topic-specific log file."""
        try:
            log_file = os.path.join(self.log_dir, f"{topic}.log")
            with open(log_file, "a") as f:
                f.write(line)
        except Exception as e:
            print(f"[Logger] Failed to write to file for topic {topic}: {e}")

    def stop(self):
        """Stop consumer safely."""
        with self.lock:
            self._stop_existing_consumer()
            if self.current_topics:
                log_files = [os.path.join(self.log_dir, f"{topic}.log") for topic in self.current_topics]
                print(f"[Consumer] Stopped. Messages logged in '{self.log_dir}/' directory:")
                for log_file in log_files:
                    print(f"  - {log_file}")
            else:
                print(f"[Consumer] Stopped. Log directory: '{self.log_dir}/'")