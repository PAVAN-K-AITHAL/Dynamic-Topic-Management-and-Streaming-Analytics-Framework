#!/usr/bin/env python3
"""
alert_consumer.py

Consumes fraud alerts from Kafka topic 'fraud-alerts' and:
1. Stores them in the Admin DB via REST API
2. Logs alerts to file for auditing
3. Prints high-severity alerts to console

Usage:
    python alert_consumer.py
"""

import os
import sys
import json
import time
import threading
import requests
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config.settings import (
    KAFKA_BROKER, ADMIN_URL, TOPIC_FRAUD_ALERTS,
    TOPIC_MODEL_METRICS, CONSUMER_NAME
)

from kafka import KafkaConsumer


class FraudAlertConsumer:
    """Consumes fraud alerts from Kafka and stores them via Admin API."""

    def __init__(self, log_dir="logs/fraud_alerts"):
        self.broker = KAFKA_BROKER
        self.admin_url = ADMIN_URL
        self.consumer = None
        self.running = False
        self.log_dir = log_dir
        self.alert_count = 0
        self.high_severity_count = 0

        os.makedirs(self.log_dir, exist_ok=True)

    def start(self):
        """Start consuming fraud alerts."""
        print(f"[AlertConsumer] Starting — subscribing to '{TOPIC_FRAUD_ALERTS}'")
        print(f"[AlertConsumer] Kafka broker: {self.broker}")
        
        try:
            self.consumer = KafkaConsumer(
                TOPIC_FRAUD_ALERTS,
                bootstrap_servers=self.broker,
                group_id="fraud-alert-consumer",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else {},
            )
            self.running = True
            print(f"[AlertConsumer] ✅ Connected and listening")
        except Exception as e:
            print(f"[AlertConsumer] ❌ Failed to connect: {e}")
            return

        while self.running:
            try:
                for msg in self.consumer:
                    if not self.running:
                        break
                    self._process_alert(msg.value)
            except Exception as e:
                print(f"[AlertConsumer] Error: {e}")
                time.sleep(1)

    def _process_alert(self, alert):
        """Process a single fraud alert."""
        self.alert_count += 1
        
        fraud_score = alert.get('fraud_score', 0)
        is_fraud = alert.get('is_fraud', False)
        txn_id = alert.get('transaction_id', 'unknown')
        amount = alert.get('amount', 0)
        user_id = alert.get('user_id', 'unknown')
        
        # Log severity
        if fraud_score > 0.8:
            severity = "🔴 HIGH"
            self.high_severity_count += 1
        elif fraud_score > 0.5:
            severity = "🟡 MEDIUM"
        else:
            severity = "🟢 LOW"
        
        # Console output for high-severity alerts
        if fraud_score > 0.5:
            print(f"\n[AlertConsumer] {severity} FRAUD ALERT #{self.alert_count}")
            print(f"  Transaction: {txn_id}")
            print(f"  User: {user_id}")
            print(f"  Amount: ${amount:.2f}")
            print(f"  Fraud Score: {fraud_score:.4f}")
            
            # Show explanation if available
            explanation = alert.get('explanation')
            if explanation:
                print(f"  Top contributing features:")
                for feat in explanation[:3]:
                    print(f"    - {feat.get('feature')}: {feat.get('direction')} "
                          f"(SHAP: {feat.get('shap_value', 0):.4f})")
            print()
        
        # Store in Admin DB
        self._store_alert(alert)
        
        # Log to file
        self._log_to_file(alert)
        
        # Progress reporting
        if self.alert_count % 100 == 0:
            print(f"[AlertConsumer] Processed {self.alert_count} alerts "
                  f"({self.high_severity_count} high severity)")

    def _store_alert(self, alert):
        """Store fraud alert via Admin API."""
        try:
            requests.post(
                f"{self.admin_url}/fraud/alerts",
                json=alert,
                timeout=3
            )
        except Exception as e:
            # Non-critical — log and continue
            pass

    def _log_to_file(self, alert):
        """Append alert to log file."""
        try:
            log_file = os.path.join(self.log_dir, "fraud_alerts.jsonl")
            with open(log_file, "a") as f:
                alert_line = json.dumps({
                    **alert,
                    "_logged_at": datetime.utcnow().isoformat()
                })
                f.write(alert_line + "\n")
        except Exception as e:
            pass

    def stop(self):
        """Stop the consumer."""
        self.running = False
        if self.consumer:
            try:
                self.consumer.close()
            except Exception:
                pass
        print(f"[AlertConsumer] Stopped. Total alerts: {self.alert_count}, "
              f"High severity: {self.high_severity_count}")
        print(f"[AlertConsumer] Logs saved to: {self.log_dir}/fraud_alerts.jsonl")


def main():
    consumer = FraudAlertConsumer()
    
    import signal
    def shutdown(sig, frame):
        print("\n[AlertConsumer] Shutting down...")
        consumer.stop()
        raise SystemExit(0)
    
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    
    consumer.start()


if __name__ == "__main__":
    main()
