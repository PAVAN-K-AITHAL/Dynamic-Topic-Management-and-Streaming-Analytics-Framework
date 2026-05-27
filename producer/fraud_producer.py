#!/usr/bin/env python3
"""
fraud_producer.py

Specialized producer for the fraud detection pipeline.
Reads creditcard.csv row by row, assigns synthetic user/merchant IDs,
adds realistic timestamps, and streams to Kafka at configurable speed.

Can run standalone OR work with the existing hot-folder IngestThread pattern.

Usage:
    python fraud_producer.py                    # Default 10x speed
    python fraud_producer.py --speed 100        # 100x speed
    python fraud_producer.py --speed 0          # Max throughput (no delay)
"""

import os
import sys
import csv
import json
import time
import hashlib
import signal
import argparse
import threading
import queue
from datetime import datetime, timedelta

# ── Import centralized config ────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config.settings import (
    KAFKA_BROKER, ADMIN_URL, DATA_DIR, REPLAY_SPEED,
    TOPIC_RAW_TRANSACTIONS
)
from topic_watcher import TopicWatcherThread
from publisher_thread import PublisherThread


class FraudProducer:
    """
    Reads creditcard.csv and streams transactions to Kafka.
    
    Enhancements over basic IngestThread:
    - Assigns synthetic user_id based on V1-V3 feature clustering
    - Assigns synthetic merchant_id based on V4-V6 features
    - Converts Time column (seconds from first txn) to ISO timestamps
    - Configurable replay speed
    - Transaction ID generation
    """

    def __init__(self, csv_path, out_q, replay_speed=10.0, loop=True):
        self.csv_path = csv_path
        self.out_q = out_q
        self.replay_speed = replay_speed
        self.loop = loop
        self.running = True
        self.txn_count = 0
        self.fraud_count = 0
        self.start_time = None
        
        # Base timestamp — pretend transactions start "now"
        self.base_timestamp = datetime.utcnow()
        
    def _generate_user_id(self, row):
        """
        Assign a synthetic user_id based on V1-V3 feature ranges.
        This groups similar transaction patterns into ~100 virtual users.
        """
        try:
            v1 = float(row.get('V1', 0))
            v2 = float(row.get('V2', 0))
            v3 = float(row.get('V3', 0))
            # Bucket V1 into 10 groups, V2 into 10 groups → ~100 users
            bucket = (int((v1 + 5) / 2) % 10) * 10 + (int((v2 + 5) / 2) % 10)
            bucket = max(0, min(99, bucket))
            return f"user_{bucket:03d}"
        except (ValueError, TypeError):
            return "user_000"

    def _generate_merchant_id(self, row):
        """
        Assign a synthetic merchant_id based on V4-V6 features.
        ~50 virtual merchants.
        """
        try:
            v4 = float(row.get('V4', 0))
            v5 = float(row.get('V5', 0))
            # Hash to get consistent merchant assignment
            key = f"{int(v4*10)}{int(v5*10)}"
            bucket = int(hashlib.md5(key.encode()).hexdigest()[:4], 16) % 50
            return f"merchant_{bucket:03d}"
        except (ValueError, TypeError):
            return "merchant_000"

    def _build_payload(self, row):
        """Build enriched transaction payload from CSV row."""
        # Convert Time (seconds from start) to ISO timestamp
        try:
            time_offset = float(row.get('Time', 0))
        except (ValueError, TypeError):
            time_offset = 0
        
        timestamp = self.base_timestamp + timedelta(seconds=time_offset)
        
        # Generate transaction ID
        self.txn_count += 1
        txn_id = f"txn_{self.txn_count:08d}"
        
        # Track fraud count
        is_fraud = str(row.get('Class', '0')).strip() == '1'
        if is_fraud:
            self.fraud_count += 1
        
        # Build payload with all features
        payload = {
            "transaction_id": txn_id,
            "timestamp": timestamp.isoformat(),
            "user_id": self._generate_user_id(row),
            "merchant_id": self._generate_merchant_id(row),
        }
        
        # Add all original features
        for key in ['Time', 'V1', 'V2', 'V3', 'V4', 'V5', 'V6', 'V7',
                     'V8', 'V9', 'V10', 'V11', 'V12', 'V13', 'V14',
                     'V15', 'V16', 'V17', 'V18', 'V19', 'V20', 'V21',
                     'V22', 'V23', 'V24', 'V25', 'V26', 'V27', 'V28',
                     'Amount', 'Class']:
            try:
                payload[key] = float(row[key]) if row.get(key) else 0.0
            except (ValueError, TypeError):
                payload[key] = 0.0
        
        return payload

    def _compute_delay(self, current_time_val, prev_time_val):
        """Compute delay between sends based on replay speed."""
        if self.replay_speed <= 0:
            return 0  # Max throughput
        
        try:
            time_diff = float(current_time_val) - float(prev_time_val)
            if time_diff <= 0:
                return 0.001  # Minimum delay for same-time transactions
            return time_diff / self.replay_speed
        except (ValueError, TypeError):
            return 0.01

    def run(self):
        """Main loop: read CSV, build payloads, enqueue for publishing."""
        print(f"[FraudProducer] Starting — CSV: {self.csv_path}")
        print(f"[FraudProducer] Replay speed: {self.replay_speed}x")
        print(f"[FraudProducer] Loop: {self.loop}")
        
        self.start_time = time.time()
        iteration = 0
        
        while self.running:
            iteration += 1
            prev_time_val = 0
            
            try:
                with open(self.csv_path, 'r') as f:
                    reader = csv.DictReader(f)
                    row_num = 0
                    
                    for row in reader:
                        if not self.running:
                            break
                        
                        row_num += 1
                        payload = self._build_payload(row)
                        
                        # Enqueue for publishing
                        try:
                            self.out_q.put(
                                (TOPIC_RAW_TRANSACTIONS, payload),
                                timeout=5
                            )
                        except queue.Full:
                            print("[FraudProducer] ⚠️ Queue full, waiting...")
                            self.out_q.put((TOPIC_RAW_TRANSACTIONS, payload))
                        
                        # Progress logging every 10,000 rows
                        if row_num % 10000 == 0:
                            elapsed = time.time() - self.start_time
                            rate = self.txn_count / elapsed if elapsed > 0 else 0
                            print(f"[FraudProducer] Processed {row_num:,} rows "
                                  f"(Total: {self.txn_count:,}, Fraud: {self.fraud_count:,}, "
                                  f"Rate: {rate:.0f} txn/s)")
                        
                        # Compute and apply delay
                        current_time_val = row.get('Time', 0)
                        delay = self._compute_delay(current_time_val, prev_time_val)
                        prev_time_val = current_time_val
                        
                        if delay > 0:
                            time.sleep(min(delay, 1.0))  # Cap delay at 1 second
                
            except FileNotFoundError:
                print(f"[FraudProducer] ❌ File not found: {self.csv_path}")
                print("[FraudProducer] Waiting for file...")
                time.sleep(5)
                continue
            except Exception as e:
                print(f"[FraudProducer] ❌ Error: {e}")
                time.sleep(1)
                continue
            
            elapsed = time.time() - self.start_time
            print(f"[FraudProducer] ✅ Iteration {iteration} complete — "
                  f"{self.txn_count:,} total txns, {self.fraud_count:,} fraud, "
                  f"{elapsed:.1f}s elapsed")
            
            if not self.loop:
                print("[FraudProducer] Single pass complete. Stopping.")
                break
            
            # Reset base timestamp for next loop
            self.base_timestamp = datetime.utcnow()
            print("[FraudProducer] 🔄 Restarting from beginning...")
            time.sleep(1)

    def stop(self):
        self.running = False

    def get_stats(self):
        elapsed = time.time() - self.start_time if self.start_time else 0
        return {
            'total_transactions': self.txn_count,
            'total_fraud': self.fraud_count,
            'elapsed_seconds': elapsed,
            'rate_per_sec': self.txn_count / elapsed if elapsed > 0 else 0
        }


def main():
    parser = argparse.ArgumentParser(description='Fraud Detection Transaction Producer')
    parser.add_argument('--speed', type=float, default=REPLAY_SPEED,
                        help=f'Replay speed multiplier (default: {REPLAY_SPEED}x, 0=max throughput)')
    parser.add_argument('--no-loop', action='store_true',
                        help='Stop after one pass through the dataset')
    parser.add_argument('--csv', type=str, default=None,
                        help='Path to creditcard.csv (default: auto-detect in data dir)')
    args = parser.parse_args()

    # Find the CSV
    csv_path = args.csv
    if csv_path is None:
        csv_path = os.path.join(str(DATA_DIR), 'creditcard.csv')
    
    if not os.path.exists(csv_path):
        print(f"[FraudProducer] ❌ Dataset not found at: {csv_path}")
        print(f"[FraudProducer] Download from: https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud")
        print(f"[FraudProducer] Place creditcard.csv in: {DATA_DIR}")
        sys.exit(1)

    print(f"[FraudProducer] Dataset: {csv_path} ({os.path.getsize(csv_path) / 1e6:.1f} MB)")
    
    # Setup queue and threads
    q = queue.Queue(maxsize=5000)
    
    # Topic watcher — ensures 'raw-transactions' topic exists in Kafka + Admin DB
    admin_topics_url = f"{ADMIN_URL}/topics"
    watcher = TopicWatcherThread(broker=KAFKA_BROKER, admin_url=admin_topics_url, poll_interval=5)
    
    # Publisher — sends messages from queue to Kafka
    publisher = PublisherThread(broker=KAFKA_BROKER, out_q=q)
    
    # Fraud producer — reads CSV and builds payloads
    producer = FraudProducer(
        csv_path=csv_path,
        out_q=q,
        replay_speed=args.speed,
        loop=not args.no_loop
    )

    # Register the raw-transactions topic as pending
    watcher.create_pending_topic(
        TOPIC_RAW_TRANSACTIONS,
        "Credit card transactions for fraud detection"
    )

    # Start threads
    t_watcher = threading.Thread(target=watcher.run, daemon=True)
    t_publisher = threading.Thread(target=publisher.run, daemon=True)
    t_producer = threading.Thread(target=producer.run, daemon=True)

    print("[FraudProducer] Starting all threads...")
    t_watcher.start()
    t_publisher.start()
    
    # Wait a moment for topic to be created/approved
    print("[FraudProducer] Waiting for topic approval...")
    time.sleep(3)
    
    t_producer.start()

    def shutdown(sig, frame):
        print("\n[FraudProducer] Shutting down gracefully...")
        stats = producer.get_stats()
        print(f"[FraudProducer] Final stats: {json.dumps(stats, indent=2)}")
        producer.stop()
        watcher.stop()
        publisher.stop()
        time.sleep(1)
        raise SystemExit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
