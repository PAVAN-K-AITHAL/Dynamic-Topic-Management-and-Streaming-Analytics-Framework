#!/usr/bin/env python3
import threading, queue, signal, time, os, sys

# ── Import centralized config ────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config.settings import KAFKA_BROKER, ADMIN_URL, DATA_DIR

from topic_watcher import TopicWatcherThread
from ingest_thread import IngestThread
from publisher_thread import PublisherThread

BROKER     = KAFKA_BROKER
ADMIN_TOPICS_URL = f"{ADMIN_URL}/topics"
DATA_DIR_STR = str(DATA_DIR)

def main():
    q = queue.Queue(maxsize=5000)
    watcher   = TopicWatcherThread(broker=BROKER, admin_url=ADMIN_TOPICS_URL, poll_interval=5)
    ingestor  = IngestThread(watcher=watcher, out_q=q, datasets_dir=DATA_DIR_STR,
                             poll_interval=3, send_interval=1, loop=True)
    publisher = PublisherThread(broker=BROKER, out_q=q)

    t1 = threading.Thread(target=watcher.run,   daemon=True)
    t2 = threading.Thread(target=ingestor.run,  daemon=True)
    t3 = threading.Thread(target=publisher.run, daemon=True)

    print("[Main] Starting Producer Threads...")
    print(f"[Main] Broker: {BROKER}")
    print(f"[Main] Admin URL: {ADMIN_TOPICS_URL}")
    print(f"[Main] Data Dir: {DATA_DIR_STR}")
    t1.start(); t2.start(); t3.start()

    def shutdown(sig, frame):
        print("\n[Main] Shutting down gracefully...")
        watcher.stop(); ingestor.stop(); publisher.stop()
        time.sleep(1)
        raise SystemExit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
