#!/usr/bin/env python3
import threading, queue, signal, time
from topic_watcher import TopicWatcherThread
from ingest_thread import IngestThread
from publisher_thread import PublisherThread

BROKER     = "10.147.19.93:9092"
ADMIN_URL  = "http://10.147.19.93:5000/topics"
DATA_DIR   = "/home/pes1ug23cs420/173_Project2_BD/producer/data"

def main():
    q = queue.Queue(maxsize=5000)
    watcher   = TopicWatcherThread(broker=BROKER, admin_url=ADMIN_URL, poll_interval=5)
    ingestor  = IngestThread(watcher=watcher, out_q=q, datasets_dir=DATA_DIR,
                             poll_interval=3, send_interval=1, loop=True)
    publisher = PublisherThread(broker=BROKER, out_q=q)

    t1 = threading.Thread(target=watcher.run,   daemon=True)
    t2 = threading.Thread(target=ingestor.run,  daemon=True)
    t3 = threading.Thread(target=publisher.run, daemon=True)

    print("[Main] Starting Producer Threads...")
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

