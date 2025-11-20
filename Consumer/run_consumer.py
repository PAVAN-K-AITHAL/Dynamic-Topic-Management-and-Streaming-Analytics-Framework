# run_consumer.py

from kafka_consumer import DynamicKafkaConsumer
from topic_poller import TopicPoller
from subscription_poller import SubscriptionPoller
from config import ADMIN_BASE_URL
import requests
import time
import os

# Get consumer name and group ID from environment or use defaults from config
CONSUMER_NAME = os.getenv('CONSUMER_NAME', 'consumer-1')
GROUP_ID = os.getenv('GROUP_ID', 'dynamic-consumer-group')

def register_consumer():
    """Consumer registration is implicit in the new API - no separate registration needed."""
    print(f"[System] Consumer '{CONSUMER_NAME}' ready (registration not required).")

def fetch_saved_subscriptions():
    """Fetch previous subscriptions for this consumer."""
    try:
        r = requests.get(f"{ADMIN_BASE_URL}/subscriptions/{CONSUMER_NAME}")
        if r.status_code == 200:
            topics = [t["topic_name"] for t in r.json()]
            print(f"[System] Restored subscriptions from DB: {topics or 'None'}")
            return set(topics)
    except Exception as e:
        print(f"[System] Error fetching subscriptions: {e}")
    return set()

if __name__ == "__main__":
    print(f"[System] Starting Consumer: {CONSUMER_NAME}")
    print(f"[System] Consumer Group: {GROUP_ID}")
    # Each consumer gets its own log directory
    log_dir = f"logs/{CONSUMER_NAME}"
    consumer = DynamicKafkaConsumer(log_dir=log_dir, group_id=GROUP_ID)
    register_consumer()

    active_topics = []
    subscribed_topics = fetch_saved_subscriptions()

    def on_topic_update(new_topics):
        global active_topics
        active_topics = new_topics
        print("\n[TopicPoller] Active topics updated:")
        if active_topics:
            for i, t in enumerate(active_topics, start=1):
                print(f"  {i}. {t}")
        else:
            print("  (No active topics available)")

        # Auto re-subscribe to topics that are still active
        restored = [t for t in subscribed_topics if t in active_topics]
        if restored:
            consumer.start_consumer(restored)
            print(f"[System] Auto-subscribed to still-active topics: {restored}")
        else:
            print("[System] No previously active topics available.")
        print(">> ", end="", flush=True)

    poller = TopicPoller(on_topic_update)
    poller.start()

    def on_subscription_update(new_subscriptions):
        global subscribed_topics
        # Keep only subscriptions that are currently active topics
        filtered = [t for t in new_subscriptions if t in active_topics]
        subscribed_topics = set(filtered)
        consumer.start_consumer(list(subscribed_topics))
        print(f"[SubscriptionPoller] Updated subscriptions → {list(subscribed_topics)}")

    sub_poller = SubscriptionPoller(on_subscription_update)
    sub_poller.start()

    print("[System] Dynamic Kafka Consumer Started.")
    print("Polling Admin Node for ACTIVE topics and Subscriptions...\n")
    print("Control is managed via the Frontend UI. Press Ctrl+C to stop.\n")

    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n[System] Stopping consumer...")
        consumer.stop()
        poller.stop()
        sub_poller.stop()
