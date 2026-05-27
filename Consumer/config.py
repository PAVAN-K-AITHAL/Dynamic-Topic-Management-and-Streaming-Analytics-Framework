"""
Consumer configuration — imports from centralized config.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config.settings import KAFKA_BROKER, ADMIN_URL, CONSUMER_GROUP_ID, POLL_INTERVAL as _POLL_INTERVAL, CONSUMER_NAME as _CONSUMER_NAME

BROKER_URL = KAFKA_BROKER
ADMIN_BASE_URL = ADMIN_URL
GROUP_ID = CONSUMER_GROUP_ID
POLL_INTERVAL = _POLL_INTERVAL
CONSUMER_NAME = _CONSUMER_NAME