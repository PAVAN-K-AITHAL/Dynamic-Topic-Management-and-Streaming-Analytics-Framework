#!/usr/bin/env python3
import json, time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError


class PublisherThread:
   

    def __init__(self, broker, out_q):
        self.broker = broker
        self.out_q = out_q
        self.running = True

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[self.broker],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
                linger_ms=100,  
                acks="all",     
                compression_type="gzip",
                metadata_max_age_ms=5000,
                request_timeout_ms=10000,
                max_in_flight_requests_per_connection=5
            )
            print(f"[Publisher] Connected to broker: {self.broker}")
        except NoBrokersAvailable as e:
            print("[Publisher]  Cannot connect to broker:", e)
            self.producer = None

    def run(self):
        print("[Publisher] Started")
        if not self.producer:
            return

        while self.running:
            try:
                topic, msg = self.out_q.get(timeout=1)
                self.producer.send(topic, value=msg)
                print(f"[Publisher] sent -> {topic}: {msg}")

                
                self.producer._metadata.refresh()
            except KafkaTimeoutError:
                print("[Publisher]  Kafka timeout")
            except Exception as e:
                pass

            time.sleep(0.2)

    def stop(self):
        self.running = False
        try:
            if self.producer:
                self.producer.flush()
                self.producer.close(timeout=5)
                print("[Publisher]  Clean shutdown.")
        except Exception:
            pass

