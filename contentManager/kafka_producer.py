import logging
import json
import time
from kafka import KafkaProducer

class Producer:
    def __init__(self, servers, topic):
        self.producer = KafkaProducer(bootstrap_servers=servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.topic = topic

    def setTopic(topic):
        self.topic = topic

    def produce(self, record):
        try:
            print(self.topic)
            self.producer.send(self.topic, record)
            time.sleep(1)
            print("flushing")
            self.flush(timeout=1.0)
        except Exception as e:
            print(e)

    def flush(self, timeout=None):
        self.producer.flush(timeout=timeout)

    def close(self):
        try:
            if self.producer:
                self.producer.close()
            logging.Handler.close(self)
        finally:
            self.release()