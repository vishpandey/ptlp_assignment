import logging
import json
import time
from kafka import KafkaProducer
from kafkaHandler import logger

class Producer:
    def __init__(self, servers, topic):
        self.producer = KafkaProducer(bootstrap_servers=servers, 
                                value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.topic = topic

    def produce(self, record):
        try:
            self.producer.send(self.topic, record)
            time.sleep(1)
            self.flush(timeout=1.0)
        except Exception as e:
            logger.error(e)

    def flush(self, timeout=None):
        self.producer.flush(timeout=timeout)

    def close(self):
        try:
            if self.producer:
                self.producer.close()
            logging.Handler.close(self)
        finally:
            self.release()