from benchmark.Producer.Producer import Producer
from kafka import KafkaProducer
import logging
import json

class ApacheKafkaProducer(Producer):
    def __init__(self, producer_id, **kwargs):
        self.producer_id = producer_id
        # remember whether a value_serializer was provided so we don't double-serialize
        self._has_value_serializer = 'value_serializer' in kwargs and kwargs.get('value_serializer') is not None
        self.producer = KafkaProducer(**kwargs)
        logging.info("Apache Kafka Producer %s initialized with bootstrap servers: %s", self.producer_id, kwargs.get("bootstrap_servers"))

    def send(self, topic, value):
        try:
            if self._has_value_serializer:
                self.producer.send(topic, value)
            else:
                if isinstance(value, bytes):
                    payload = value
                elif isinstance(value, str):
                    payload = value.encode('utf-8')
                else:
                    payload = json.dumps(value).encode('utf-8')
                self.producer.send(topic, payload)
        except Exception as e:
            logging.error("Failed to send message to topic %s: %s", topic, str(e))

    def close(self):
        self.producer.close()
        logging.info("Apache Kafka Producer %s closed", self.producer_id)
    
    def flush(self):
        self.producer.flush()
