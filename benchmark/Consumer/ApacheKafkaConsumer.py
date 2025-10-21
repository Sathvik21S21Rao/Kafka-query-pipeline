from benchmark.Consumer.Consumer import Consumer
from kafka import KafkaConsumer
import logging
import json

class ApacheKafkaConsumer(Consumer):
    def __init__(self, consumer_id, **kwargs):
        self.consumer_id = consumer_id
        if('value_deserializer' not in kwargs or kwargs.get('value_deserializer') is None):
            kwargs['value_deserializer']=lambda m: json.loads(m.decode('utf-8'))
        self.consumer = KafkaConsumer(**kwargs)

        logging.info("Apache Kafka Consumer %s initialized with bootstrap servers: %s", self.consumer_id, kwargs.get("bootstrap_servers"))
    def poll(self, timeout_ms):
        return self.consumer.poll(timeout_ms=timeout_ms)
    def close(self):
        self.consumer.close()
        logging.info("Apache Kafka Consumer %s closed", self.consumer_id)
    def subscribe(self, topics):
        self.consumer.subscribe(topics)
        logging.info("Apache Kafka Consumer %s subscribed to topics: %s", self.consumer_id, topics)