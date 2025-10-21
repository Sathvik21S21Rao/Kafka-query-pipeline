from benchmark.Consumer.Consumer import Consumer
from benchmark.Consumer.ApacheKafkaConsumer import ApacheKafkaConsumer

class ConsumerFactory:
    @staticmethod
    def get_consumer(consumer_type, consumer_id, **kwargs) -> Consumer:
        if consumer_type == "apache":
            return ApacheKafkaConsumer(consumer_id, **kwargs)
        else:
            raise ValueError(f"Unknown consumer type: {consumer_type}")