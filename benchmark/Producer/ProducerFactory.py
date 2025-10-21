from benchmark.Producer.ApacheKafkaProducer import ApacheKafkaProducer
from benchmark.Producer.Producer import Producer

class ProducerFactory:
    @staticmethod
    def get_producer(producer_type, producer_id, **kwargs) -> Producer:
        if producer_type == "apache":
            return ApacheKafkaProducer(producer_id, **kwargs)
        else:
            raise ValueError(f"Unknown producer type: {producer_type}")