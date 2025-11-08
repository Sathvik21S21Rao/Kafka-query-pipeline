from benchmark.Producer.ProducerFactory import ProducerFactory
from benchmark.Consumer.ConsumerFactory import ConsumerFactory
import sys
import json
import yaml
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

with open('./config.yml') as f:
    cfg = yaml.safe_load(f)

consumer = ConsumerFactory.get_consumer(consumer_type=cfg["Kafka"], consumer_id="local_watermark_consumer", group_id=cfg['sql']['global_watermark.py']['consumer_group'], bootstrap_servers=cfg['bootstrap_servers'], auto_offset_reset=cfg["sql"]["global_watermark.py"]["offset_reset"], enable_auto_commit=True, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
producer = ProducerFactory.get_producer(producer_type=cfg["Kafka"], producer_id="global_watermark_producer", bootstrap_servers=cfg['bootstrap_servers'], value_serializer=lambda m: json.dumps(m).encode('utf-8'), linger_ms=cfg["sql"]["global_watermark.py"]["linger_ms"],enable_idempotence=True)
NUM_PARTITIONS = cfg["num_partitions"]
consumer.subscribe(cfg["sql"]["global_watermark.py"]["consume_from"])

local_watermarks = {i: float('inf') for i in range(NUM_PARTITIONS)}
max_retries = cfg["sql"]["global_watermark.py"]["max_retries"]
retry_count = 0
total_producer_to_kafka_latency=0
total_events=0

while retry_count < max_retries:
    message_batch = consumer.poll(timeout_ms=100)
    
    if not message_batch:
        retry_count += 1
        continue
    
    retry_count = 0
    
    for topic_partition, messages in message_batch.items():
        for message in messages:
            local_watermarks[message.value["partition"]] = min(local_watermarks[message.value["partition"]], message.value["watermark"])
            total_producer_to_kafka_latency+=message.timestamp-message.value["current_time"]
            total_events+=1
        if all(w != float('inf') for w in local_watermarks.values()):
            producer.send(cfg['sql']["global_watermark.py"]["produce_to"], value={'watermark': int(min(local_watermarks.values()))})
            local_watermarks = {i: float('inf') for i in range(NUM_PARTITIONS)}
            producer.flush()
logger.info(f"Global Watermark: Average producer to kafka latency: {total_producer_to_kafka_latency/total_events} ms over {total_events} events")
