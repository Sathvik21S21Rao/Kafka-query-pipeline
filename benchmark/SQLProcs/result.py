from benchmark.Consumer.ConsumerFactory import ConsumerFactory
import yaml
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


with open("./config.yml", 'r') as config_file:
    cfg = yaml.safe_load(config_file)

consumer = ConsumerFactory.get_consumer(consumer_type=cfg["Kafka"], consumer_id="result_consumer", group_id=cfg['sql']['result.py']['consumer_group'], bootstrap_servers=cfg['bootstrap_servers'], auto_offset_reset=cfg['sql']["result.py"]["offset_reset"], enable_auto_commit=True, value_deserializer=lambda m: json.loads(m.decode('utf-8')))

consumer.subscribe(cfg["sql"]["result.py"]["consume_from"])

latencies = {}
retries = 0
max_retries = cfg["sql"]["result.py"]["max_retries"]
while retries < max_retries:
    msg_pack = consumer.poll(timeout_ms=100)
    if not msg_pack:
        retries += 1
        continue
    for topic_partition, messages in msg_pack.items():
        for message in messages:
            res = message.value
            timestamp = message.timestamp
            latencies.update({row[0]: {"query_time_latency": (timestamp - row[1]), "produce_time_latency": (row[1] - row[2])} for row in res["max_processing_times"]})


    
logger.info(f"Result.py: {latencies}")
