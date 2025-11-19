import yaml
import json
import logging
from benchmark.Consumer.ConsumerFactory import ConsumerFactory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


with open("./config.yml", 'r') as config_file:
    cfg = yaml.safe_load(config_file)

consumer = ConsumerFactory.get_consumer(consumer_type=cfg["Kafka"], consumer_id="result_consumer", group_id=cfg['spark']['result.py']['consumer_group'], bootstrap_servers=cfg['bootstrap_servers'], auto_offset_reset=cfg['spark']["result.py"]["offset_reset"], enable_auto_commit=True, value_deserializer=lambda m: json.loads(m.decode('utf-8')))

consumer.subscribe(cfg["spark"]["result.py"]["consume_from"])

end_to_end_latencies = {}
query_time_latencies={}
retries = 0
max_retries = cfg["spark"]["result.py"]["max_retries"]
while retries < max_retries:
    msg_pack = consumer.poll(timeout_ms=100)
    if not msg_pack:
        retries += 1
        continue
    for topic_partition, messages in msg_pack.items():
        for message in messages:
        
            data = message.value
            timestamp = message.timestamp
            window_start = data.get("window_start")
            campaign_id = data.get("campaign_id")
            ctr = data.get("ctr")
            max_produce_time=data.get("max_produce_time")
            print(max_produce_time)
            try:
                end_to_end_latencies[window_start]=max(end_to_end_latencies.get(window_start,0),timestamp-max_produce_time)
            except:
                pass
    retries = 0  

    
logger.info(f"Result.py: {end_to_end_latencies}")

