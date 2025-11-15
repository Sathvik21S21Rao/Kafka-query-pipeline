import yaml
from benchmark.Generator import generator
from benchmark.Producer.ProducerFactory import ProducerFactory
import json
import sys
import pickle
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

with open("./data.pkl", "rb") as fh:
    user_ids = pickle.load(fh)
    page_ids = pickle.load(fh)
    ad_ids = pickle.load(fh)
    ad_type_mapping = pickle.load(fh)
    event_type = pickle.load(fh)
    campaign_id = pickle.load(fh)

with open('./config.yml') as f:
    cfg = yaml.safe_load(f)
    producer = ProducerFactory.get_producer(producer_type=cfg["Kafka"], producer_id=sys.argv[1], bootstrap_servers=cfg['bootstrap_servers'], value_serializer=lambda v: json.dumps(v).encode('utf-8'), linger_ms=cfg["producer.py"]["linger_ms"],enable_idempotence=True)
    if cfg["mode"]=="poisson":
        gen=generator.SteadyPoissonGenerator(throughput=int(sys.argv[2]),producer=producer,user_ids=user_ids,page_ids=page_ids,ad_ids=ad_ids,ad_type_mapping=ad_type_mapping,event_type=event_type, topic=cfg["producer.py"]["produce_to"])
    elif cfg["mode"]=="mmmp":
        gen=generator.MMMPGenerator(throughput=int(sys.argv[2]),producer=producer,lambda_H=cfg["lambda_H"],d_H=cfg["d_H"],d_L=cfg["d_L"],user_ids=user_ids,page_ids=page_ids,ad_ids=ad_ids,ad_type_mapping=ad_type_mapping,event_type=event_type, topic=cfg["producer.py"]["produce_to"])

simulated_time=0
windowid=0
itr=0
events_sent=0
while simulated_time<cfg["duration"]*1e9:
    events_sent+=gen.generate_events(windowid,simulated_time)
    if (itr+1)%10==0:
        logger.info(f"Producer {sys.argv[1]} sent {events_sent} events for window {windowid} starting at simulated time {simulated_time}")
        events_sent=0
    simulated_time+=1e9
    itr+=1
    windowid=itr//10
    time.sleep(1)

logger.info(f"Producer {sys.argv[1]} finished sending events.")
producer.flush()
producer.close()

