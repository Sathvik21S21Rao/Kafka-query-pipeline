from benchmark.Producer.ProducerFactory import ProducerFactory
from benchmark.Consumer.ConsumerFactory import ConsumerFactory
import json
import yaml
import logging
import psycopg2
from dotenv import load_dotenv
import os
import time

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

with open('./config.yml') as f:
    cfg = yaml.safe_load(f)

consumer = ConsumerFactory.get_consumer(consumer_type=cfg["Kafka"], consumer_id="global_watermark_consumer", group_id=cfg['sql']['query.py']['consumer_group'], bootstrap_servers=cfg['bootstrap_servers'], auto_offset_reset=cfg['sql']["query.py"]["offset_reset"], enable_auto_commit=True, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
producer = ProducerFactory.get_producer(producer_type=cfg["Kafka"], producer_id="query_producer", bootstrap_servers=cfg['bootstrap_servers'], value_serializer=lambda m: json.dumps(m).encode('utf-8'), linger_ms=cfg["sql"]["query.py"]["linger_ms"],enable_idempotence=True)

NUM_PARTITIONS = cfg["num_partitions"]
consumer.subscribe(cfg["sql"]["query.py"]["consume_from"])

conn=psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)

query="""SELECT v.window_id, v.campaign_id, v.views, c.clicks, c.clicks::float/ NULLIF(v.views,0) AS ctr FROM (SELECT e.window_id, c.campaign_id, COUNT(*) as views FROM events e, mappings c WHERE e.ad_id = c.ad_id AND e.event_type='view' AND e.window_id < %s GROUP BY e.window_id, c.campaign_id) v LEFT JOIN (SELECT e.window_id, c.campaign_id, COUNT(*) as clicks FROM events e, mappings c WHERE e.ad_id = c.ad_id AND e.event_type='click' AND e.window_id < %s GROUP BY e.window_id, c.campaign_id) c ON v.window_id = c.window_id AND v.campaign_id = c.campaign_id ORDER BY v.window_id, ctr DESC"""

global_watermark=0
completed_windows=0
max_retries = cfg['sql']["query.py"]["max_retries"]
retries = 0

while retries < max_retries:
    message_pack = consumer.poll(timeout_ms=100)
    
    if not message_pack:
        retries += 1
        continue
    
    retries = 0
    
    for topic_partition, messages in message_pack.items():
        for message in messages:
            # deserialize message value
            global_watermark = message.value["watermark"]

            if global_watermark > completed_windows:
                start_time = time.time()
                with conn.cursor() as cur:
                    cur.execute(query, (global_watermark,global_watermark))
                    new_events = cur.fetchall()
                    get_max_processing_time_query="SELECT window_id, MAX(processing_time) AS max_processing_time, MAX(produce_time) AS max_produce_time FROM events WHERE window_id < %s GROUP BY window_id;"

                    cur.execute(get_max_processing_time_query,(global_watermark,))
                    rows=cur.fetchall()
                    producer.send(cfg['sql']["query.py"]["produce_to"], value={"results":new_events,"max_processing_times":rows})
                    cur.execute("DELETE FROM events WHERE window_id < %s", (global_watermark,))
                conn.commit()
                completed_windows = global_watermark
                end_time = time.time()
                logger.info(f"Query process: Completed processing for window {global_watermark-1} in {end_time - start_time:.2f} seconds. Time: {time.time_ns()}")

producer.flush()
producer.close()
consumer.close()
