from benchmark.Consumer.ConsumerFactory import ConsumerFactory
from benchmark.Producer.ProducerFactory import ProducerFactory
import yaml
import sys
import json
import logging
from dotenv import load_dotenv
import psycopg2
import os
import time

load_dotenv()
conn=psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

with open('./config.yml') as f:
    cfg = yaml.safe_load(f)

producer=ProducerFactory.get_producer(producer_type=cfg["Kafka"], producer_id="db_insert"+sys.argv[1],bootstrap_servers=cfg['bootstrap_servers'],value_serializer=lambda v: json.dumps(v).encode('utf-8'),linger_ms=cfg["sql"]["db_insert.py"]["linger_ms"],enable_idempotence=True)
consumer=ConsumerFactory.get_consumer(consumer_type=cfg["Kafka"], consumer_id="consumer_db_insert"+sys.argv[1], group_id=cfg['sql']['db_insert.py']['consumer_group'],bootstrap_servers=cfg['bootstrap_servers'],auto_offset_reset=cfg["sql"]["db_insert.py"]["offset_reset"],enable_auto_commit=True,value_deserializer=lambda m: json.loads(m.decode('utf-8')))
consumer.subscribe(cfg["sql"]["db_insert.py"]["consume_from"])
throughput=int(sys.argv[2])
max_retries = cfg['sql']["db_insert.py"]["max_retries"]
retry_count = 0
total_producer_to_kafka_latency=0
total_events=0
buffer=[]
global_watermark=0
while retry_count < max_retries:
    message_batch = consumer.poll(timeout_ms=100)
    
    if not message_batch:
        retry_count += 1
        continue
    
    retry_count = 0  # Reset retry count when messages are received
    
    for topic_partition, messages in message_batch.items():
        for message in messages:
            if message.topic == "event":
                # buffer events to write into psql in bulk
                event = message.value
                total_producer_to_kafka_latency+=message.timestamp-event["produce_time"]
                total_events+=1
                event["produce_time"]=message.timestamp
                
                buffer.append(event.copy())
                
                if global_watermark is not None and event["window_id"] < global_watermark:
                    continue
                
                if len(buffer) >= throughput//10:
                    with conn.cursor() as cur:
                        cur.executemany("""
                        INSERT INTO events (user_id, page_id, ad_id, ad_type, ns_time, ip_address, window_id,processing_time,event_type,produce_time)
                        VALUES (%s, %s, %s, %s, %s, %s, %s,(EXTRACT(EPOCH FROM CLOCK_TIMESTAMP()) * 1000), %s,%s)
                        """, [(e["user_id"], e["page_id"], e["ad_id"], e["ad_type"], e["ns_time"], e["ip_address"], e["window_id"], e["event_type"], e["produce_time"]) for e in buffer])
                    conn.commit()

                    local_watermark = min([e["window_id"] for e in buffer])
                    producer.send(cfg['sql']["db_insert.py"]["produce_to"], value={"partition":message.partition,"watermark": local_watermark,"current_time":time.time_ns()//1_000_000})
                    producer.flush()
                    buffer.clear()
                
                if len(buffer)%(throughput//100)==0 and buffer:
                    local_watermark = min([e["window_id"] for e in buffer])
                    producer.send(cfg['sql']["db_insert.py"]["produce_to"], value={"partition":message.partition,"watermark": local_watermark,"current_time":time.time_ns()//1_000_000})
                
            
            else:
                global_watermark = message.value["watermark"]
producer.flush()

logger.info(f"Average producer to kafka latency in db_insert process: {total_producer_to_kafka_latency/total_events} ms")

conn.close()
consumer.close()
producer.close()    