from benchmark.Consumer.ConsumerFactory import ConsumerFactory
import yaml
with open('config.yml', 'r') as f:
    cfg = yaml.safe_load(f)

consumer=ConsumerFactory.get_consumer(consumer_id="spark_watermark_consumer",consumer_type="apache",bootstrap_servers=cfg["kafka"]["bootstrap_servers"],group_id="spark_watermark_group")

consumer.subscribe(["watermark"])

max_retries = cfg["spark"]["watermark.py"]["max_retries"]
retries = 0

while retries < max_retries:
    msg = consumer.poll(timeout_ms=100)
    for topic_partition, messages in msg.items():
        for message in messages:
            print(f"[WM-CONSUMER] Received watermark message: {message.value}")

