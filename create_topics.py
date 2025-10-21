import asyncio
import asyncpg
from dotenv import load_dotenv
from kafka.admin import KafkaAdminClient, NewTopic
import time



def cleanup_kafka_topics():
    load_dotenv()

    """Clean up Kafka topics by deleting and recreating them"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=['127.0.0.1:9092'],
            client_id='cleanup_client'
        )
        
        topics_to_clean = ['event', 'query_results', 'local_watermarks', 'global_watermark']
        
        print("Cleaning up Kafka topics...")
        
        # Delete existing topics
        try:
            admin_client.delete_topics(topics_to_clean, timeout_ms=10000)
            print(f"Deleted topics: {topics_to_clean}")
            time.sleep(20)
        except Exception as e:
            print(f"Some topics might not exist or couldn't be deleted: {e}")
        
        # Use minimal valid topic configurations
        topic_configs = {
            'retention.ms': '604800000',  # 7 days retention
            'cleanup.policy': 'delete',
            'message.timestamp.type': 'LogAppendTime' 
        }
        
        new_topics = [
            NewTopic(name='event', num_partitions=5, replication_factor=1, topic_configs=topic_configs),
            NewTopic(name='query_results', num_partitions=1, replication_factor=1, topic_configs=topic_configs),
            NewTopic(name='local_watermarks', num_partitions=1, replication_factor=1, topic_configs=topic_configs),
            NewTopic(name='global_watermark', num_partitions=1, replication_factor=1, topic_configs=topic_configs)
        ]
        
        try:
            admin_client.create_topics(new_topics, timeout_ms=10000)
            print("Recreated topics successfully!")
            time.sleep(10)
        except Exception as e:
            print(f"Error recreating topics (they might already exist): {e}")
        
        admin_client.close()
        print("Kafka cleanup completed successfully!")
        
    except Exception as e:
        print(f"Error cleaning up Kafka topics: {e}")



