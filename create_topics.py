import asyncio
import asyncpg
from dotenv import load_dotenv
from kafka.admin import KafkaAdminClient, NewTopic
import time



def cleanup_kafka_topics(topics:dict,bootstrap_servers:list|str):
    load_dotenv()

    """Clean up Kafka topics by deleting and recreating them"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='cleanup_client'
        )
        
        
        print("Cleaning up Kafka topics...")
        
        # Delete existing topics
        try:
            admin_client.delete_topics(list(topics.keys()), timeout_ms=10000)
            print(f"Deleted topics: {list(topics.keys())}")
            time.sleep(20)
        except Exception as e:
            print(f"Some topics might not exist or couldn't be deleted: {e}")
        
        # Use minimal valid topic configurations
        topic_configs = {
            'retention.ms': '604800000',  # 7 days retention
            'cleanup.policy': 'delete',
            'message.timestamp.type': 'LogAppendTime' 
        }
        new_topics = [NewTopic(name=topic, num_partitions=topics[topic]['partitions'], replication_factor=topics[topic]['replication_factor'], topic_configs=topic_configs) for topic in topics]
        
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



