from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from dotenv import load_dotenv
import time


def create_kafka_topics(topics: dict, bootstrap_servers: list | str):
    """
    Create Kafka topics if they don't already exist.

    Args:
        topics (dict): Dict where keys are topic names and values are dicts with 'partitions' and 'replication_factor'.
        bootstrap_servers (list | str): Kafka bootstrap server(s).
    """
    load_dotenv()

    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='create_topics'
        )

        # Get list of existing topics
        existing_topics = admin_client.list_topics()
        print(f"Existing topics: {existing_topics}")

        # Determine which topics are missing
        missing_topics = {
            name: config for name, config in topics.items() if name not in existing_topics
        }
        print(missing_topics)

        if not missing_topics:
            print("All topics already exist — nothing to create.")
            return

        # Define default topic configs
        topic_configs = {
            'retention.ms': '604800000',  # 7 days
            'cleanup.policy': 'delete',
            'message.timestamp.type': 'LogAppendTime'
        }

        # Build list of NewTopic objects for missing topics
        new_topics = [
            NewTopic(
                name=name,
                num_partitions=cfg['num_partitions'],
                replication_factor=cfg['replication_factor'],
                topic_configs=topic_configs
            )
            for name, cfg in missing_topics.items()
        ]

        print(f"Creating topics: {list(missing_topics.keys())}")
        admin_client.create_topics(new_topics, timeout_ms=10000)
        print("Missing topics created successfully!")
        time.sleep(60)

    except TopicAlreadyExistsError as e:
        print(f"Some topics already exist: {e}")
    except Exception as e:
        print(f"Error creating Kafka topics: {e}")
    finally:
        try:
            admin_client.close()
        except Exception:
            pass

