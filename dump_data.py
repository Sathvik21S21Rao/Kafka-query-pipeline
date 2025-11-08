import uuid
import random
import pickle
import psycopg2
import os
from dotenv import load_dotenv

def dump_data(pickle_path="data.pkl",type:str="sql"):
    load_dotenv()

    user_ids = [str(uuid.uuid4()) for _ in range(10000)]
    page_ids = [str(uuid.uuid4()) for _ in range(100)]
    ad_ids = [str(uuid.uuid4()) for _ in range(1000)]
    ad_type_mapping = {ad_id: random.choice(["image", "video", "carousel"]) for ad_id in ad_ids}
    event_type = ['view', 'click', 'purchase']
    campaign_id = [i for i in range(100)]
    ad_to_campaign_mapping = {ad_id: campaign_id[i % len(campaign_id)] for i, ad_id in enumerate(ad_ids)}

    # write pickled data
    with open(pickle_path, "wb") as fh:
        pickle.dump(user_ids, fh)
        pickle.dump(page_ids, fh)
        pickle.dump(ad_ids, fh)
        pickle.dump(ad_type_mapping, fh)
        pickle.dump(event_type, fh)
        pickle.dump(campaign_id, fh)
        
    with open("ad_to_campaign_mapping.pkl", "wb") as fh:
        pickle.dump(ad_to_campaign_mapping, fh)

    if type.lower() == "sql":
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )

        try:
            with conn.cursor() as cursor:
                cursor.execute("DROP TABLE IF EXISTS events")
                cursor.execute("DROP INDEX IF EXISTS idx_events_window_id")
                cursor.execute("DROP INDEX IF EXISTS idx_events_event_type")
                cursor.execute("DROP INDEX IF EXISTS idx_events_ad_id_event_type_window")
                cursor.execute("DROP TABLE IF EXISTS mappings")

                cursor.execute("""
                    CREATE TABLE events (
                        user_id VARCHAR(36),
                        page_id VARCHAR(36),
                        ad_id VARCHAR(36),
                        ad_type VARCHAR(40),
                        event_type VARCHAR(24),
                        ns_time BIGINT,
                        ip_address VARCHAR(36),
                        processing_time BIGINT,
                        window_id INT,
                        produce_time BIGINT,
                        PRIMARY KEY (user_id, ad_id, ns_time)
                    )
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS mappings (
                        ad_id VARCHAR(36),
                        campaign_id INT,
                        PRIMARY KEY (ad_id, campaign_id)
                    )
                """)
                for i, ad_id in enumerate(ad_ids):
                    cursor.execute(
                        "INSERT INTO mappings (ad_id, campaign_id) VALUES (%s, %s)",
                        (ad_id, campaign_id[(i + 1) % len(campaign_id)])
                    )
            conn.commit()

            # Create indexes outside of transaction block
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_window_id ON events(window_id);")
                cursor.execute("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_event_type ON events(event_type);")
                cursor.execute("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_ad_id_event_type_window ON events(ad_id, event_type, window_id);")
            conn.autocommit = False
        finally:
            conn.close()

if __name__ == "__main__":
    dump_data()