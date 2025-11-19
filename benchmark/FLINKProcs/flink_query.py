import os
import yaml
import pickle
from pyflink.table import TableEnvironment, EnvironmentSettings
from pyflink.table.types import DataTypes
from pyflink.common import Configuration

# --- Configuration Loading ---
try:
    with open("./config.yml", "r") as f:
        cfg = yaml.safe_load(f)
except FileNotFoundError:
    print("Error: config.yml not found. Please create it with bootstrap_servers.")
    exit(1)

# Load ad_to_campaign_mapping
try:
    with open("./ad_to_campaign_mapping.pkl", "rb") as f:
        ad_to_campaign_mapping = pickle.load(f)
except FileNotFoundError:
    print("Error: ad_to_campaign_mapping.pkl not found. Using placeholder data.")
    # The placeholder data is: {"ad_A": 1, "ad_B": 2, "ad_C": 1, "ad_D": 3} 
    ad_to_campaign_mapping = {"ad_A": 1, "ad_B": 2, "ad_C": 1, "ad_D": 3}  


# --- Flink Environment Setup ---
print("Initializing Flink Table Environment...")

env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# --- Dependency Configuration ---
KAFKA_CONNECTOR_JAR = "/opt/flink/opt/flink-sql-connector-kafka-4.0.1-2.0.jar"
t_env.get_config().set("rest.bind-address", "0.0.0.0")
t_env.get_config().set("pipeline.jars", f"file://{KAFKA_CONNECTOR_JAR}")
t_env.get_config().set("parallelism.default", "4")
t_env.get_config().set("rest.port", "8085")
t_env.get_config().set("taskmanager.numberofTaskSlots,","4")
t_env.get_config().set("taskmanager.memory.process.size","4096m")
t_env.get_config().set("table.optimizer.broadcast.filter-enabled", "true")
t_env.get_config().set("taskmanager.cpu.cores","4")
t_env.get_config().set("python.fn-execution.bundle.size", "50000")
t_env.get_config().set("python.fn-execution.bundle.time", "500")


# --- Constants ---
KAFKA_BOOTSTRAP_SERVERS = cfg["bootstrap_servers"]
INPUT_TOPIC = "event"
OUTPUT_TOPIC = "query_results"

# --- 1. Define Source Table (Kafka) ---
print(f"Configuring Kafka Source: {INPUT_TOPIC}")
t_env.execute_sql(f"""
CREATE TABLE kafka_source (
    user_id STRING,
    page_id STRING,
    ad_id STRING,
    ad_type STRING,
    ns_time BIGINT,
    ip_address STRING,
    event_type STRING,
    produce_time BIGINT,

    -- computed column (must come after physical columns)
    event_time AS CAST(TO_TIMESTAMP_LTZ(ns_time / 1000000, 3) AS TIMESTAMP(3)),

    WATERMARK FOR event_time AS event_time - INTERVAL '3' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = '{INPUT_TOPIC}',
  'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'flink_consumer_group',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

""")

# --- 2. Define Mapping Table (In-Memory) ---
print("Configuring Campaign Mapping Table...")
mapping_data = [(k, v) for k, v in ad_to_campaign_mapping.items()]
ad_to_campaign_df = t_env.from_elements(
        mapping_data,
        DataTypes.ROW([
            DataTypes.FIELD("ad_id", DataTypes.STRING()),
            DataTypes.FIELD("campaign_id", DataTypes.INT())
        ])
    )
t_env.create_temporary_view("ad_to_campaign_mapping", ad_to_campaign_df)

# --- 3. Define Sink Table (Not Used) ---
# The sink is not defined as we are printing the results.

# --- 4. Flink SQL Transformation (Debug: Print Join Result) ---
print("Executing Flink SQL Query and printing Inner Join results...")
t_env.execute_sql(f"""
    CREATE TABLE kafka_sink (
        window_start STRING,
        campaign_id INT,
        ctr DOUBLE,
        max_produce_time BIGINT

    ) WITH (
      'connector' = 'kafka',
      'topic' = '{OUTPUT_TOPIC}',
      'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
      'format' = 'json'
    )
""")

join_debug_query = f"""
INSERT INTO kafka_sink

SELECT
        CAST(TUMBLE_START(event_time, INTERVAL '10' SECOND) AS STRING) AS window_start,
        T.campaign_id,
        CAST(SUM(CASE WHEN A.event_type = 'click' THEN 1 ELSE 0 END) AS DOUBLE) /
        (SUM(CASE WHEN A.event_type = 'view' THEN 1 ELSE 0 END) + 1) AS ctr,
        MAX(A.produce_time) AS max_produce_time
    FROM
        kafka_source AS A
    INNER JOIN
        ad_to_campaign_mapping AS T
    ON A.ad_id = T.ad_id -- Using standard join, swap to LIKE or TRIM if join fails
    GROUP BY
        TUMBLE(A.event_time, INTERVAL '10' SECOND),
        T.campaign_id

"""


t_env.execute_sql(join_debug_query).wait()
