from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table import expressions as expr
import yaml
import pickle



with open("./config.yml", "r") as f:
    cfg = yaml.safe_load(f)

with open("./ad_to_campaign_mapping.pkl", "rb") as f:
    ad_to_campaign_mapping = pickle.load(f)

lookup_rows = [(k, v) for k, v in ad_to_campaign_mapping.items()]


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(2)

settings = (
    EnvironmentSettings
    .new_instance()
    .in_streaming_mode()
    .build()
)

t_env = StreamTableEnvironment.create(stream_execution_environment=env,
                                      environment_settings=settings)


lookup_schema = DataTypes.ROW([
    DataTypes.FIELD("ad_id", DataTypes.STRING()),
    DataTypes.FIELD("campaign_id", DataTypes.STRING())
])

lookup_table = t_env.from_elements(lookup_rows, lookup_schema)
t_env.create_temporary_view("ad_to_campaign", lookup_table)

source_ddl = f"""
CREATE TABLE events (
    user_id STRING,
    page_id STRING,
    ad_id STRING,
    ad_type STRING,
    ns_time BIGINT,
    ip_address STRING,
    window_id INT,
    window_start_time BIGINT,
    event_type STRING,
    produce_time BIGINT,

    -- Convert ns → timestamp_ltz
    event_time AS TO_TIMESTAMP_LTZ(ns_time / 1000000000, 3),

    -- Flink 2.0 watermark
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
)
WITH (
    'connector' = 'kafka',
    'topic' = 'event',
    'properties.bootstrap.servers' = '{cfg["bootstrap_servers"]}',
    'properties.group.id' = 'flink_consumer',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);
"""

t_env.execute_sql(source_ddl)



sink_ddl = f"""
CREATE TABLE query_results (
    `key` STRING,
    `value` STRING
)
WITH (
    'connector' = 'kafka',
    'topic' = 'query_results',
    'properties.bootstrap.servers' = '{cfg["bootstrap_servers"]}',
    'format' = 'raw'
);
"""

t_env.execute_sql(sink_ddl)



query = t_env.sql_query("""
SELECT
    CAST(window_start AS STRING) AS `key`,
    TO_JSON(
        ROW(
            window_start,
            campaign_id,
            views,
            clicks,
            ctr,
            max_produce_time
        )
    ) AS `value`
FROM (
    SELECT
        window_start,
        window_end,
        ac.campaign_id AS campaign_id,

        COUNT(CASE WHEN e.event_type = 'view' THEN 1 END) AS views,
        COUNT(CASE WHEN e.event_type = 'click' THEN 1 END) AS clicks,

        MAX(e.produce_time) AS max_produce_time,

        CAST(COUNT(CASE WHEN e.event_type = 'click' THEN 1 END) AS DOUBLE)
        / (COUNT(CASE WHEN e.event_type = 'view' THEN 1 END) + 1)
        AS ctr

    FROM TABLE(
        TUMBLE(TABLE events, DESCRIPTOR(event_time), INTERVAL '10' SECOND)
    ) AS e
    JOIN ad_to_campaign AS ac
    ON e.ad_id = ac.ad_id

    GROUP BY window_start, window_end, ac.campaign_id
);
""")


query.execute_insert("query_results").wait()
