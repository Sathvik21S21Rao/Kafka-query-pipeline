"""
PyFlink streaming job that reads JSON events from Kafka topic `event`, aggregates
counts of `view` and `click` per `window_id` and `campaign_id`, computes CTR,
and writes JSON results to Kafka topic `query_results`.

Assumptions:
- Events are JSON objects with at least these fields:
  - window_id (integer)
  - campaign_id (string)
  - event_type (string) either 'view' or 'click'
  - produce_time (long milliseconds since epoch) optional
- Kafka broker at 127.0.0.1:9092 (adjust in DDL if different)
- PyFlink and the Kafka connector are available in the runtime environment.

Run (example):
  # from the project root, with PyFlink installed
  python benchmark/flink_query.py

You can also submit this to a Flink cluster with `flink run -py benchmark/flink_query.py`.
"""

from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    # create a streaming TableEnvironment (Blink planner)
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    # Kafka source: events
    # The JSON format will try to parse fields by name. Adjust schema as needed.
    t_env.execute_sql("""
    CREATE TABLE events (
      `window_id` BIGINT,
      `campaign_id` STRING,
      `ad_id` STRING,
      `event_type` STRING,
      `ns_time` BIGINT,
      `produce_time` BIGINT,
      -- optional processing time column if needed
      `proc_time` AS PROCTIME()
    ) WITH (
      'connector' = 'kafka',
      'topic' = 'event',
      'properties.bootstrap.servers' = '127.0.0.1:9092',
      'properties.group.id' = 'flink_query_group',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'json',
      'json.ignore-parse-errors' = 'true'
    )
    """)

    # Kafka sink: aggregated query_results
    # We'll output JSON with fields that result consumers expect
    t_env.execute_sql("""
    CREATE TABLE query_results (
      `window_start` BIGINT,
      `campaign_id` STRING,
      `views` BIGINT,
      `clicks` BIGINT,
      `ctr` DOUBLE,
      `max_produce_time` BIGINT
    ) WITH (
      'connector' = 'kafka',
      'topic' = 'query_results',
      'properties.bootstrap.servers' = '127.0.0.1:9092',
      'format' = 'json'
    )
    """)

    # Aggregation: count views and clicks per (window_id, campaign_id)
    # Compute CTR as clicks / NULLIF(views, 0)
    insert_sql = """
    INSERT INTO query_results
    SELECT
      window_id AS window_start,
      campaign_id,
      SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS views,
      SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS clicks,
      CAST(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS DOUBLE) /
        NULLIF(SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END), 0) AS ctr,
      MAX(produce_time) AS max_produce_time
    FROM events
    GROUP BY window_id, campaign_id
    """

    # Submit the continuous insert query
    t_env.execute_sql(insert_sql)

    # Some environments require explicitly executing the job. If supported, you
    # can call t_env.execute(job_name). The insert_sql above typically starts
    # the job in interactive Python environments.
    try:
        # Attempt to call explicit execute if available
        t_env.execute('FlinkKafkaQueryJob')
    except Exception:
        # Not all PyFlink versions require or expose execute(); ignore if not supported.
        pass


if __name__ == '__main__':
    main()
