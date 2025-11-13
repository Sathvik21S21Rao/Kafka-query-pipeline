import pyspark
import yaml
import pickle
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from pyspark.sql.functions import col, from_json, to_timestamp, window, count, when, to_json, struct,max
import logging
import time

# Define schema
schema = StructType([
    StructField("user_id", StringType()),
    StructField("page_id", StringType()),
    StructField("ad_id", StringType()),
    StructField("ad_type", StringType()),
    StructField("ns_time", LongType()),
    StructField("ip_address", StringType()),
    StructField("window_id", IntegerType()),
    StructField("window_start_time", LongType()),
    StructField("event_type", StringType()),
    StructField("produce_time", LongType())
])


with open("./config.yml", "r") as f:
    cfg = yaml.safe_load(f)

with open("./ad_to_campaign_mapping.pkl", "rb") as f:
    ad_to_campaign_mapping = pickle.load(f)

session = pyspark.sql.SparkSession.builder.appName(cfg["spark"]["app_name"]).master(cfg["spark"]["master"]).getOrCreate()
session.sparkContext.setLogLevel("WARN")
ad_to_campaign_df = session.createDataFrame(list(ad_to_campaign_mapping.items()), ["ad_id", "campaign_id"])
ad_to_campaign_df.cache()

# Read Kafka stream
df = (
    session.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", cfg["bootstrap_servers"])
    .option("subscribe", "event")
    .option("startingOffsets", "latest")
    .option("kafka.request.timeout.ms","500000")
    .load()
)

df_parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
df_parsed = df_parsed.withColumn("event_time", to_timestamp(col("ns_time") / 1000000000)).withWatermark("event_time", "10 seconds")
events=df_parsed.join(ad_to_campaign_df,"ad_id","inner")
agg = (
    events.groupBy(window(col("event_time"), "10 seconds"), col("campaign_id"))
    .agg(
        count(when(col("event_type") == "view", True)).alias("views"),
        count(when(col("event_type") == "click", True)).alias("clicks"),
        max(col("produce_time")).alias("max_produce_time")
    )
    .withColumn("ctr",col("clicks") / (col("views")+1))
)

output = agg.select(
    col("window.start").cast("string").alias("key"),
    to_json(struct(
        col("window.start").alias("window_start"),
        col("campaign_id"),
        col("views"),
        col("clicks"),
        col("ctr"),
        col("max_produce_time")
    )).alias("value")
)

query = (
    output.writeStream
    .format("kafka")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/spark/checkpoints/campaign_agg_query")
    .option("kafka.bootstrap.servers", cfg["bootstrap_servers"])
    .option("topic", "query_results")
    .start()
)
query.awaitTermination()
