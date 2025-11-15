import pyspark
import yaml
import pickle
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from pyspark.sql.functions import col, from_json, to_timestamp, window, count, when, to_json, struct, max
import datetime
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.functions import broadcast

# --------------------- CONFIG & SETUP ---------------------

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

# Load config
with open("./config.yml", "r") as f:
    cfg = yaml.safe_load(f)

# Load ad→campaign mapping
with open("./ad_to_campaign_mapping.pkl", "rb") as f:
    ad_to_campaign_mapping = pickle.load(f)

# Initialize Spark
session = (
    pyspark.sql.SparkSession.builder
    .appName(cfg["spark"]["app_name"])
    .master(cfg["spark"]["master"])
    .getOrCreate()
)

session.sparkContext.setLogLevel("WARN")

# Create static lookup DF
ad_to_campaign_df = session.createDataFrame(
    list(ad_to_campaign_mapping.items()),
    ["ad_id", "campaign_id"]
)


# --------------------- WATERMARK → KAFKA LISTENER ---------------------

class WatermarkKafkaListener(StreamingQueryListener):
    def onQueryStarted(self,event):
        pass

    def onQueryTerminated(self,event):
        pass

    def onQueryProgress(self, event):
        wm = event.progress.eventTime.get("watermark")

        if wm:
            row = {
                "batch_id": event.progress.batchId,
                "processing_time": datetime.datetime.now().isoformat(),
                "watermark": wm,
                "input_rows": event.progress.numInputRows
            }



            print(f"[WM-KAFKA] batch={row['batch_id']} watermark={wm} processing_time={row['processing_time']} input_rows={row['input_rows']}")

session.streams.addListener(WatermarkKafkaListener())

session.conf.set("spark.sql.shuffle.partitions","4")
session.conf.set("spark.default.parallelism","4")

# --------------------- KAFKA INPUT STREAM ---------------------

df = (
    session.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", cfg["bootstrap_servers"])
    .option("subscribe", "event")
    .option("kafka.group.id", "spark_consumer")
    .option("startingOffsets", "earliest")
    .option("kafka.session.timeout.ms", "500000")
    .load()
)

df_parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")


# Convert ns_time to timestamp and watermark
df_parsed = (
    df_parsed
    .withColumn("event_time", to_timestamp(col("ns_time") / 1000000000))
    .withWatermark("event_time", "10 seconds")
)

# Join with campaign lookup
events = df_parsed.join(broadcast(ad_to_campaign_df), "ad_id", "inner")

# --------------------- AGGREGATION ---------------------

agg = (
    events.groupBy(
        window(col("event_time"), "10 seconds"),
        col("campaign_id")
    )
    .agg(
        count(when(col("event_type") == "view", True)).alias("views"),
        count(when(col("event_type") == "click", True)).alias("clicks"),
        max(col("produce_time")).alias("max_produce_time")
    )
    .withColumn("ctr", col("clicks") / (col("views") + 1))
)

# --------------------- OUTPUT TO KAFKA ---------------------

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
