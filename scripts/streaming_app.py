import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, max, min
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("volume", IntegerType()),
    StructField("event_time", TimestampType())
])

spark = SparkSession.builder \
    .appName("StockProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# 1. Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw_api_events") \
    .load()

# 2. Parse JSON
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 3. Windowed Aggregation with Watermark 
# aggregated_df = parsed_df \
#     .withWatermark("event_time", "2 minutes") \
#     .groupBy(
#         window(col("event_time"), "5 minutes", "1 minute"),
#         col("symbol")
#     ).agg(
#         avg("price").alias("avg_price"),
#         max("price").alias("max_price")
#     )
aggregated_df = parsed_df \
    .withWatermark("event_time", "0 seconds") \
    .groupBy(
        window(col("event_time"), "1 minute", "1 minute"), # Smaller windows for testing
        col("symbol")
    ).agg(
        avg("price").alias("avg_price"),
        max("price").alias("max_price")
    )

# 4. Write to Console (or another Kafka topic)
# query = aggregated_df.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option("checkpointLocation", "./checkpoints") \
#     .start()
# 5. Write data into a file for Streamlit to read
# query = aggregated_df.writeStream \
#     .outputMode("complete") \
#     .format("csv") \
#     .option("header", "true") \
#     .option("path", "data/aggregates") \
#     .option("checkpointLocation", "checkpoints/dashboard") \
#     .start()
query = aggregated_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "data/aggregates") \
    .option("checkpointLocation", "checkpoints/dashboard") \
    .trigger(processingTime='5 seconds') \
    .start()

query.awaitTermination()