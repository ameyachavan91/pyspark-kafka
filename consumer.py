from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# Create a Spark session
spark = SparkSession.builder.appName("KafkaConsumer").getOrCreate()

# Define the schema for the data
schema = StructType([
    StructField("key", StringType(), True),
    StructField("value", StringType(), True)
])

# Create a stream to read from the Kafka topic
stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test") \
    .option("startingOffsets", "earliest") \
    .load()

# Extract the value column from the stream
df = stream.select(col("value").cast("string"))

# Show the data
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for the query to finish
query.awaitTermination()
