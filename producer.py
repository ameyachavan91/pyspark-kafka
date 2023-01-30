from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# Create a Spark session
spark = SparkSession.builder.appName("KafkaProducer").getOrCreate()

# Define the schema for the data
schema = StructType([
    StructField("key", StringType(), True),
    StructField("value", StringType(), True)
])

# Create a dataframe with sample data
data = [("key1", "value1"), ("key2", "value2"), ("key3", "value3")]
df = spark.createDataFrame(data, schema)

# Convert the dataframe to a stream
stream = df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "test") \
    .option("checkpointLocation", "./checkpoints") \
    .start()

# Wait for the stream to finish
stream.awaitTermination()
