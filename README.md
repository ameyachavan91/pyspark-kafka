# PySpark Kafka Code Snippets
This repository contains PySpark code to produce messages to a Kafka topic and consume messages from a Kafka topic.

# Prerequisites
- Python 3.x
- Spark 2.x
- Kafka 2.x

# Producer Code
The producer code is in the file producer.py. It produces messages to a Kafka topic named "test". The messages are sent to a Kafka broker running on localhost on port 9092. The checkpointLocation option is used to store the state of the stream processing in case of failures.

# Consumer Code
The consumer code is in the file consumer.py. It consumes messages from a Kafka topic named "test". The startingOffsets option is set to earliest to start reading from the earliest available message in the topic. The data is displayed using the writeStream method and the console format.

# How to Run
1. Start a Kafka broker on localhost on port 9092.
2. Run the producer code using the following command: spark-submit producer.py
3. Run the consumer code using the following command: spark-submit consumer.py
4. Observe the messages produced to the "test" topic in the consumer output.

# Conclusion
This repository demonstrates a simple PySpark Kafka producer and consumer implementation. The code can be further optimized and extended based on the requirements.



