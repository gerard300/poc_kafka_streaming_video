from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.types import *
import os
import pyspark

print(pyspark.__version__)
# packages = "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.5" # Just for unstructured streaming
packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2"
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages {0} pyspark-shell".format(packages))

print("PYSPARK_SUBMIT_ARGS = ", os.environ["PYSPARK_SUBMIT_ARGS"], "\n")
print("JAVA_HOME = ", os.environ["JAVA_HOME"])

spark = (SparkSession
         .builder
         .appName("structured_streaming_kafka_video")
         .getOrCreate())

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "video2") \
    .load()


df = df.selectExpr("CAST(value AS STRING)")
streaming_query = df.writeStream \
    .format("console") \
    .queryName("diff_time") \
    .trigger(processingTime='5 seconds') \
    .start()

streaming_query.awaitTermination()





