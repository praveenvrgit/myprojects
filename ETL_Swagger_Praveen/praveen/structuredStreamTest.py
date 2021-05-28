from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 pyspark-shell'


KAFKA_TOPIC_NAME_CONS = "streamTest"

KAFKA_BOOTSTRAP_SERVERS_CONS = '10.3.2.14:9092'

if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Demo Application Started ...")

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka Demo") \
        .getOrCreate()

    print("")
    print("Application ID: ", spark.sparkContext.applicationId)
    print("")

    # Construct a streaming DataFrame that reads from testtopic
    transaction_detail_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .load() \
        .selectExpr("CAST(value AS STRING)")

    print("Printing Schema of transaction_detail_df: ")
    transaction_detail_df.printSchema()
