import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.types import StructType, StructField, StringType
import logging
from config.config import config

def start_streaming(spark):
    try:
        # Define the schema for the incoming JSON data
        schema = StructType([
            StructField('title', StringType()),
            StructField('desc', StringType()),
            StructField('published_at', StringType())
        ])

        # Read data from the socket
        stream_df = spark.readStream.format("socket").option("host", "0.0.0.0").option("port", 9999).load()

        # Flatten the nested structure
        stream_df = stream_df.selectExpr("explode(from_json(value, 'array<struct<ticker:array<string>,title:string,desc:string,published_at:string>>')) as data").select("data.*")

        # Prepare data for Kafka
        kafka_df = stream_df.selectExpr("CAST(title AS STRING) AS key", "to_json(struct(*)) AS value")

        # Write to Kafka
        query = (kafka_df.writeStream.format("kafka")
                 .option("kafka.bootstrap.servers", config["kafka"]["bootstrap.servers"])
                 .option("kafka.security.protocol", config["kafka"]["security.protocol"])
                 .option("kafka.sasl.mechanism", config["kafka"]["sasl.mechanisms"])
                 .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='{u}' password='{p}';".format(u=config["kafka"]["sasl.username"], p=config['kafka']['sasl.password']))
                 .option("checkpointLocation", "/temp/checkpoint")  # Specify a valid checkpoint location
                 .option("topic", "news_data")
                 .start())

        # Log start message
        logging.info("Starting the streaming job...")

        # Wait for the termination of the query with a timeout
        query.awaitTermination(timeout=50)

        # Log termination message
        logging.info("Streaming job terminated.")

    except Exception as e:
        logging.error(f"Error in streaming job: {e}")

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)

    # Create a Spark session
    spark_connection = SparkSession.builder.appName("SocketStreamConsumer").getOrCreate()

    # Start the streaming process
    start_streaming(spark_connection)
