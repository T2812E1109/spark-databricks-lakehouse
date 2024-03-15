from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

BOOTSTRAP_SERVER = "pkc-abcdefgh.us-west4.gcp.confluent.cloud:9092"
JAAS_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"
CLUSTER_API_KEY = os.getenv("CLUSTER_API_KEY")
CLUSTER_API_SECRET = os.getenv("CLUSTER_API_SECRET")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")


def read_from_kafka(spark: SparkSession):
    """
    Reads data from a Kafka topic into a Spark DataFrame with secured authentication.

    Parameters:
        spark (SparkSession): The active Spark session instance.

    Returns:
        DataFrame: A DataFrame representing the Kafka topic data.
    """
    kafka_read_df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            f"{JAAS_MODULE} required username='{CLUSTER_API_KEY}' password='{CLUSTER_API_SECRET}';",
        )
        .option("subscribe", KAFKA_TOPIC)
        .load()
    )
    return kafka_read_df


spark = SparkSession.builder.appName("KafkaReadExample").getOrCreate()

df = read_from_kafka(spark)
display(df)
