# MAGIC %md
# MAGIC org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0

import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    ArrayType,
    StructType,
)


class Bronze:
    def __init__(self):
        self.base_data_dir = "/FileStore/data_spark_streaming_scholarnest"
        self.BOOTSTRAP_SERVER = os.getenv("BOOTSTRAP_SERVER")
        self.CLUSTER_API_KEY = os.getenv("CLUSTER_API_KEY")
        self.CLUSTER_API_SECRET = os.getenv("CLUSTER_API_SECRET")

    def ingest_from_kafka(self, starting_time: int = 1) -> DataFrame:
        """Ingests data from Kafka into a Spark DataFrame."""
        JAAS_CONFIG = f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{self.CLUSTER_API_KEY}' password='{self.CLUSTER_API_SECRET}';"

        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.BOOTSTRAP_SERVER)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.sasl.jaas.config", JAAS_CONFIG)
            .option("subscribe", "invoices")
            .option("maxOffsetsPerTrigger", 30)
            .option("startingTimestamp", starting_time)
            .load()
        )

    def get_schema(self) -> StructType:
        """Defines the schema for the incoming Kafka data."""
        return StructType(
            [
                StructField("InvoiceNumber", StringType(), True),
            ]
        )

    def get_invoices(self, kafka_df: DataFrame) -> DataFrame:
        """Transforms Kafka DataFrame to apply the defined schema."""
        return kafka_df.selectExpr(
            "CAST(key AS STRING) key",
            "CAST(value AS STRING) value",
            "topic",
            "timestamp",
        ).select(
            col("key"),
            from_json(col("value"), self.get_schema()).alias("value"),
            col("topic"),
            col("timestamp"),
        )

    def upsert(self, invoices_df: DataFrame, batch_id: int):
        """Performs an upsert operation into the target Delta table."""
        pass

    def process(self, starting_time: int = 1):
        """Processes the ingested Kafka data."""
        print("Starting Bronze Stream...", end="")
        kafka_df = self.ingest_from_kafka(starting_time)
        invoices_df = self.get_invoices(kafka_df)
        query = (
            invoices_df.writeStream.foreachBatch(self.upsert)
            .option(
                "checkpointLocation", f"{self.base_data_dir}/checkpoint/invoices_bz"
            )
            .outputMode("append")
            .start()
        )
        print("Done")
        return query
