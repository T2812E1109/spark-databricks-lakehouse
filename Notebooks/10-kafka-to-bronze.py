from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    ArrayType,
)
import os


class Bronze:
    def __init__(self, base_data_dir="/FileStore/data_spark_streaming_scholarnest"):
        self.base_data_dir = base_data_dir
        self.BOOTSTRAP_SERVER = os.getenv("BOOTSTRAP_SERVER")
        self.CLUSTER_API_KEY = os.getenv("CLUSTER_API_KEY")
        self.CLUSTER_API_SECRET = os.getenv("CLUSTER_API_SECRET")

    def get_schema(self) -> StructType:
        return StructType(
            [
                StructField("InvoiceNumber", StringType(), True),
                StructField("CreatedTime", LongType(), True),
                StructField("StoreID", StringType(), True),
                StructField("PosID", StringType(), True),
                StructField("CashierID", StringType(), True),
                StructField("CustomerType", StringType(), True),
                StructField("CustomerCardNo", StringType(), True),
                StructField("TotalAmount", DoubleType(), True),
                StructField("NumberOfItems", LongType(), True),
                StructField("PaymentMethod", StringType(), True),
                StructField("TaxableAmount", DoubleType(), True),
                StructField("CGST", DoubleType(), True),
                StructField("SGST", DoubleType(), True),
                StructField("CESS", DoubleType(), True),
                StructField("DeliveryType", StringType(), True),
            ]
        )

    def ingest_from_kafka(self, starting_time=1) -> DataFrame:
        JAAS_CONFIG = f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{self.CLUSTER_API_KEY}' password='{self.CLUSTER_API_SECRET}';"

        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.BOOTSTRAP_SERVER)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.sasl.jaas.config", JAAS_CONFIG)
            .option("subscribe", "invoices")
            .option("maxOffsetsPerTrigger", 10)
            .option("startingTimestamp", starting_time)
            .load()
        )

    def get_invoices(self, kafka_df: DataFrame) -> DataFrame:
        return kafka_df.selectExpr(
            "CAST(key AS STRING) key",
            "from_json(CAST(value AS STRING), schema) AS value",
            "topic",
            "timestamp",
        ).select("key", "value.*", "topic", "timestamp")

    def process(self, starting_time=1):
        print("Starting Bronze Stream...", end="")
        raw_df = self.ingest_from_kafka(starting_time)
        invoices_df = self.get_invoices(raw_df.withColumn("schema", self.get_schema()))
        sQuery = (
            invoices_df.writeStream.queryName("bronze-ingestion")
            .option(
                "checkpointLocation", f"{self.base_data_dir}/checkpoint/invoices_bz"
            )
            .outputMode("append")
            .toTable("invoices_bz")
        )
        print("Done")
        return sQuery
