import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, struct, to_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    ArrayType,
)


class KafkaProducer:
    def __init__(self):
        self.base_data_dir = "/FileStore/data_spark_streaming_scholarnest"
        self.BOOTSTRAP_SERVER = os.getenv("BOOTSTRAP_SERVER", "default_server")
        self.CLUSTER_API_KEY = os.getenv("CLUSTER_API_KEY", "default_key")
        self.CLUSTER_API_SECRET = os.getenv("CLUSTER_API_SECRET", "default_secret")

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

    def read_invoices(self, condition: str):
        return (
            spark.readStream.format("json")
            .schema(self.get_schema())
            .load(f"{self.base_data_dir}/data/invoices")
            .where(condition)
        )

    def get_kafka_message(self, df, key_column: str):
        return df.selectExpr(
            f"CAST({key_column} AS STRING) AS key", "to_json(struct(*)) AS value"
        )

    def send_to_kafka(self, kafka_df):
        try:
            query = (
                kafka_df.writeStream.format("kafka")
                .option("kafka.bootstrap.servers", self.BOOTSTRAP_SERVER)
                .option("topic", "invoices")
                .option(
                    "checkpointLocation",
                    f"{self.base_data_dir}/checkpoint/kafka_producer",
                )
                .start()
            )
            return query
        except Exception as e:
            print(f"Error sending data to Kafka: {e}")
            return None

    def process(self, condition: str):
        print("Starting Kafka Producer Stream...", end="")
        invoices_df = self.read_invoices(condition)
        kafka_df = self.get_kafka_message(invoices_df, "StoreID")
        query = self.send_to_kafka(kafka_df)
        print("Done\n")
        return query
