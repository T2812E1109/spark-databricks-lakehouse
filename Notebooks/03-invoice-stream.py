from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    DoubleType,
    LongType,
)


class InvoiceStream:
    def __init__(self, base_data_dir="/FileStore/data_spark_streaming_scholarnest"):
        self.base_data_dir = base_data_dir

    def get_schema(self) -> StructType:
        return StructType(
            [
                StructField("InvoiceNumber", StringType(), True),
                StructField("CreatedTime", LongType(), True),
                StructField("StoreID", StringType(), True),
                StructField("PosID", StringType(), True),
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
                StructField(
                    "DeliveryAddress",
                    StructType(
                        [
                            StructField("AddressLine", StringType(), True),
                            StructField("City", StringType(), True),
                            StructField("ContactNumber", StringType(), True),
                            StructField("PinCode", StringType(), True),
                            StructField("State", StringType(), True),
                        ]
                    ),
                    True,
                ),
                StructField(
                    "InvoiceLineItems",
                    ArrayType(
                        StructType(
                            [
                                StructField("ItemCode", StringType(), True),
                                StructField("ItemDescription", StringType(), True),
                                StructField("ItemPrice", DoubleType(), True),
                                StructField("ItemQty", LongType(), True),
                                StructField("TotalValue", DoubleType(), True),
                            ]
                        ),
                        True,
                    ),
                    True,
                ),
            ]
        )

    def read_invoices(self) -> DataFrame:
        return (
            spark.readStream.format("json")
            .schema(self.get_schema())
            .load(f"{self.base_data_dir}/data/invoices")
        )

    def explode_invoices(self, invoiceDF: DataFrame) -> DataFrame:
        return invoiceDF.selectExpr(
            "InvoiceNumber",
            "CreatedTime",
            "StoreID",
            "PosID",
            "CustomerType",
            "PaymentMethod",
            "DeliveryType",
            "DeliveryAddress.City",
            "DeliveryAddress.State",
            "DeliveryAddress.PinCode",
            "explode(InvoiceLineItems) as LineItem",
        )

    def flatten_invoices(self, explodedDF: DataFrame) -> DataFrame:
        return explodedDF.select(
            "*",
            expr("LineItem.ItemCode"),
            expr("LineItem.ItemDescription"),
            expr("LineItem.ItemPrice"),
            expr("LineItem.ItemQty"),
            expr("LineItem.TotalValue"),
        ).drop("LineItem")

    def append_invoices(self, flattenedDF: DataFrame):
        return (
            flattenedDF.writeStream.format("delta")
            .option("checkpointLocation", f"{self.base_data_dir}/checkpoint/invoices")
            .outputMode("append")
            .toTable("invoice_line_items")
        )

    def process(self):
        print("Starting Invoice Processing Stream...", end="")
        invoicesDF = self.read_invoices()
        explodedDF = self.explode_invoices(invoicesDF)
        resultDF = self.flatten_invoices(explodedDF)
        sQuery = self.append_invoices(resultDF)
        print("Done")
        return sQuery
