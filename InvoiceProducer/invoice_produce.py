from confluent_kafka import Producer
import json
import time
import os


class InvoiceProducer:
    def __init__(self):
        self.topic = "invoices"
        self.conf = {
            "bootstrap.servers": "pkc-abcdefgh.us-west4.gcp.confluent.cloud:9092",
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": os.getenv("KAFKA_SASL_USERNAME"),
            "sasl.password": os.getenv("KAFKA_SASL_PASSWORD"),
            "client.id": os.getenv("KAFKA_CLIENT_ID", "prashant-laptop"),
        }

    def delivery_callback(self, err, msg):
        """Handles the result of the message delivery."""
        if err:
            print(f"ERROR: Message failed delivery: {err}")
        else:
            key = msg.key().decode("utf-8")
            invoice_id = json.loads(msg.value().decode("utf-8"))["InvoiceNumber"]
            print(f"Produced event to: key = {key} value = {invoice_id}")

    def produce_invoices(self, producer):
        """Reads invoices from a file and produces them to a Kafka topic."""
        with open("data/invoices.json") as lines:
            for line in lines:
                invoice = json.loads(line)
                store_id = invoice["StoreID"]
                producer.produce(
                    self.topic,
                    key=str(store_id),
                    value=json.dumps(invoice),
                    callback=self.delivery_callback,
                )
                time.sleep(0.1)
                producer.poll(0)

    def start(self):
        """Initializes the Kafka producer and starts producing invoices."""
        kafka_producer = Producer(self.conf)
        self.produce_invoices(kafka_producer)
        kafka_producer.flush()


if __name__ == "__main__":
    invoice_producer = InvoiceProducer()
    invoice_producer.start()
