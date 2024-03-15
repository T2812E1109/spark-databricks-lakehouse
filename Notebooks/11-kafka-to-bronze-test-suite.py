# MAGIC %md
# MAGIC org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0

# MAGIC %run ./10-kafka-to-bronze

import time
import pytest


class KafkaToBronzeTestSuite:
    def __init__(self, base_data_dir="/FileStore/data_spark_streaming_scholarnest"):
        self.base_data_dir = base_data_dir

    def setup(self):
        """Set up necessary configurations and clean up before running tests."""
        print("Setting up for the tests...")
        spark.sql("DROP TABLE IF EXISTS invoices_bz")
        dbutils.fs.rm(f"{self.base_data_dir}/checkpoint/invoices_bz", True)

    def teardown(self):
        """Clean up after tests."""
        print("Cleaning up after the tests...")
        spark.sql("DROP TABLE IF EXISTS invoices_bz")
        dbutils.fs.rm(f"{self.base_data_dir}/checkpoint/invoices_bz", True)

    def wait_for_micro_batch(self, sleep=30):
        """Wait for a specified time to ensure micro-batch processing."""
        print(f"Waiting for {sleep} seconds...")
        time.sleep(sleep)

    def assert_result(self, expected_count):
        """Assert the result of the test by comparing expected and actual counts."""
        actual_count = spark.sql("SELECT COUNT(*) FROM invoices_bz").collect()[0][0]
        assert (
            expected_count == actual_count
        ), f"Test failed! Expected {expected_count}, but got {actual_count}."

    def run_tests(self):
        """Run the test scenarios."""
        try:
            self.setup()

            bz_stream = Bronze()

            print("Testing Scenario - Start from beginning on a new checkpoint...")
            bz_query = bz_stream.process()
            self.wait_for_micro_batch()
            bz_query.stop()
            self.assert_result(30)

            print(
                "Testing Scenario - Restart from where it stopped on the same checkpoint..."
            )
            bz_query = bz_stream.process()
            self.wait_for_micro_batch()
            bz_query.stop()
            self.assert_result(30)

            self.setup()
            print(
                "Testing Scenario - Start from specific timestamp on a new checkpoint..."
            )
            bz_query = bz_stream.process(starting_time=1697945539000)
            self.wait_for_micro_batch()
            bz_query.stop()
            self.assert_result(40)

        finally:
            self.teardown()
            print("All validations passed.")


if __name__ == "__main__":
    test_suite = KafkaToBronzeTestSuite()
    test_suite.run_tests()
