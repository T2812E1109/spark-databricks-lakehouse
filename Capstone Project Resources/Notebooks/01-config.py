# Notebook source
class Config:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.db_name = "sbit_db"
        self.maxFilesPerTrigger = 1000

    @property
    def base_dir_data(self):
        return self._get_external_location_url("data_zone")

    @property
    def base_dir_checkpoint(self):
        return self._get_external_location_url("checkpoint")

    def _get_external_location_url(self, location_name):
        try:
            result = (
                self.spark.sql(f"describe external location `{location_name}`")
                .select("url")
                .first()
            )
            if result is None:
                raise ValueError(f"No URL found for location {location_name}")
            return result.url
        except Exception as e:
            raise RuntimeError(
                f"Error accessing Spark SQL for {location_name}: {str(e)}"
            )
