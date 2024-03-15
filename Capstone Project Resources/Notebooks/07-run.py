DEFAULT_ENVIRONMENT = "dev"
DEFAULT_RUN_TYPE = "once"  # or "continuous"
DEFAULT_PROCESSING_TIME = "5 seconds"

dbutils.widgets.text(
    "Environment", DEFAULT_ENVIRONMENT, "Set the current environment/catalog name"
)
dbutils.widgets.text("RunType", DEFAULT_RUN_TYPE, "Set once to run as a batch")
dbutils.widgets.text(
    "ProcessingTime", DEFAULT_PROCESSING_TIME, "Set the microbatch interval"
)


def get_widget_value(widget_name, cast_type=str):
    value = dbutils.widgets.get(widget_name)
    if cast_type is bool:
        return value.lower() == "once"
    elif cast_type is int and value.isdigit():
        return int(value)
    return value


env = get_widget_value("Environment")
once = get_widget_value("RunType", bool)
processing_time = get_widget_value("ProcessingTime")

mode = "batch" if once else f"stream with {processing_time} microbatch"
print(f"Starting sbit in {mode} mode.")

spark_conf_settings = {
    "spark.sql.shuffle.partitions": spark.sparkContext.defaultParallelism,
    "spark.databricks.delta.optimizeWrite.enabled": True,
    "spark.databricks.delta.autoCompact.enabled": True,
    "spark.sql.streaming.stateStore.providerClass": "com.databricks.sql.streaming.state.RocksDBStateStoreProvider",
}

for conf, value in spark_conf_settings.items():
    spark.conf.set(conf, value)


def run_notebook(notebook_path):
    dbutils.notebook.run(notebook_path, 60)


for notebook in ["02-setup", "03-history-loader", "04-bronze", "05-silver", "06-gold"]:
    run_notebook(notebook)

SH = SetupHelper(env)
HL = HistoryLoader(env)

setup_required = (
    spark.sql(f"SHOW DATABASES IN {SH.catalog}")
    .filter(f"databaseName == '{SH.db_name}'")
    .count()
    != 1
)

if setup_required:
    SH.setup()
    SH.validate()
    HL.load_history()
    HL.validate()
else:
    spark.sql(f"USE {SH.catalog}.{SH.db_name}")

BZ = Bronze(env)
SL = Silver(env)
GL = Gold(env)

# Process ETL stages
BZ.consume(once, processing_time)
SL.upsert(once, processing_time)
GL.upsert(once, processing_time)
