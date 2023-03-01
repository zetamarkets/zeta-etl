# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from os.path import join

## From old mate stackoverflow: https://stackoverflow.com/questions/74646723/modulenotfounderror-no-module-named-dlt-error-when-running-delta-live-tables
try:
  import dlt # When run in a pipeline, this package will exist (no way to import it here)
except ImportError:
  class dlt: # "Mock" the dlt class so that we can syntax check the rest of our python in the databricks notebook editor
    def table(comment, **options): # Mock the @dlt.table attribute so that it is seen as syntactically valid below
      def _(f):
        pass
      return _;

# COMMAND ----------

MONITORING_ORDERS_TABLE = "monitoring-orders"
MONITORING_STATS_TABLE = "monitoring-stats"

S3_BUCKET_LANDED = "zetadex-mainnet-landing"
BASE_PATH_LANDED = join("/mnt", S3_BUCKET_LANDED)
S3_BUCKET_TRANSFORMED = "zetadex-mainnet"
BASE_PATH_TRANSFORMED = join("/mnt", S3_BUCKET_TRANSFORMED)

# COMMAND ----------

monitoring_orders_schema = """
underlying string,
timestamp timestamp,
priority_fee bigint,
error_message string
"""

@dlt.table(
    comment="Success/failure for periodic placeOrders",
    table_properties={
        "quality": "bronze",
    },
    path=join(BASE_PATH_TRANSFORMED, MONITORING_ORDERS_TABLE, "raw"),
    schema=monitoring_orders_schema,
)
def raw_monitoring_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.region", "ap-southeast-1")
        .option("cloudFiles.includeExistingFiles", True)
        .option("cloudFiles.useNotifications", True)
        .option("partitionColumns", "year,month,day,hour")
        .schema(monitoring_orders_schema)
        .load(join(BASE_PATH_LANDED, MONITORING_ORDERS_TABLE, "data"))
    )

# COMMAND ----------

monitoring_stats_schema = """
underlying string,
timestamp timestamp,
update_age_local int,
update_age_exchange int,
mark_price double
"""

@dlt.table(
    comment="Regular backend exchange statistics",
    table_properties={
        "quality": "bronze",
    },
    path=join(BASE_PATH_TRANSFORMED, MONITORING_STATS_TABLE, "raw"),
    schema=monitoring_stats_schema,
)
def raw_monitoring_stats():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.region", "ap-southeast-1")
        .option("cloudFiles.includeExistingFiles", True)
        .option("cloudFiles.useNotifications", True)
        .option("partitionColumns", "year,month,day,hour")
        .schema(monitoring_stats_schema)
        .load(join(BASE_PATH_LANDED, MONITORING_STATS_TABLE, "data"))
    )

# COMMAND ----------


