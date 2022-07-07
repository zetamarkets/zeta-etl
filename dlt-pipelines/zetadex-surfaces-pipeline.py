# Databricks notebook source
# Databricks notebook source
dbutils.widgets.dropdown("network", "devnet", ["devnet", "mainnet"], "Network")
# NETWORK = dbutils.widgets.get("network")
NETWORK = spark.conf.get("pipeline.network")
print(spark.conf)

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from os.path import join
import dlt

# COMMAND ----------

from datetime import datetime, timezone
current_date = str(datetime.now(timezone.utc).date())
current_hour = datetime.now(timezone.utc).hour

# COMMAND ----------

SURFACES_TABLE = "surfaces"

# COMMAND ----------

# MAGIC %md
# MAGIC # ETL

# COMMAND ----------

S3_BUCKET_LANDED = f"zetadex-{NETWORK}-landing"
BASE_PATH_LANDED = join("/mnt", S3_BUCKET_LANDED)
S3_BUCKET_TRANSFORMED = f"zetadex-{NETWORK}"
BASE_PATH_TRANSFORMED = join("/mnt", S3_BUCKET_TRANSFORMED)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Surfaces

# COMMAND ----------

surfaces_schema = """
timestamp timestamp, 
underlying string,
expiry_timestamp timestamp, 
expiry_series_index int, 
interest_rate double, 
vol_surface array<double>,
nodes array<double>, 
slot bigint, 
year string,
month string,
day string,
hour string
"""

@dlt.table(
    comment="Raw data for platform interest rate and volatility surfaces",
    table_properties={
        "quality":"bronze", 
    },
    path=join(BASE_PATH_TRANSFORMED,SURFACES_TABLE,"raw"),
    schema=surfaces_schema
)
def raw_surfaces():
    return (spark
           .readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "json")
           .option("cloudFiles.region", "ap-southeast-1")
           .option("cloudFiles.includeExistingFiles", True)
           .option("cloudFiles.useNotifications", True)
           .option("partitionColumns", "year,month,day,hour")
           .schema(surfaces_schema)
           .load(join(BASE_PATH_LANDED,SURFACES_TABLE,"data"))
          )

# COMMAND ----------

zip_map = F.udf(lambda x, y: dict(zip(x, y)), T.MapType(T.DoubleType(), T.DoubleType()))

@dlt.table(
    comment="Cleaned data for platform interest rate and volatility surfaces",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED,SURFACES_TABLE,"cleaned")
)
def cleaned_surfaces():
    return (dlt.read_stream("raw_surfaces")
           .withWatermark("timestamp", "1 minute")
           .dropDuplicates(["underlying", "expiry_timestamp", "slot"])
           .withColumn("vol_surface", zip_map("nodes", "vol_surface"))
           .withColumn("date_", F.to_date("timestamp"))
           .withColumn("hour_", F.date_format("timestamp", "HH").cast("int"))
           .drop("nodes", "expiry_series_index", "year", "month", "day", "hour")
          )

# COMMAND ----------

@dlt.table(
    comment="Expiry-hourly aggregated data for interest rate and volatility",
    table_properties={
        "quality":"gold", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED,SURFACES_TABLE,"agg-m1h")
)
def agg_surfaces_expiry_1h():
    return (dlt.read("cleaned_surfaces")
            .withWatermark("timestamp", "1 hour")
            .groupBy(F.window("timestamp", "1 hour").alias("timestamp_window"), "underlying", "expiry_timestamp")
            .agg(
             F.first("interest_rate", ignorenulls=True).alias("interest_rate"),
             F.first("vol_surface", ignorenulls=True).alias("vol_surface"),
            )
            .withColumn("date_", F.to_date(F.col("timestamp_window.end") - F.expr('INTERVAL 1 HOUR')))
            .withColumn("hour_", F.date_format(F.col("timestamp_window.end") - F.expr('INTERVAL 1 HOUR'), "HH").cast("int"))
            )
