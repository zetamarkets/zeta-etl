# Databricks notebook source
# Databricks notebook source
dbutils.widgets.dropdown("network", "devnet", ["devnet", "mainnet"], "Network")
# NETWORK = dbutils.widgets.get("network")
NETWORK = spark.conf.get("pipeline.network")

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T
from os.path import join
import dlt

# COMMAND ----------

TVL_TABLE = "tvl"

# COMMAND ----------

# MAGIC %md
# MAGIC # ETL

# COMMAND ----------

S3_BUCKET = f"zetaflex-{NETWORK}"
BASE_PATH = join("/mnt", S3_BUCKET)

# COMMAND ----------

# df = spark.read.json("/mnt/zetaflex-mainnet/tvl/raw/data/year=2022/month=05/day=19/hour=18/PUT-S3-zetaflex-mainnet-tvl-1-2022-05-19-18-05-13-63195153-aa4f-4c42-9b56-bf3c25f32357")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TVL

# COMMAND ----------

# DBTITLE 1,Bronze
tvl_schema = """
timestamp timestamp,
tvl double,
year string,
month string,
day string,
hour string
"""

@dlt.table(
    comment="Raw data for TVL",
    table_properties={
        "quality":"bronze", 
    },
    path=join(BASE_PATH,TVL_TABLE,"bronze/data"),
    schema=tvl_schema
)
def raw_tvl():
    return (spark
           .readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "json")
           .option("cloudFiles.region", "ap-southeast-1")
           .option("cloudFiles.includeExistingFiles", True)
           .option("cloudFiles.useNotifications", True)
           .option("partitionColumns", "year,month,day,hour")
           .schema(tvl_schema)
           .load(join(BASE_PATH,TVL_TABLE,"raw/data"))
          )

# COMMAND ----------

@dlt.table(
    comment="Cleaned data for TVL",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_", "hour_"],
    path=join(BASE_PATH,TVL_TABLE,"silver/data")
)
def cleaned_tvl():
    return (dlt.read_stream("raw_tvl")
           .withWatermark("timestamp", "1 day")
           .dropDuplicates()
           .withColumn("date_", F.to_date("timestamp"))
           .withColumn("hour_", F.date_format("timestamp", "HH"))
          )
