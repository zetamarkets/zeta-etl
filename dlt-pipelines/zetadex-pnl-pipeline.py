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

MARGIN_ACCOUNTS_PNL_TABLE = "margin-accounts-pnl"

# COMMAND ----------

# MAGIC %md
# MAGIC # ETL

# COMMAND ----------

S3_BUCKET = f"zetadex-{NETWORK}"
BASE_PATH = join("/mnt", S3_BUCKET)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Margin Account PnL

# COMMAND ----------

pnl_schema = """
timestamp timestamp,
underlying string,
owner_pub_key string,
balance double,
unrealized_pnl double,
year string,
month string,
day string,
hour string
"""

@dlt.table(
    comment="Raw data for margin account profit and loss",
    table_properties={
        "quality":"bronze", 
    },
    path=join(BASE_PATH,MARGIN_ACCOUNTS_PNL_TABLE,"bronze/data"),
    schema=pnl_schema
)
def raw_pnl():
    return (spark
           .readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "json")
           .option("cloudFiles.region", "ap-southeast-1")
           .option("cloudFiles.includeExistingFiles", True)
           .option("cloudFiles.useNotifications", True)
           .option("partitionColumns", "year,month,day,hour")
           .option("mergeSchema", "true")
           .schema(pnl_schema)
           .load(join(BASE_PATH,MARGIN_ACCOUNTS_PNL_TABLE,"raw/data"))
          )

# COMMAND ----------

@dlt.table(
    comment="Cleaned data for margin account profit and loss",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"owner_pub_key"
    },
    partition_cols=["date_", "hour_"],
    path=join(BASE_PATH,MARGIN_ACCOUNTS_PNL_TABLE,"silver/data")
)
def cleaned_pnl():
    return (dlt.read_stream("raw_pnl")
           .withWatermark("timestamp", "1 hour")
           .dropDuplicates()
           .withColumn("pnl", F.col("balance") + F.col("unrealized_pnl"))
           .withColumn("date_", F.to_date("timestamp"))
           .withColumn("hour_", F.date_format("timestamp", "HH").cast("int"))
           .drop("nodes", "expiry_series_index", "year", "month", "day", "hour")
          )

# COMMAND ----------

windowSpec = Window.partitionBy("owner_pub_key").orderBy("timestamp")

# Break ties in PnL ranking by pubkey alphabetically
@dlt.table(
    comment="User (24h, 7d, 30h) aggregated data for margin account profit and loss",
    table_properties={
        "quality":"gold", 
        "pipelines.autoOptimize.zOrderCols":"owner_pub_key"
    },
    partition_cols=["date_", "hour_"],
    path=join(BASE_PATH,MARGIN_ACCOUNTS_PNL_TABLE,"gold/data")
)
def agg_pnl():
    return (dlt.read("cleaned_pnl") # read?
            .withWatermark("timestamp", "1 hour")
            .groupBy("timestamp","date_","hour_","owner_pub_key")
            .agg(F.sum("pnl").alias("pnl"))
            .withColumn("pnl_lag_24h",F.lag("pnl",24).over(windowSpec))
            .withColumn("pnl_lag_7d",F.lag("pnl",24*7).over(windowSpec))
            .withColumn("pnl_lag_30d",F.lag("pnl",24*7*30).over(windowSpec))
            .withColumn("pnl_diff_24h",F.col("pnl")-F.col("pnl_lag_24h"))
            .withColumn("pnl_diff_7d",F.col("pnl")-F.col("pnl_lag_7d"))
            .withColumn("pnl_diff_30d",F.col("pnl")-F.col("pnl_lag_30d"))
            .withColumn("pnl_ratio_24h",F.col("pnl_diff_24h")/F.col("pnl_lag_24h"))
            .withColumn("pnl_ratio_7d",F.col("pnl_diff_7d")/F.col("pnl_lag_7d"))
            .withColumn("pnl_ratio_30d",F.col("pnl_diff_30d")/F.col("pnl_lag_30d"))
            .withColumn("pnl_ratio_24h_rank", F.rank().over(Window.partitionBy("date_","hour_").orderBy(F.desc("pnl_ratio_24h"),F.desc("pnl_diff_24h"), "owner_pub_key")))
            .withColumn("pnl_ratio_7d_rank", F.rank().over(Window.partitionBy("date_","hour_").orderBy(F.desc("pnl_ratio_7d"),F.desc("pnl_diff_7d"), "owner_pub_key")))
            .withColumn("pnl_ratio_30d_rank", F.rank().over(Window.partitionBy("date_","hour_").orderBy(F.desc("pnl_ratio_30d"),F.desc("pnl_diff_30d"), "owner_pub_key")))
            .withColumn("pnl_diff_24h_rank", F.rank().over(Window.partitionBy("date_","hour_").orderBy(F.desc("pnl_diff_24h"),F.desc("pnl_ratio_24h"), "owner_pub_key")))
            .withColumn("pnl_diff_7d_rank", F.rank().over(Window.partitionBy("date_","hour_").orderBy(F.desc("pnl_diff_7d"),F.desc("pnl_ratio_7d"), "owner_pub_key")))
            .withColumn("pnl_diff_30d_rank", F.rank().over(Window.partitionBy("date_","hour_").orderBy(F.desc("pnl_diff_30d"),F.desc("pnl_ratio_30d"), "owner_pub_key")))
            .filter(f"date_ = '{current_date}' and hour_ = {current_hour}")
            )

# COMMAND ----------


