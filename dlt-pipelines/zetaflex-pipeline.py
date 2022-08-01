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
AUCTION_TABLE = "flex-snapshot-auctions"

# COMMAND ----------

# MAGIC %md
# MAGIC # ETL

# COMMAND ----------

S3_BUCKET_LANDED = f"zetaflex-{NETWORK}-landing"
BASE_PATH_TRANSFORMED = join("/mnt", S3_BUCKET_LANDED)

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
    path=join(BASE_PATH,TVL_TABLE,"raw"),
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
           .load(join(BASE_PATH_LANDED,TVL_TABLE,"data"))
          )

# COMMAND ----------

@dlt.table(
    comment="Cleaned data for TVL",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_", "hour_"],
    path=join(BASE_PATH_TRANSFORMED,TVL_TABLE,"silver/data")
)
def cleaned_tvl():
    return (dlt.read_stream("raw_tvl")
           .withWatermark("timestamp", "1 day")
           .dropDuplicates()
           .withColumn("date_", F.to_date("timestamp"))
           .withColumn("hour_", F.date_format("timestamp", "HH"))
          )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auctions

# COMMAND ----------

df = spark.read.json("/mnt/zetaflex-mainnet-landing/flex-snapshot-auctions/data/year=2022/month=6/day=28/hour=7/snapshot-combo-options-1658991621651.json")

# COMMAND ----------

display(df)

# COMMAND ----------

auction_schema = """
  auctionTokenVault string,
  auctionTokenMint string,
  bidCurrencyMint string,
  creator string,
  auctionTokenAmount number,
  bidEnd timestamp,
  cooldownEnd timestamp,
  winningBidder string,
  exchangeAmount number,
"""

@dlt.table(
    comment="Raw data for auctions",
    table_properties={
        "quality":"bronze", 
    },
    path=join(BASE_PATH,AUCTION_TABLE,"raw"),
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
           .schema(auction_schema)
           .load(join(BASE_PATH_LANDED,AUCTION_TABLE,"data"))
          )
