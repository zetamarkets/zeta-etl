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

REFERRERS_TABLE = "referrers"
REFERRALS_TABLE = "referrals"

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
# MAGIC ## Referrers

# COMMAND ----------

referrers_schema = """
referrer string,
alias string,
indexed_timestamp timestamp,
year string,
month string,
day string,
hour string
"""

@dlt.table(
    comment="Raw data for referrers",
    table_properties={
        "quality":"bronze", 
    },
    path=join(BASE_PATH_TRANSFORMED,REFERRERS_TABLE,"raw"),
    schema=referrers_schema
)
def raw_referrers():
    return (spark
           .readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "json")
           .option("cloudFiles.region", "ap-southeast-1")
           .option("cloudFiles.includeExistingFiles", True)
           .option("cloudFiles.useNotifications", True)
           .option("partitionColumns", "year,month,day,hour")
           .schema(referrers_schema)
           .load(join(BASE_PATH_LANDED,REFERRERS_TABLE,"data"))
          )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Referrals

# COMMAND ----------

referrals_schema = """
referrer string,
referral string,
timestamp timestamp,
indexed_timestamp timestamp,
year string,
month string,
day string,
hour string
"""

@dlt.table(
    comment="Raw data for referrals",
    table_properties={
        "quality":"bronze", 
    },
    path=join(BASE_PATH_TRANSFORMED,REFERRALS_TABLE,"raw"),
    schema=referrals_schema
)
def raw_referrals():
    return (spark
           .readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "json")
           .option("cloudFiles.region", "ap-southeast-1")
           .option("cloudFiles.includeExistingFiles", True)
           .option("cloudFiles.useNotifications", True)
           .option("partitionColumns", "year,month,day,hour")
           .schema(referrals_schema)
           .load(join(BASE_PATH_LANDED,REFERRALS_TABLE,"data"))
          )

# COMMAND ----------

@dlt.view()
def raw_referrals_v():
    referrals_df = (dlt.read_stream("raw_referrals")
                       .withWatermark("indexed_timestamp", "1 hour"))
    return (dlt.read_stream("raw_referrers")
            .withWatermark("indexed_timestamp", "1 hour")
#            .dropDuplicates(["underlying", "expiry_timestamp", "slot"])
           .join(referrals_df,
                on=["indexed_timestamp", "referrer"],
                how="inner")
           .select("referrer", "alias", "referral", "timestamp", "raw_referrers.indexed_timestamp")
           .withColumn("date_", F.to_date("indexed_timestamp"))
           .withColumn("hour_", F.date_format("indexed_timestamp", "HH").cast("int"))
          )

# COMMAND ----------

dlt.create_streaming_live_table(
    name="cleaned_referrals",
    comment="Cleaned and deduped referrers and referrals",
    table_properties={
        "quality": "silver"
    }
)

dlt.apply_changes(
    target = "cleaned_referrals", 
    source = "raw_referrals_v", 
    keys = ["referrer","referral"], 
    sequence_by = "indexed_timestamp"
)
