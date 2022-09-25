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
COMBO_OPTION_TABLE = "flex-snapshot-combo-options"
OPTION_TABLE = "flex-snapshot-options"
SETTLEMENT_ACCOUNT_TABLE = "flex-snapshot-settlement-accounts"
UNDERLYING_TABLE = "flex-snapshot-underlyings"

# COMMAND ----------

# MAGIC %md
# MAGIC # ETL

# COMMAND ----------

S3_BUCKET_LANDED = f"zetaflex-{NETWORK}-landing"
BASE_PATH_LANDED = join("/mnt", S3_BUCKET_LANDED)
S3_BUCKET_TRANSFORMED = f"zetaflex-{NETWORK}"
BASE_PATH_TRANSFORMED = join("/mnt", S3_BUCKET_TRANSFORMED)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TVL

# COMMAND ----------

# DBTITLE 1,Bronze
tvl_schema = """
timestamp timestamp,
tvl double,
tokens Map<string, float>,
year string,
month string,
day string
"""

@dlt.table(
    comment="Raw data for flex TVL",
    table_properties={
        "quality":"bronze", 
    },
    path=join(BASE_PATH_TRANSFORMED,TVL_TABLE,"raw"),
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
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED,TVL_TABLE,"silver/data")
)
def cleaned_tvl():
    return (dlt.read_stream("raw_tvl")
           .withWatermark("timestamp", "1 day")
           .dropDuplicates(["year", "month", "day"])
           .withColumn("date_", F.to_date("timestamp"))
          )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auctions

# COMMAND ----------

auction_schema = """
  auction_address string,
  auction_token_vault string,
  auction_token_mint string,
  bid_currency_mint string,
  creator string,
  auction_token_amount bigint,
  bid_end timestamp,
  cooldown_end timestamp,
  winning_bidder string,
  exchange_amount bigint,
  indexed_timestamp timestamp,
  year string,
  month string,
  day string
"""

@dlt.table(
    comment="Raw data for flex auctions",
    table_properties={
        "quality":"bronze", 
    },
    path=join(BASE_PATH_TRANSFORMED,AUCTION_TABLE,"raw"),
    schema=auction_schema
)
def raw_auction():
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

# COMMAND ----------

dlt.create_streaming_live_table(
    name="cleaned_auction",
    comment="Cleaned and deduped auctions",
    table_properties={
        "quality": "silver"
    }
)

dlt.apply_changes(
    target = "cleaned_auction", 
    source = "raw_auction", 
    keys = ["auction_address"], 
    sequence_by = "indexed_timestamp"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combo Options

# COMMAND ----------

combo_option_schema = """
  combo_option_account string,
  collateral_vault string,
  option_mint string,
  underlying_mint string,
  collateral_mint string,
  creator string,
  underlying_count int,
  option_params array<
                  struct<
                      strike bigint,
                      kind string,
                      size int
                      >
                  >,
  settlement_start timestamp,
  settlement_end timestamp,
  settlement_type string,
  oracle_method string,
  collateral_amount int,
  settlement_price int,
  profit_per_option int,
  remaining_collateral int,
  indexed_timestamp timestamp,
  year string,
  month string,
  day string
"""

@dlt.table(
    comment="Raw data for flex combo options",
    table_properties={
        "quality":"bronze", 
    },
    path=join(BASE_PATH_TRANSFORMED,COMBO_OPTION_TABLE,"raw"),
    schema=combo_option_schema
)
def raw_combo_option():
    return (spark
           .readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "json")
           .option("cloudFiles.region", "ap-southeast-1")
           .option("cloudFiles.includeExistingFiles", True)
           .option("cloudFiles.useNotifications", True)
           .option("partitionColumns", "year,month,day,hour")
           .schema(combo_option_schema)
           .load(join(BASE_PATH_LANDED,COMBO_OPTION_TABLE,"data"))
          )

# COMMAND ----------

dlt.create_streaming_live_table(
    name="cleaned_combo_option",
    comment="Cleaned and deduped combo options",
    table_properties={
        "quality": "silver"
    }
)

dlt.apply_changes(
    target = "cleaned_combo_option", 
    source = "raw_combo_option", 
    keys = ["combo_option_account"], 
    sequence_by = "indexed_timestamp"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Options

# COMMAND ----------

option_schema = """
  option_account string,
  collateral_vault string,
  option_mint string,
  underlying_mint string,
  collateral_mint string,
  exercise_mint string,
  creator string,
  underlying_count bigint,
  strike string,
  kind string,
  settlement_start timestamp,
  settlement_end timestamp,
  settlement_type string,
  oracle_method string,
  collateral_amount bigint,
  settlement_price bigint,
  profit_per_option bigint,
  remaining_collateral bigint,
  indexed_timestamp timestamp,
  year string,
  month string,
  day string
"""

@dlt.table(
    comment="Raw data for flex options",
    table_properties={
        "quality":"bronze", 
    },
    path=join(BASE_PATH_TRANSFORMED,OPTION_TABLE,"raw"),
    schema=option_schema
)
def raw_option():
    return (spark
           .readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "json")
           .option("cloudFiles.region", "ap-southeast-1")
           .option("cloudFiles.includeExistingFiles", True)
           .option("cloudFiles.useNotifications", True)
           .option("partitionColumns", "year,month,day,hour")
           .schema(option_schema)
           .load(join(BASE_PATH_LANDED,OPTION_TABLE,"data"))
          )

# COMMAND ----------

dlt.create_streaming_live_table(
    name="cleaned_option",
    comment="Cleaned and deduped options",
    table_properties={
        "quality": "silver"
    }
)

dlt.apply_changes(
    target = "cleaned_option", 
    source = "raw_option", 
    keys = ["option_account"], 
    sequence_by = "indexed_timestamp"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Settlement Accounts

# COMMAND ----------

settlement_account_schema = """
  settlement_account_address string,
  underlying_mint string,
  expected_settlement_ts timestamp,
  actual_settlement_ts timestamp,
  settlement_price bigint,
  indexed_timestamp timestamp,
  year string,
  month string,
  day string
"""

@dlt.table(
    comment="Raw data for flex settlement accounts",
    table_properties={
        "quality":"bronze", 
    },
    path=join(BASE_PATH_TRANSFORMED,SETTLEMENT_ACCOUNT_TABLE,"raw"),
    schema=settlement_account_schema
)
def raw_settlement_account():
    return (spark
           .readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "json")
           .option("cloudFiles.region", "ap-southeast-1")
           .option("cloudFiles.includeExistingFiles", True)
           .option("cloudFiles.useNotifications", True)
           .option("partitionColumns", "year,month,day,hour")
           .schema(settlement_account_schema)
           .load(join(BASE_PATH_LANDED,SETTLEMENT_ACCOUNT_TABLE,"data"))
          )

# COMMAND ----------

dlt.create_streaming_live_table(
    name="cleaned_settlement_account",
    comment="Cleaned and deduped settlement accounts",
    table_properties={
        "quality": "silver"
    }
)

dlt.apply_changes(
    target = "cleaned_settlement_account", 
    source = "raw_settlement_account", 
    keys = ["settlement_account_address"], 
    sequence_by = "indexed_timestamp"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Underlyings

# COMMAND ----------

underlying_schema = """
  underlyings_address string,
  underlying_type string,
  mint string,
  oracle string,
  count bigint,
  indexed_timestamp timestamp,
  year string,
  month string,
  day string
"""

@dlt.table(
    comment="Raw data for flex underlyings",
    table_properties={
        "quality":"bronze", 
    },
    path=join(BASE_PATH_TRANSFORMED,UNDERLYING_TABLE,"raw"),
    schema=underlying_schema
)
def raw_underlying():
    return (spark
           .readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "json")
           .option("cloudFiles.region", "ap-southeast-1")
           .option("cloudFiles.includeExistingFiles", True)
           .option("cloudFiles.useNotifications", True)
           .option("partitionColumns", "year,month,day,hour")
           .schema(underlying_schema)
           .load(join(BASE_PATH_LANDED,UNDERLYING_TABLE,"data"))
          )

# COMMAND ----------

dlt.create_streaming_live_table(
    name="cleaned_underlying",
    comment="Cleaned and deduped underlyings",
    table_properties={
        "quality": "silver"
    }
)

dlt.apply_changes(
    target = "cleaned_underlying", 
    source = "raw_underlying", 
    keys = ["underlyings_address"], 
    sequence_by = "indexed_timestamp"
)

# COMMAND ----------


