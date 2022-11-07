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

TRANSACTIONS_TABLE = "transactions"
MARKETS_TABLE = "markets"
MARGIN_ACCOUNTS_PNL_TABLE = "margin-accounts-pnl"

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
# MAGIC ## Markets

# COMMAND ----------

markets_schema = """
underlying string,
active_timestamp timestamp,
expiry_timestamp timestamp, 
strike double, 
kind string, 
perp_sync_queue_head int,
perp_sync_queue_length int,
market_pub_key string, 
timestamp timestamp, 
slot bigint
"""

@dlt.table(
    comment="Raw metadata for Zeta's Serum markets",
    table_properties={
        "quality":"bronze", 
    },
    path=join(BASE_PATH_TRANSFORMED,MARKETS_TABLE,"raw"),
    schema=markets_schema
)
def raw_markets():
    return (spark
           .readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "json")
           .option("cloudFiles.region", "ap-southeast-1")
           .option("cloudFiles.includeExistingFiles", True)
           .option("cloudFiles.useNotifications", True)
           .option("partitionColumns", "year,month,day,hour")
           .schema(markets_schema)
           .load(join(BASE_PATH_LANDED,MARKETS_TABLE,"data"))
          )

# COMMAND ----------

@dlt.table(
    comment="Cleaned metadata for Zeta's Serum markets",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["underlying"],
    path=join(BASE_PATH_TRANSFORMED,MARKETS_TABLE,"cleaned")
)
def cleaned_markets():
    return (dlt.read_stream("raw_markets")
           .withWatermark("timestamp", "1 hour")
           .dropDuplicates(["underlying","active_timestamp","expiry_timestamp","strike","kind","market_pub_key"])
           .drop("timestamp","slot")
          )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transactions

# COMMAND ----------

transactions_schema = """
transaction_id string,
block_timestamp timestamp, 
is_successful boolean, 
fee bigint, 
instructions array<
  struct<
    instruction struct<
      clientOrderId string,
      expiryIndex int,
      mapNonce int,
      nonce int,
      orderId string,
      orderType string,
      price double,
      side string,
      size double, 
      amount double,
      tag string,
      tradingFee double,
      oraclePrice double,
      isTaker boolean
    >,
    name string,
    named_accounts map<string, string>,
    program_id string
  >
>,
accounts array<string>, 
log_messages array<string>, 
slot bigint, 
fetch_timestamp timestamp, 
year string,
month string,
day string,
hour string
"""

@dlt.table(
    comment="Raw data for platform transactions",
    table_properties={
        "quality":"bronze", 
    },
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"raw"),
    schema=transactions_schema
)
def raw_transactions():
    return (spark
           .readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "json")
           .option("cloudFiles.region", "ap-southeast-1")
           .option("cloudFiles.includeExistingFiles", True)
           .option("cloudFiles.useNotifications", True)
           .option("partitionColumns", "year,month,day,hour")
           .schema(transactions_schema)
           .load(join(BASE_PATH_LANDED,TRANSACTIONS_TABLE,"data"))
          )

# COMMAND ----------

@dlt.view
def zetagroup_mapping():
    return spark.table(f"zetadex_{NETWORK}.zetagroup_mapping")

# Transactions
@dlt.table(
    comment="Cleaned data for platform transactions",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"cleaned")
)
def cleaned_transactions():
    return (dlt.read_stream("raw_transactions")
           .withWatermark("block_timestamp", "1 hour")
           .dropDuplicates(["transaction_id"])
           .filter("is_successful")
           .drop("year", "month", "day", "hour")
           .withColumn("date_", F.to_date("block_timestamp"))
           .withColumn("hour_", F.date_format("block_timestamp", "HH").cast("int"))
          )
    
# Deposits
@dlt.table(
    comment="Cleaned data for deposit instructions",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"cleaned-ix-deposit")
)
def cleaned_ix_deposit():
    zetagroup_mapping_df = dlt.read("zetagroup_mapping")
    return (dlt.read_stream("cleaned_transactions")
           .withWatermark("block_timestamp", "1 hour")
           .select("*", F.posexplode("instructions").alias("instruction_index", "instruction"))
           .filter(f"instruction.name == 'deposit'")
           .join(zetagroup_mapping_df, 
                 (F.col("instruction.named_accounts.zetaGroup") == zetagroup_mapping_df.zetagroup_pub_key),
                 how="left"
                )
           .select("transaction_id",
                   "block_timestamp",
                   "instruction_index",
                   "instruction.name",
                   "instruction.instruction.amount",
                   "instruction.named_accounts",
                   "instruction.program_id",
                   "underlying"
                  )
           .withColumn("date_", F.to_date("block_timestamp"))
           .withColumn("hour_", F.date_format("block_timestamp", "HH").cast("int"))
          )
    
# Withdraw
@dlt.table(
    comment="Cleaned data for withdraw instructions",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"cleaned-ix-withdraw")
)
def cleaned_ix_withdraw():
    zetagroup_mapping_df = dlt.read("zetagroup_mapping")
    return (dlt.read_stream("cleaned_transactions")
           .withWatermark("block_timestamp", "1 hour")
           .select("*", F.posexplode("instructions").alias("instruction_index", "instruction"))
           .filter(f"instruction.name == 'withdraw'")
           .join(zetagroup_mapping_df, 
                 (F.col("instruction.named_accounts.zetaGroup") == zetagroup_mapping_df.zetagroup_pub_key),
                 how="left"
                )
           .select("transaction_id",
                   "block_timestamp",
                   "instruction_index",
                   "instruction.name",
                   "instruction.instruction.amount",
                   "instruction.named_accounts",
                   "instruction.program_id",
                   "underlying"
                  )
           .withColumn("date_", F.to_date("block_timestamp"))
           .withColumn("hour_", F.date_format("block_timestamp", "HH").cast("int"))
                     )
    
# Place order
@dlt.table(
    comment="Cleaned data for placeOrder instructions",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"cleaned-ix-place-order")
)
def cleaned_ix_place_order():
    markets_df = dlt.read("cleaned_markets")
    return (dlt.read_stream("cleaned_transactions")
           .withWatermark("block_timestamp", "1 hour")
           .select("*", F.posexplode("instructions").alias("instruction_index", "instruction"))
           .filter(F.col("instruction.name").startswith('placeOrder'))
           .join(markets_df, 
                 (F.col("instruction.named_accounts.market") == markets_df.market_pub_key)
                  & F.col("block_timestamp").between(markets_df.active_timestamp, markets_df.expiry_timestamp),
                 how="left"
                )
           .select("transaction_id",
                   "block_timestamp",
                   "instruction_index",
                   "underlying",
                   F.col("expiry_timestamp").alias("expiry"),
                   "strike",
                   "kind",
                   "instruction.name",
                   "instruction.instruction.price",
                   "instruction.instruction.size",
                   "instruction.instruction.side",
                   F.col("instruction.instruction.orderType").alias("order_type"),
                   F.col("instruction.instruction.isTaker").alias("is_taker"),
                   F.col("instruction.instruction.orderId").alias("order_id"),
                   F.col("instruction.instruction.clientOrderId").alias("client_order_id"),
                   F.col("instruction.instruction.tradingFee").alias("trading_fee"),
                   F.col("instruction.instruction.oraclePrice").alias("oracle_price"),
                   "instruction.instruction.tag",
                   "instruction.named_accounts",
                   "instruction.program_id"
                  )
           .withColumn("date_", F.to_date("block_timestamp"))
           .withColumn("hour_", F.date_format("block_timestamp", "HH").cast("int"))
                        )
    
# Cancel order
@dlt.table(
    comment="Cleaned data for cancelOrder instructions",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"cleaned-ix-cancel-order")
)
def cleaned_ix_cancel_order():
    markets_df = dlt.read("cleaned_markets")
    return (dlt.read_stream("cleaned_transactions")
           .withWatermark("block_timestamp", "1 hour")
           .select("*", F.posexplode("instructions").alias("instruction_index", "instruction"))
           .filter("""
           instruction.name == 'cancelOrder'
           or instruction.name == 'cancelOrderHalted'
           or instruction.name == 'cancelOrderByClientOrderId'
           or instruction.name == 'cancelExpiredOrder'
           """)
           .join(markets_df, 
                 (F.col("instruction.named_accounts.market") == markets_df.market_pub_key)
                  & F.col("block_timestamp").between(markets_df.active_timestamp, markets_df.expiry_timestamp),
                 how="left"
                )
           .select("transaction_id",
                   "block_timestamp",
                   "instruction_index",
                   "underlying",
                   F.col("expiry_timestamp").alias("expiry"),
                   "strike",
                   "kind",
                   "instruction.name",
                   "instruction.instruction.side",
                   F.col("instruction.instruction.orderId").alias("order_id"),
                   F.col("instruction.instruction.clientOrderId").alias("client_order_id"),
                   "instruction.named_accounts",
                   "instruction.program_id"
                  )
           .withColumn("date_", F.to_date("block_timestamp"))
           .withColumn("hour_", F.date_format("block_timestamp", "HH").cast("int"))
                         )
    
# Liquidate
@dlt.table(
    comment="Cleaned data for liquidate instructions",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"cleaned-ix-liquidate")
)
def cleaned_ix_liquidate():
    markets_df = dlt.read("cleaned_markets")
    return (dlt.read_stream("cleaned_transactions")
           .withWatermark("block_timestamp", "1 hour")
           .select("*", F.posexplode("instructions").alias("instruction_index", "instruction"))
           .filter("instruction.name == 'liquidate'")
           .join(markets_df, (F.col("instruction.named_accounts.market") == markets_df.market_pub_key)
                & F.col("block_timestamp").between(markets_df.active_timestamp, markets_df.expiry_timestamp) )
           .select("transaction_id",
                   "block_timestamp",
                   "instruction_index",
                   "underlying",
                   F.col("expiry_timestamp").alias("expiry"),
                   "strike",
                   "kind",
                   "instruction.name",
                   "instruction.instruction.size",
                   "instruction.named_accounts",
                   "instruction.program_id"
                  )
           .withColumn("date_", F.to_date("block_timestamp"))
           .withColumn("hour_", F.date_format("block_timestamp", "HH").cast("int"))
                      )

# COMMAND ----------

@dlt.table(
    comment="User-underlying-hourly aggregated data for deposits",
    table_properties={
        "quality":"gold", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"agg-ix-deposit-u1h"),
)
def agg_ix_deposit_u1h():
    return (dlt.read_stream("cleaned_ix_deposit")
                 .withWatermark("block_timestamp", "1 hour")
                 .groupBy(F.window("block_timestamp", "1 hour").alias("timestamp_window"), F.col("named_accounts.authority").alias("owner_pub_key"), "underlying")
                 .agg(
                     F.count("*").alias("deposit_count"),
                     F.sum("amount").alias("deposit_amount")
                  )
                 .withColumn("date_", F.to_date(F.col("timestamp_window.end") - F.expr('INTERVAL 1 HOUR')))
                 .withColumn("hour_", F.date_format(F.col("timestamp_window.end") - F.expr('INTERVAL 1 HOUR'), "HH").cast("int"))
            )
    
@dlt.table(
    comment="User-underlying-hourly aggregated data for withdrawals",
    table_properties={
        "quality":"gold", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"agg-ix-withdraw-u1h"),
)
def agg_ix_withdraw_u1h():
    return (dlt.read_stream("cleaned_ix_withdraw")
                 .withWatermark("block_timestamp", "1 hour")
                 .groupBy(F.window("block_timestamp", "1 hour").alias("timestamp_window"), F.col("named_accounts.authority").alias("owner_pub_key"), "underlying")
                 .agg(
                     F.count("*").alias("withdraw_count"),
                     F.sum("amount").alias("withdraw_amount")
                  )
                 .withColumn("date_", F.to_date(F.col("timestamp_window.end") - F.expr('INTERVAL 1 HOUR')))
                 .withColumn("hour_", F.date_format(F.col("timestamp_window.end") - F.expr('INTERVAL 1 HOUR'), "HH").cast("int"))
            )

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
    path=join(BASE_PATH_TRANSFORMED,MARGIN_ACCOUNTS_PNL_TABLE,"raw"),
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
           .load(join(BASE_PATH_LANDED,MARGIN_ACCOUNTS_PNL_TABLE,"data"))
          )

# COMMAND ----------

# @dlt.table(
#     comment="Cleaned data for margin account profit and loss",
#     table_properties={
#         "quality":"silver", 
#         "pipelines.autoOptimize.zOrderCols":"owner_pub_key"
#     },
#     partition_cols=["date_"],
#     path=join(BASE_PATH_TRANSFORMED,MARGIN_ACCOUNTS_PNL_TABLE,"cleaned")
# )
# def cleaned_pnl():
#     deposits_df = (dlt.read_stream("agg_ix_deposit_u1h")
#                  .withColumn("date_hour", F.date_trunc("hour", F.col("timestamp_window.start")))
#                  .withWatermark("date_hour", "1 hour")
#                  .drop("date_","hour_")
#                 )
#     withdrawals_df = (dlt.read_stream("agg_ix_withdraw_u1h")
#                  .withColumn("date_hour", F.date_trunc("hour", F.col("timestamp_window.start")))
#                  .withWatermark("date_hour", "1 hour")
#                  .drop("date_","hour_")
#                 )
#     return (dlt.read_stream("raw_pnl")
#            .withWatermark("timestamp", "1 hour")
#            .dropDuplicates(["underlying","year","month","day","hour"])
#            .join(deposits_df.alias("d"),
#               F.expr("""
#                raw_pnl.underlying = d.underlying AND
#                raw_pnl.owner_pub_key = d.owner_pub_key AND
#                raw_pnl.timestamp >= d.date_hour AND raw_pnl.timestamp < d.date_hour + interval 1 hour
#                """), 
#               how="left")
#            .join(withdrawals_df.alias("w"),
#               F.expr("""
#                raw_pnl.underlying = w.underlying AND
#                raw_pnl.owner_pub_key = w.owner_pub_key AND
#                raw_pnl.timestamp >= w.date_hour AND raw_pnl.timestamp < w.date_hour + interval 1 hour
#                """), 
#               how="left")
# #            .withColumn("deposits", F.coalesce("deposits",F.lit(0)))
# #            .withColumn("withdrawals", F.coalesce("withdrawals",F.lit(0)))
#            .withColumn("pnl", F.col("balance") + F.col("unrealized_pnl") - (F.col("deposit_amount") - F.col("withdraw_amount")))
#            .withColumn("date_", F.to_date("timestamp"))
#            .withColumn("hour_", F.date_format("timestamp", "HH").cast("int"))
#            .select(
#                "raw_pnl.timestamp",
#                "raw_pnl.underlying",
#                "raw_pnl.owner_pub_key",
#                "balance",
#                "unrealized_pnl",
#                "deposit_amount",
#                "withdraw_amount",
#                "pnl",
#                "date_",
#                "hour_"
#            )
#           )

# COMMAND ----------

@dlt.table(
    comment="Cleaned data for margin account profit and loss",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"owner_pub_key"
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED,MARGIN_ACCOUNTS_PNL_TABLE,"cleaned")
)
def cleaned_pnl():
    deposits_df = (dlt.read("agg_ix_deposit_u1h")
#                  .withColumn("date_hour", F.date_trunc("hour", F.col("timestamp_window.start")))
#                  .withWatermark("date_hour", "1 hour")
                 .drop("date_","hour_")
                )
    withdrawals_df = (dlt.read("agg_ix_withdraw_u1h")
#                  .withColumn("date_hour", F.date_trunc("hour", F.col("timestamp_window.start")))
#                  .withWatermark("date_hour", "1 hour")
                 .drop("date_","hour_")
                )
    return (dlt.read("raw_pnl") # ideally would be read_stream
           .withWatermark("timestamp", "1 hour")
           .dropDuplicates(["owner_pub_key","underlying","year","month","day","hour"])
           .join(deposits_df.alias("d"),
              F.expr("""
               raw_pnl.underlying = d.underlying AND
               raw_pnl.owner_pub_key = d.owner_pub_key AND
               raw_pnl.timestamp >= d.timestamp_window.start + interval '1' hour AND raw_pnl.timestamp < d.timestamp_window.end + interval '1' hour
               """),
              how="left")
           .join(withdrawals_df.alias("w"),
              F.expr("""
               raw_pnl.underlying = w.underlying AND
               raw_pnl.owner_pub_key = w.owner_pub_key AND
               raw_pnl.timestamp >= w.timestamp_window.start + interval '1' hour AND raw_pnl.timestamp < w.timestamp_window.end + interval '1' hour
               """),
              how="left")
           .withColumn("deposit_amount", F.coalesce("deposit_amount",F.lit(0)))
           .withColumn("withdraw_amount", F.coalesce("withdraw_amount",F.lit(0)))
           .withColumn("deposit_amount_cumsum", F.sum("deposit_amount").over(Window.partitionBy("raw_pnl.underlying","raw_pnl.owner_pub_key").orderBy("raw_pnl.timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)))
           .withColumn("withdraw_amount_cumsum", F.sum("withdraw_amount").over(Window.partitionBy("raw_pnl.underlying","raw_pnl.owner_pub_key").orderBy("raw_pnl.timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)))
           .withColumn("pnl", F.col("balance") + F.col("unrealized_pnl") - (F.col("deposit_amount_cumsum") - F.col("withdraw_amount_cumsum")))
           .withColumn("date_", F.to_date("timestamp"))
           .withColumn("hour_", F.date_format("timestamp", "HH").cast("int"))
           .select(
               "raw_pnl.timestamp",
               "raw_pnl.underlying",
               "raw_pnl.owner_pub_key",
               "balance",
               "unrealized_pnl",
               "pnl",
               "deposit_amount",
               "withdraw_amount",
               "deposit_amount_cumsum",
               "withdraw_amount_cumsum",
               "date_",
               "hour_"
           )
          )

# COMMAND ----------

# @dlt.table(
#     comment="Cleaned data for margin account profit and loss",
#     table_properties={
#         "quality":"silver", 
#         "pipelines.autoOptimize.zOrderCols":"owner_pub_key"
#     },
#     partition_cols=["date_"],
#     path=join(BASE_PATH_TRANSFORMED,MARGIN_ACCOUNTS_PNL_TABLE,"cleaned")
# )
# def cleaned_pnl():
#     deposits_df = (dlt.read_stream("cleaned_ix_deposit")
#                  .withColumn("date_hour", F.date_trunc("hour", "block_timestamp"))
#                  .withWatermark("date_hour", "1 hour")
#                  .drop("date_","hour_")
#                 )
#     withdrawals_df = (dlt.read_stream("cleaned_ix_withdraw")
#                  .withColumn("date_hour", F.date_trunc("hour", "block_timestamp"))
#                  .withWatermark("date_hour", "1 hour")
#                  .drop("date_","hour_")
#                 )
#     return (dlt.read_stream("raw_pnl")
#            .withWatermark("timestamp", "1 hour")
#            .dropDuplicates(["underlying","year","month","day","hour"])
#            .join(deposits_df.alias("d"),
#               F.expr("""
#                raw_pnl.underlying = d.underlying AND
#                raw_pnl.owner_pub_key = d.named_accounts.authority AND
#                raw_pnl.timestamp >= d.date_hour AND raw_pnl.timestamp < d.date_hour + interval 1 hour
#                """), 
#               how="left")
#            .join(withdrawals_df.alias("w"),
#               F.expr("""
#                raw_pnl.underlying = w.underlying AND
#                raw_pnl.owner_pub_key = w.named_accounts.authority AND
#                raw_pnl.timestamp >= w.date_hour AND raw_pnl.timestamp < w.date_hour + interval 1 hour
#                """), 
#               how="left")
#             .groupBy(F.date_trunc("hour", "timestamp").alias("timestamp_hour"), "owner_pub_key", "raw_pnl.underlying")
#             .agg(
#                 F.first("balance").alias("balance"),
#                 F.first("unrealized_pnl").alias("unrealized_pnl"),
#                 F.sum("d.amount").alias("deposit_amount"),
#                 F.sum("w.amount").alias("withdraw_amount")
#              )
#            .withColumn("pnl", F.col("balance") + F.col("unrealized_pnl") - (F.col("deposit_amount") - F.col("withdraw_amount")))
#            .withColumn("date_", F.to_date("timestamp_hour"))
#            .withColumn("hour_", F.date_format("timestamp_hour", "HH").cast("int"))
#            .select(
#                "timestamp_hour",
#                "raw_pnl.underlying",
#                "raw_pnl.owner_pub_key",
#                "balance",
#                "unrealized_pnl",
#                "deposit_amount",
#                "withdraw_amount",
#                "pnl",
#                "date_",
#                "hour_"
#            )
#           )

# COMMAND ----------

windowSpec = Window.partitionBy("owner_pub_key").orderBy("timestamp")
windowSpec24h = Window.partitionBy("owner_pub_key").orderBy("timestamp").rowsBetween(-24, Window.currentRow)
windowSpec7d = Window.partitionBy("owner_pub_key").orderBy("timestamp").rowsBetween(-24*7, Window.currentRow)
windowSpec30d = Window.partitionBy("owner_pub_key").orderBy("timestamp").rowsBetween(-24*7*30, Window.currentRow)

# Break ties in PnL ranking by pubkey alphabetically
@dlt.table(
    comment="User (24h, 7d, 30h) aggregated data for margin account profit and loss",
    table_properties={
        "quality":"gold", 
        "pipelines.autoOptimize.zOrderCols":"owner_pub_key"
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED,MARGIN_ACCOUNTS_PNL_TABLE,"agg")
)
def agg_pnl():
    return (dlt.read("cleaned_pnl")
            .withWatermark("timestamp", "1 hour")
            .groupBy("timestamp","date_","hour_","owner_pub_key")
            .agg(
                F.sum("pnl").alias("pnl"),
                F.sum("deposit_amount").alias("deposit_amount"),
                F.sum("withdraw_amount").alias("withdraw_amount"),
                F.sum("balance").alias("balance"),
                )
            .withColumn("pnl_lag_24h",F.lag("pnl",24).over(windowSpec))
            .withColumn("pnl_lag_7d",F.lag("pnl",24*7).over(windowSpec))
            .withColumn("pnl_lag_30d",F.lag("pnl",24*7*30).over(windowSpec))
            .withColumn("balance_lag_24h",F.lag("balance",24).over(windowSpec))
            .withColumn("balance_lag_7d",F.lag("balance",24*7).over(windowSpec))
            .withColumn("balance_lag_30d",F.lag("balance",24*7*30).over(windowSpec))
            .withColumn("pnl_diff_24h",F.col("pnl")-F.col("pnl_lag_24h")-(F.sum("deposit_amount").over(windowSpec24h)-F.sum("withdraw_amount").over(windowSpec24h)))
            .withColumn("pnl_diff_7d",F.col("pnl")-F.col("pnl_lag_7d")-(F.sum("deposit_amount").over(windowSpec7d)-F.sum("withdraw_amount").over(windowSpec7d)))
            .withColumn("pnl_diff_30d",F.col("pnl")-F.col("pnl_lag_30d")-(F.sum("deposit_amount").over(windowSpec30d)-F.sum("withdraw_amount").over(windowSpec30d)))
            .withColumn("pnl_ratio_24h",F.col("pnl_diff_24h")/F.col("balance_lag_24h"))
            .withColumn("pnl_ratio_7d",F.col("pnl_diff_7d")/F.col("balance_lag_7d"))
            .withColumn("pnl_ratio_30d",F.col("pnl_diff_30d")/F.col("balance_lag_30d"))
            .withColumn("pnl_ratio_24h_rank", F.rank().over(Window.partitionBy("date_","hour_").orderBy(F.desc("pnl_ratio_24h"),F.desc("pnl_diff_24h"), "owner_pub_key")))
            .withColumn("pnl_ratio_7d_rank", F.rank().over(Window.partitionBy("date_","hour_").orderBy(F.desc("pnl_ratio_7d"),F.desc("pnl_diff_7d"), "owner_pub_key")))
            .withColumn("pnl_ratio_30d_rank", F.rank().over(Window.partitionBy("date_","hour_").orderBy(F.desc("pnl_ratio_30d"),F.desc("pnl_diff_30d"), "owner_pub_key")))
            .withColumn("pnl_diff_24h_rank", F.rank().over(Window.partitionBy("date_","hour_").orderBy(F.desc("pnl_diff_24h"),F.desc("pnl_ratio_24h"), "owner_pub_key")))
            .withColumn("pnl_diff_7d_rank", F.rank().over(Window.partitionBy("date_","hour_").orderBy(F.desc("pnl_diff_7d"),F.desc("pnl_ratio_7d"), "owner_pub_key")))
            .withColumn("pnl_diff_30d_rank", F.rank().over(Window.partitionBy("date_","hour_").orderBy(F.desc("pnl_diff_30d"),F.desc("pnl_ratio_30d"), "owner_pub_key")))
            .filter(f"date_ = '{current_date}' and hour_ = {current_hour}")
            )

# COMMAND ----------


