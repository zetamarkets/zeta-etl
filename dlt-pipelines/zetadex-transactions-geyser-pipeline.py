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

PRICE_FACTOR = 1e6
SIZE_FACTOR = 1e3

TRANSACTIONS_TABLE = "transactions-geyser"

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
# MAGIC ## Transactions

# COMMAND ----------

# DBTITLE 1,Bronze
transactions_schema = """
signature string,
instructions array<
    struct<
        name string,
        args map<string,string>,
        accounts struct<
            named map<string, string>,
            remaining array<string>
        >,
        events array<
            struct<
                name string,
                event map<string,string>
            >
        >
    >
>,
is_successful boolean,
slot bigint,
block_time timestamp,
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
def raw_transactions_geyser():
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

# DBTITLE 1,Silver
@dlt.view
def zetagroup_mapping():
    return spark.table(f"zetadex_{NETWORK}.zetagroup_mapping")

@dlt.view
def cleaned_markets():
    return spark.table(f"zetadex_{NETWORK}.cleaned_markets")

# Transactions
@dlt.table(
    comment="Cleaned data for platform transactions",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"cleaned-transactions")
)
def cleaned_transactions_geyser():
    return (dlt.read_stream("raw_transactions_geyser")
           .withWatermark("block_time", "1 hour")
           .dropDuplicates(["signature"])
           .filter("is_successful")
           .drop("year", "month", "day", "hour")
           .withColumn("date_", F.to_date("block_time"))
           .withColumn("hour_", F.date_format("block_time", "HH").cast("int"))
          )
    
# Deposits
@dlt.table(
    comment="Cleaned data for deposit instructions",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_", "underlying"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"cleaned-ix-deposit")
)
def cleaned_ix_deposit_geyser():
    zetagroup_mapping_df = dlt.read("zetagroup_mapping")
    return (dlt.read_stream("cleaned_transactions_geyser")
           .withWatermark("block_time", "1 hour")
           .select("*", F.posexplode("instructions").alias("instruction_index", "instruction"))
           .filter(f"instruction.name == 'deposit'")
           .join(zetagroup_mapping_df, 
                 (F.col("instruction.accounts.named.zeta_group") == zetagroup_mapping_df.zetagroup_pub_key),
                 how="left"
                )
           .select("signature",
                   "instruction_index",
                   "instruction.name",
                   (F.col("instruction.args.amount") / PRICE_FACTOR).alias("deposit_amount"),
                   F.col("instruction.accounts.named").alias("accounts"),
                   "underlying",
                   "block_time",
                   "slot"
                  )
           .withColumn("date_", F.to_date("block_time"))
           .withColumn("hour_", F.date_format("block_time", "HH").cast("int"))
          )
    
# Withdraw
@dlt.table(
    comment="Cleaned data for withdraw instructions",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_", "underlying"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"cleaned-ix-withdraw")
)
def cleaned_ix_withdraw_geyser():
    zetagroup_mapping_df = dlt.read("zetagroup_mapping")
    return (dlt.read_stream("cleaned_transactions_geyser")
       .withWatermark("block_time", "1 hour")
       .select("*", F.posexplode("instructions").alias("instruction_index", "instruction"))
       .filter(f"instruction.name == 'withdraw'")
       .join(zetagroup_mapping_df, 
             (F.col("instruction.accounts.named.zeta_group") == zetagroup_mapping_df.zetagroup_pub_key),
             how="left"
            )
       .select("signature",
               "instruction_index",
               "instruction.name",
               (F.col("instruction.args.amount") / PRICE_FACTOR).alias("withdraw_amount"),
               F.col("instruction.accounts.named").alias("accounts"),
               "underlying",
               "block_time",
               "slot"
              )
       .withColumn("date_", F.to_date("block_time"))
       .withColumn("hour_", F.date_format("block_time", "HH").cast("int"))
      )
    
# Place order
@dlt.table(
    comment="Cleaned data for placeOrder type instructions",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_", "underlying"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"cleaned-ix-place-order")
)
def cleaned_ix_place_order_geyser():
    markets_df = dlt.read("cleaned_markets")
    return (dlt.read_stream("cleaned_transactions_geyser")
           .withWatermark("block_time", "1 hour")
           .select("*", F.posexplode("instructions").alias("instruction_index", "instruction"))
           .filter(F.col("instruction.name").startswith('place_order'))
           .withColumn("event", F.explode("instruction.events"))
           .filter("event.name == 'place_order_event'")
           .join(markets_df, 
                 (F.col("instruction.accounts.named.market") == markets_df.market_pub_key)
                  & F.col("block_time").between(markets_df.active_timestamp, markets_df.expiry_timestamp),
                 how="left"
                )
           .select("signature",
                   "instruction_index",
                   "underlying",
                   F.col("expiry_timestamp").alias("expiry"),
                   "strike",
                   "kind",
                   "instruction.name",
                   (F.col("instruction.args.price") / PRICE_FACTOR).alias("price"),
                   (F.col("instruction.args.size") / SIZE_FACTOR).alias("size"),
                   "instruction.args.side",
                   "instruction.args.order_type",
                   "instruction.args.client_order_id",
                   "instruction.args.tag",
                   (F.col("event.event.fee") / PRICE_FACTOR).alias("trading_fee"),
                   (F.col("event.event.oracle_price") / PRICE_FACTOR).alias("oracle_price"),
                   "event.event.order_id",
                   F.col("instruction.accounts.named").alias("accounts"),
                   "block_time",
                   "slot"
                  )
           .withColumn("date_", F.to_date("block_time"))
           .withColumn("hour_", F.date_format("block_time", "HH").cast("int"))
            )
    
# Cancel order
@dlt.table(
    comment="Cleaned data for cancelOrder type instructions",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_", "underlying"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"cleaned-ix-cancel-order")
)
def cleaned_ix_cancel_order_geyser():
    markets_df = dlt.read("cleaned_markets")
    return (dlt.read_stream("cleaned_transactions_geyser")
           .withWatermark("block_time", "1 hour")
           .select("*", F.posexplode("instructions").alias("instruction_index", "instruction"))
           .filter(F.col("instruction.name").contains("cancel_order"))
           .withColumn("event", F.explode("instruction.events"))
           .filter("event.name == 'cancel_event'")
           .join(markets_df, 
                 (F.col("instruction.accounts.named.market") == markets_df.market_pub_key)
                  & F.col("block_time").between(markets_df.active_timestamp, markets_df.expiry_timestamp),
                 how="left"
                )
           .select("signature",
                   "instruction_index",
                   "underlying",
#                    F.upper("event.event.asset").alias("underlying"),
                   F.col("expiry_timestamp").alias("expiry"),
                   "strike",
                   "kind",
                   "instruction.name",
                   F.col("event.event.user").alias("user_pub_key"),
                   "event.event.market_index",
                   "event.event.side",
                   "event.event.size",
                   "event.event.order_id",
                   "event.event.client_order_id",
                   F.col("instruction.accounts.named").alias("accounts"),
                   "block_time",
                   "slot"
                  )
           .withColumn("date_", F.to_date("block_time"))
           .withColumn("hour_", F.date_format("block_time", "HH").cast("int"))
           )
    
# Liquidate
@dlt.table(
    comment="Cleaned data for liquidate instructions",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_", "underlying"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"cleaned-ix-liquidate")
)
def cleaned_ix_liquidate_geyser():
    markets_df = dlt.read("cleaned_markets")
    return (dlt.read_stream("cleaned_transactions_geyser")
           .withWatermark("block_time", "1 hour")
           .select("*", F.posexplode("instructions").alias("instruction_index", "instruction"))
           .filter("instruction.name == 'liquidate'")
           .withColumn("event", F.col("instruction.events")[0])
           .join(markets_df, (F.col("instruction.accounts.named.market") == markets_df.market_pub_key)
                & F.col("block_time").between(markets_df.active_timestamp, markets_df.expiry_timestamp) )
           .select("signature",
                   "instruction_index",
                   "underlying",
                   F.col("expiry_timestamp").alias("expiry"),
                   "strike",
                   "kind",
                   "instruction.name",
                   (F.col("instruction.args.size") / SIZE_FACTOR).alias("desired_size"),
                   (F.col("event.event.liquidator_reward") / PRICE_FACTOR).alias("liquidator_reward"),
                   (F.col("event.event.insurance_reward") / PRICE_FACTOR).alias("insurance_reward"),
                   (F.col("event.event.cost_of_trades") / PRICE_FACTOR).alias("cost_of_trades"),
                   (F.col("event.event.size") / SIZE_FACTOR).alias("liquidated_size"), # duplicate of the args?
                   (F.col("event.event.remaining_liquidatee_balance") / PRICE_FACTOR).alias("remaining_liquidatee_balance"),
                   (F.col("event.event.remaining_liquidator_balance") / PRICE_FACTOR).alias("remaining_liquidator_balance"),
                   (F.col("event.event.mark_price") / PRICE_FACTOR).alias("mark_price"),
                   (F.col("event.event.underlying_price") / PRICE_FACTOR).alias("underlying_price"),
                   F.col("instruction.accounts.named").alias("accounts"),
                   "block_time",
                   "slot"
                  )
           .withColumn("date_", F.to_date("block_time"))
           .withColumn("hour_", F.date_format("block_time", "HH").cast("int"))
           )

# Trades
@dlt.table(
    comment="Cleaned data for trades",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_", "underlying"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"cleaned-ix-trade")
)
def cleaned_ix_trade_geyser():
    markets_df = dlt.read("cleaned_markets")
    return (dlt.read_stream("cleaned_transactions_geyser")
           .withWatermark("block_time", "1 hour")
           .select("*", F.posexplode("instructions").alias("instruction_index", "instruction"))
           .filter((F.col("instruction.name") == 'crank_event_queue') | F.col("instruction.name").startswith('place_order'))
           .withColumn("event", F.explode("instruction.events"))
           .filter("event.name == 'trade_event'")
           .join(markets_df, 
                 (F.col("instruction.accounts.named.market") == markets_df.market_pub_key)
                  & F.col("block_time").between(markets_df.active_timestamp, markets_df.expiry_timestamp),
                 how="left"
                )
           .select("signature",
                   "instruction_index",
                   "underlying",
                   F.col("expiry_timestamp").alias("expiry"),
                   "strike",
                   "kind",
                   "event.name",
                   "event.event.margin_account",
                   ((F.col("event.event.cost_of_trades") / F.col("event.event.size")) / (PRICE_FACTOR/SIZE_FACTOR)).alias("price"),
                   (F.col("event.event.size") / SIZE_FACTOR).alias("size"),
        #            (F.col("event.event.cost_of_trades") / PRICE_FACTOR).alias("cost_of_trades"),
                   F.when(F.col("event.event.is_bid").cast("boolean"), "bid").otherwise("ask").alias("side"),
                   F.when(F.col("instruction.name") == 'crank_event_queue', "maker").otherwise("taker").alias("maker_taker"),
                   F.col("event.event.index").cast("smallint").alias("market_index"),
                   "event.event.client_order_id",
                   "event.event.order_id",
                   (F.col("event.event.fee") / PRICE_FACTOR).alias("trading_fee"), # not instrumented for maker yet (but is 0 currently)
                   (F.col("event.event.oracle_price") / PRICE_FACTOR).alias("oracle_price"), # not instrumented for maker yet
                   F.col("instruction.accounts.named").alias("accounts"),
                   "block_time",
                   "slot"
                  )
           .withColumn("date_", F.to_date("block_time"))
           .withColumn("hour_", F.date_format("block_time", "HH").cast("int"))
            )

# Position Movement
@dlt.table(
    comment="Cleaned data for position movement instructions (strategy accounts)",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_", "underlying"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"cleaned-ix-position-movement")
)
def cleaned_ix_position_movement_geyser():
    zetagroup_mapping_df = dlt.read("zetagroup_mapping")
    return (dlt.read_stream("cleaned_transactions_geyser")
           .withWatermark("block_time", "1 hour")
           .select("*", F.posexplode("instructions").alias("instruction_index", "instruction"))
           .filter("instruction.name == 'position_movement'")
           .withColumn("event", F.col("instruction.events")[0])
           .join(zetagroup_mapping_df, 
             (F.col("instruction.accounts.named.zeta_group") == zetagroup_mapping_df.zetagroup_pub_key),
             how="left"
            )
           .select("signature",
                   "instruction_index",
                   "underlying",
                   "instruction.name",
                   "instruction.args.movement_type",
                   "instruction.args.movements", # need to parse this https://github.com/zetamarkets/zeta-options/blob/a273907e8d6e4fb44fc2c05c5e149d66e89b08cc/zeta/programs/zeta/src/context.rs#L1280-L1284
                   (F.col("event.event.net_balance_transfer") / PRICE_FACTOR).alias("net_balance_transfer"),
                   (F.col("event.event.margin_account_balance") / PRICE_FACTOR).alias("margin_account_balance"),
                   (F.col("event.event.spread_account_balance") / PRICE_FACTOR).alias("spread_account_balance"),
                   (F.col("event.event.movement_fees") / PRICE_FACTOR).alias("movement_fees"),
                   F.col("instruction.accounts.named").alias("accounts"),
                   "block_time",
                   "slot"
                  )
           .withColumn("date_", F.to_date("block_time"))
           .withColumn("hour_", F.date_format("block_time", "HH").cast("int"))
           )

# Settle Positions
@dlt.table(
    comment="Cleaned data for position settlement",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_", "underlying"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"cleaned-ix-settle-positions")
)
def cleaned_ix_settle_positions_geyser():
    zetagroup_mapping_df = dlt.read("zetagroup_mapping")
    return (dlt.read_stream("cleaned_transactions_geyser")
           .withWatermark("block_time", "1 hour")
           .select("*", F.posexplode("instructions").alias("instruction_index", "instruction"))
           .filter("instruction.name == 'settle_positions'")
           .join(zetagroup_mapping_df, 
             (F.col("instruction.accounts.named.zeta_group") == zetagroup_mapping_df.zetagroup_pub_key),
             how="left"
            )
           .select("signature",
                   "instruction_index",
                   "underlying",
                   F.col("instruction.args.expiry_ts").cast("long").cast("timestamp").alias("expiry_ts"),
                   F.col("instruction.accounts.named").alias("accounts"),
                   "block_time",
                   "slot"
                  )
           .withColumn("date_", F.to_date("block_time"))
           .withColumn("hour_", F.date_format("block_time", "HH").cast("int"))
            )
