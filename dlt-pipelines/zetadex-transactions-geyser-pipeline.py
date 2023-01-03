# Databricks notebook source
dbutils.widgets.dropdown("network", "devnet", ["devnet", "mainnet"], "Network")
# NETWORK = dbutils.widgets.get("network")
NETWORK = spark.conf.get("pipeline.network")

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from os.path import join
from itertools import chain
import dlt

# COMMAND ----------

from datetime import datetime, timezone
current_date = str(datetime.now(timezone.utc).date())
current_hour = datetime.now(timezone.utc).hour

# COMMAND ----------

PRICE_FACTOR = 1e6
SIZE_FACTOR = 1e3

TRANSACTIONS_TABLE = "transactions-geyser"
SLOTS_TABLE = "slots-geyser"

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
# MAGIC ## Slots

# COMMAND ----------

slots_schema = """
slot bigint,
status string,
type string,
year string,
month string,
day string,
hour string
"""

@dlt.table(
    comment="Raw data for geyser slots",
    table_properties={
        "quality":"bronze", 
    },
    path=join(BASE_PATH_TRANSFORMED,SLOTS_TABLE,"raw"),
    schema=slots_schema
)
def raw_slots_geyser():
    return (spark
           .readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "json")
           .option("cloudFiles.region", "ap-southeast-1")
           .option("cloudFiles.includeExistingFiles", True)
           .option("cloudFiles.useNotifications", True)
           .option("partitionColumns", "year,month,day,hour")
           .schema(slots_schema)
           .load(join(BASE_PATH_LANDED,SLOTS_TABLE,"data"))
          )

# COMMAND ----------

@dlt.table(
    comment="Cleaned data for finalized geyser slots",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"slot"
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED,SLOTS_TABLE,"cleaned")
)
def cleaned_slots_geyser():
    return (dlt.read_stream("raw_slots_geyser")
           .withColumn("indexed_timestamp", F.to_timestamp(F.concat(F.col("year"), F.lit("-"), F.col("month"), F.lit("-"), F.col("day"), F.lit(" "), F.col("hour")), "yyyy-MM-dd HH"))
           .withWatermark("indexed_timestamp", "1 hour")
           .filter("status == 'finalized'")
           .select("slot", "indexed_timestamp")
           .dropDuplicates(["slot"])
           .withColumn("date_", F.to_date("indexed_timestamp"))
           .withColumn("hour_", F.date_format("indexed_timestamp", "HH").cast("int"))
          )

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
           .dropDuplicates(["signature"])
          )

# COMMAND ----------

# DBTITLE 1,Silver
@F.udf(returnType=
     T.StructType([
          T.StructField("name", T.StringType(), False), 
          T.StructField("event", T.MapType(T.StringType(), T.StringType()), False)
         ])
    )
def place_trade_event_merge(arr):
    p = None
    t = None
    for x in arr:
        if x.name == "place_order_event":
            p = x
        elif x.name.startswith("trade_event"):
            t = x
    if t is not None:
        return ('place_order_trade_event', {**p.event, **t.event})
    else:
        return p

@dlt.view
def zetagroup_mapping_v():
    return spark.table(f"zetadex_{NETWORK}.zetagroup_mapping")

@dlt.view
def cleaned_markets_v():
    return spark.table(f"zetadex_{NETWORK}.cleaned_markets")

# Transactions
@dlt.table(
    comment="Cleaned data for platform transactions",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"block_time"
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"cleaned-transactions")
)
def cleaned_transactions_geyser():
    return (dlt.read_stream("raw_transactions_geyser")
           .withWatermark("block_time", "1 hour")
        #    .dropDuplicates(["signature"])
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
        "pipelines.autoOptimize.zOrderCols":"block_time"
    },
    partition_cols=["date_", "underlying"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"cleaned-ix-deposit")
)
def cleaned_ix_deposit_geyser():
    zetagroup_mapping_df = dlt.read("zetagroup_mapping_v")
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
        "pipelines.autoOptimize.zOrderCols":"block_time"
    },
    partition_cols=["date_", "underlying"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"cleaned-ix-withdraw")
)
def cleaned_ix_withdraw_geyser():
    zetagroup_mapping_df = dlt.read("zetagroup_mapping_v")
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
        "pipelines.autoOptimize.zOrderCols":"block_time"
    },
    partition_cols=["date_", "underlying", "expiry", "strike", "kind"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"cleaned-ix-place-order")
)
def cleaned_ix_place_order_geyser():
    markets_df = dlt.read("cleaned_markets_v")
    return (dlt.read_stream("cleaned_transactions_geyser")
           .withWatermark("block_time", "1 hour")
           .select("*", F.posexplode("instructions").alias("instruction_index", "instruction"))
           .filter(F.col("instruction.name").rlike("^place_(perp_)?order(_v[0-9]+)?$")) # place_order and place_perp_order variants
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
    comment="Cleaned data for order completion, this includes CancelOrder variants as well as trade fill events",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"block_time"
    },
    partition_cols=["date_", "underlying", "expiry", "strike", "kind"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"cleaned-ix-order-complete")
)
def cleaned_ix_order_complete_geyser():
    markets_df = dlt.read("cleaned_markets_v")
    return (dlt.read_stream("cleaned_transactions_geyser")
           .withWatermark("block_time", "1 hour")
           .select("*", F.posexplode("instructions").alias("instruction_index", "instruction"))
           .filter((F.col("instruction.name") == 'crank_event_queue') # maker fill
                   | F.col("instruction.name").rlike("^place_(perp_)?order(_v[0-9]+)?$") # taker fill
                   | F.col("instruction.name").contains('cancel') # cancel
                  )
           .withColumn("event", F.explode("instruction.events"))
           .filter("event.name == 'order_complete_event'")
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
                   "event.event.order_complete_type",
                   F.col("event.event.user").alias("user_pub_key"),
                   "event.event.market_index",
                   "event.event.side",
                   (F.col("event.event.unfilled_size") / SIZE_FACTOR).alias("unfilled_size"),
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
        "pipelines.autoOptimize.zOrderCols":"block_time"
    },
    partition_cols=["date_", "underlying"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"cleaned-ix-liquidate")
)
def cleaned_ix_liquidate_geyser():
    markets_df = dlt.read("cleaned_markets_v")
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

# MAKER
# ix.name == 'crank_event_queue'
# ix.events == 'trade_event'

# TAKER
# ix.name == 'place_order*'
# ix.events == 'place_order_event' | 'trade_event(_v2)'
# [0] place_order_event, [1] trade_event
    
# Trades
@dlt.table(
    comment="Cleaned data for trades",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"block_time"
    },
    partition_cols=["date_", "underlying"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"cleaned-ix-trade")
)
def cleaned_ix_trade_geyser():
    markets_df = dlt.read("cleaned_markets_v")
    df = (dlt.read_stream("cleaned_transactions_geyser")
           .withWatermark("block_time", "1 hour")
           .select("*", F.posexplode("instructions").alias("instruction_index", "instruction"))
         )
    maker_df = (df
        .filter("instruction.name == 'crank_event_queue'")
        .withColumn("event", F.explode("instruction.events"))
        .filter(F.col("event.name").startswith('trade_event'))
        .withColumn("maker_taker", F.lit("maker"))
              )
    taker_df = (df
    .filter(F.col("instruction.name").rlike("^place_(perp_)?order(_v[0-9]+)?$"))
    .filter((F.array_contains("instruction.events.name", F.lit('trade_event'))) | (F.array_contains("instruction.events.name", F.lit('trade_event_v2')))) # filter to only taker orders that trade
    .withColumn("event", place_trade_event_merge("instruction.events"))
    .withColumn("maker_taker", F.lit("taker"))
           )
    # Union all maker and taker
    return (maker_df
            .union(taker_df)
            .join(markets_df, 
                 (F.col("instruction.accounts.named.market") == markets_df.market_pub_key)
                  & F.col("block_time").between(markets_df.active_timestamp, markets_df.expiry_timestamp),
                 how="left"
                )
            .select("signature",
                   "instruction_index",
                   F.coalesce("event.event.asset", "underlying").alias("underlying"),
                   F.col("expiry_timestamp").alias("expiry"),
                   "strike",
                   "kind",
                   "event.name",
                   "event.event.user",
                   "event.event.margin_account",
                   ((F.col("event.event.cost_of_trades") / F.col("event.event.size")) / (PRICE_FACTOR/SIZE_FACTOR)).alias("price"),
                   (F.col("event.event.size") / SIZE_FACTOR).alias("size"),
                   F.when(F.col("event.event.is_bid").cast("boolean"), "bid").otherwise("ask").alias("side"),
                   F.when(F.col("instruction.name").rlike("^place_(perp_)?order(_v[0-9]+)?$"), "taker").otherwise("maker")
                    # F.when(F.coalesce(F.col("event.event.isTaker"), F.lit("False")).cast("boolean"), "taker")
                    #                 .otherwise("maker")
                    .alias("maker_taker"),
                   F.col("event.event.index").cast("smallint").alias("market_index"),
                   "event.event.client_order_id",
                   "event.event.order_id",
                   "event.event.sequenceNumber",
                   "instruction.args.order_type",
                   "instruction.args.tag",
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
        "pipelines.autoOptimize.zOrderCols":"block_time"
    },
    partition_cols=["date_", "underlying"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"cleaned-ix-position-movement")
)
def cleaned_ix_position_movement_geyser():
    zetagroup_mapping_df = dlt.read("zetagroup_mapping_v")
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
        "pipelines.autoOptimize.zOrderCols":"block_time"
    },
    partition_cols=["date_", "underlying"],
    path=join(BASE_PATH_TRANSFORMED,TRANSACTIONS_TABLE,"cleaned-ix-settle-positions")
)
def cleaned_ix_settle_positions_geyser():
    zetagroup_mapping_df = dlt.read("zetagroup_mapping_v")
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

# COMMAND ----------

# DBTITLE 1,Gold
@dlt.table(
    comment='Hourly aggregated data for funding rate',
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "hour", # change
    },
    partition_cols=["asset"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "agg-funding-rate-1h")
)
def agg_funding_rate_1h():
    return (
        dlt.read_stream("cleaned_transactions_geyser")
        .withWatermark("block_time","1 hour")
        .withColumn("instruction", F.explode("instructions"))
        .withColumn("event", F.explode("instruction.events"))
        .filter("event.name == 'apply_funding_event'")
        .groupBy(
            F.col("event.event.user").alias("pubkey"),
            "event.event.margin_account",
            F.date_trunc("hour", "block_time").alias("hour"), # change to timestamp later
            "event.event.asset"
        )
        .agg(
            (F.sum(F.col("event.event.balance_change") / PRICE_FACTOR)).alias("balance_change")
        )
        .filter("balance_change <> 0")
    )

