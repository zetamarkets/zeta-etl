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
MARKETS_TABLE = "markets"
SLOTS_TABLE = "slots-geyser"
PNL_TABLE = "pnl-geyser"

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
        "quality": "bronze",
    },
    path=join(BASE_PATH_TRANSFORMED, MARKETS_TABLE, "raw"),
    schema=markets_schema,
)
def raw_markets():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.region", "ap-southeast-1")
        .option("cloudFiles.includeExistingFiles", True)
        .option("cloudFiles.useNotifications", True)
        .option("partitionColumns", "year,month,day,hour")
        .schema(markets_schema)
        .load(join(BASE_PATH_LANDED, MARKETS_TABLE, "data"))
    )

# COMMAND ----------

@dlt.table(
    comment="Cleaned metadata for Zeta's Serum markets",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "timestamp",
    },
    partition_cols=["underlying"],
    path=join(BASE_PATH_TRANSFORMED, MARKETS_TABLE, "cleaned"),
)
def cleaned_markets():
    return (
        dlt.read_stream("raw_markets")
        .withWatermark("timestamp", "1 hour")
        .dropDuplicates(
            [
                "underlying",
                "active_timestamp",
                "expiry_timestamp",
                "strike",
                "kind",
                "market_pub_key",
            ]
        )
        .drop("timestamp", "slot")
    )

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
        "quality": "bronze",
    },
    path=join(BASE_PATH_TRANSFORMED, SLOTS_TABLE, "raw"),
    schema=slots_schema,
)
def raw_slots_geyser():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.region", "ap-southeast-1")
        .option("cloudFiles.includeExistingFiles", True)
        .option("cloudFiles.useNotifications", True)
        .option("partitionColumns", "year,month,day,hour")
        .schema(slots_schema)
        .load(join(BASE_PATH_LANDED, SLOTS_TABLE, "data"))
    )

# COMMAND ----------

@dlt.table(
    comment="Cleaned data for finalized geyser slots",
    table_properties={"quality": "silver", "pipelines.autoOptimize.zOrderCols": "slot"},
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, SLOTS_TABLE, "cleaned"),
)
def cleaned_slots_geyser():
    return (
        dlt.read_stream("raw_slots_geyser")
        .withColumn(
            "indexed_timestamp",
            F.to_timestamp(
                F.concat(
                    F.col("year"),
                    F.lit("-"),
                    F.col("month"),
                    F.lit("-"),
                    F.col("day"),
                    F.lit(" "),
                    F.col("hour"),
                ),
                "yyyy-MM-dd HH",
            ),
        )
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
        program_id string,
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
        "quality": "bronze",
    },
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "raw"),
    schema=transactions_schema,
)
def raw_transactions_geyser():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.region", "ap-southeast-1")
        .option("cloudFiles.includeExistingFiles", True)
        .option("cloudFiles.useNotifications", True)
        .option("partitionColumns", "year,month,day,hour")
        .schema(transactions_schema)
        .load(join(BASE_PATH_LANDED, TRANSACTIONS_TABLE, "data"))
#         .dropDuplicates(["signature"])
    )

# COMMAND ----------

# DBTITLE 1,Silver
@F.udf(
    returnType=T.StructType(
        [
            T.StructField("name", T.StringType(), False),
            T.StructField("event", T.MapType(T.StringType(), T.StringType()), False),
        ]
    )
)
def place_trade_event_merge(arr):
    p = None
    t = None
    for x in arr:
        if x.name == "place_order_event":
            p = x
        elif x.name.startswith("trade_event"):
            t = x
    if t is not None and p is not None:
        return ("place_order_trade_event", {**p.event, **t.event})
    else:
        return p


@dlt.view
def zetagroup_mapping_v():
    return spark.table(f"zetadex_{NETWORK}.zetagroup_mapping")


@dlt.view
def cleaned_markets_v():
    # return spark.table(f"zetadex_{NETWORK}.cleaned_markets")
    return dlt.read("cleaned_markets")


# Transactions
@dlt.table(
    comment="Cleaned data for platform transactions",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "block_time",
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "cleaned-transactions"),
)
def cleaned_transactions_geyser():
    return (
        dlt.read_stream("raw_transactions_geyser")
        .withWatermark("block_time", "1 hour")
        .filter("is_successful")
        .dropDuplicates(["signature"])
        .drop("year", "month", "day", "hour")
        .withColumn("date_", F.to_date("block_time"))
        .withColumn("hour_", F.date_format("block_time", "HH").cast("int"))
    )


# Deposits
@dlt.table(
    comment="Cleaned data for deposit instructions",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "block_time",
    },
    partition_cols=["date_", "underlying"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "cleaned-ix-deposit"),
)
def cleaned_ix_deposit_geyser():
    zetagroup_mapping_df = dlt.read("zetagroup_mapping_v")
    return (
        dlt.read_stream("cleaned_transactions_geyser")
        .withWatermark("block_time", "1 hour")
        .select(
            "*", F.posexplode("instructions").alias("instruction_index", "instruction")
        )
        .filter(f"instruction.name == 'deposit'")
        .join(
            zetagroup_mapping_df,
            (
                F.col("instruction.accounts.named.zeta_group")
                == zetagroup_mapping_df.zetagroup_pub_key
            ),
            how="left",
        )
        .select(
            "signature",
            "instruction_index",
            "instruction.name",
            (F.col("instruction.args.amount") / PRICE_FACTOR).alias("deposit_amount"),
            F.col("instruction.accounts.named").alias("accounts"),
            "underlying",
            "block_time",
            "slot",
        )
        .withColumn("date_", F.to_date("block_time"))
        .withColumn("hour_", F.date_format("block_time", "HH").cast("int"))
    )


# Withdraw
@dlt.table(
    comment="Cleaned data for withdraw instructions",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "block_time",
    },
    partition_cols=["date_", "underlying"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "cleaned-ix-withdraw"),
)
def cleaned_ix_withdraw_geyser():
    zetagroup_mapping_df = dlt.read("zetagroup_mapping_v")
    return (
        dlt.read_stream("cleaned_transactions_geyser")
        .withWatermark("block_time", "1 hour")
        .select(
            "*", F.posexplode("instructions").alias("instruction_index", "instruction")
        )
        .filter(f"instruction.name == 'withdraw'")
        .join(
            zetagroup_mapping_df,
            (
                F.col("instruction.accounts.named.zeta_group")
                == zetagroup_mapping_df.zetagroup_pub_key
            ),
            how="left",
        )
        .select(
            "signature",
            "instruction_index",
            "instruction.name",
            (F.col("instruction.args.amount") / PRICE_FACTOR).alias("withdraw_amount"),
            F.col("instruction.accounts.named").alias("accounts"),
            "underlying",
            "block_time",
            "slot",
        )
        .withColumn("date_", F.to_date("block_time"))
        .withColumn("hour_", F.date_format("block_time", "HH").cast("int"))
    )


# Place order
@dlt.table(
    comment="Cleaned data for placeOrder type instructions",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "block_time",
    },
    partition_cols=["date_", "underlying", "expiry", "strike", "kind"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "cleaned-ix-place-order"),
)
def cleaned_ix_place_order_geyser():
    markets_df = dlt.read("cleaned_markets_v")
    return (
        dlt.read_stream("cleaned_transactions_geyser")
        .withWatermark("block_time", "1 hour")
        .select(
            "*", F.posexplode("instructions").alias("instruction_index", "instruction")
        )
        .filter(
            F.col("instruction.name").rlike("^place_(perp_)?order(_v[0-9]+)?$")
        )  # place_order and place_perp_order variants
        .withColumn("event", F.explode("instruction.events"))
        .filter("event.name == 'place_order_event'")
        .join(
            markets_df,
            (F.col("instruction.accounts.named.market") == markets_df.market_pub_key)
            & F.col("block_time").between(
                markets_df.active_timestamp, markets_df.expiry_timestamp
            ),
            how="left",
        )
        .select(
            "signature",
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
            "instruction.args.tif_offset",
            (F.col("event.event.fee") / PRICE_FACTOR).alias("trading_fee"),
            (F.col("event.event.oracle_price") / PRICE_FACTOR).alias("oracle_price"),
            "event.event.order_id",
            F.col("instruction.accounts.named").alias("accounts"),
            "block_time",
            "slot",
        )
        .withColumn("date_", F.to_date("block_time"))
        .withColumn("hour_", F.date_format("block_time", "HH").cast("int"))
    )


# Cancel order
@dlt.table(
    comment="Cleaned data for order completion, this includes CancelOrder variants as well as trade fill events",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "block_time",
    },
    partition_cols=["date_", "underlying", "expiry", "strike", "kind"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "cleaned-ix-order-complete"),
)
def cleaned_ix_order_complete_geyser():
    markets_df = dlt.read("cleaned_markets_v")
    return (
        dlt.read_stream("cleaned_transactions_geyser")
        .withWatermark("block_time", "1 hour")
        .select(
            "*", F.posexplode("instructions").alias("instruction_index", "instruction")
        )
        .filter(
            (F.col("instruction.name") == "crank_event_queue")  # maker fill
            | F.col("instruction.name").rlike(
                "^place_(perp_)?order(_v[0-9]+)?$"
            )  # taker fill
            | F.col("instruction.name").contains("cancel")  # cancel
        )
        .withColumn("event", F.explode("instruction.events"))
        .filter("event.name == 'order_complete_event'")
        .join(
            markets_df,
            (F.col("instruction.accounts.named.market") == markets_df.market_pub_key)
            & F.col("block_time").between(
                markets_df.active_timestamp, markets_df.expiry_timestamp
            ),
            how="left",
        )
        .select(
            "signature",
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
            "slot",
        )
        .withColumn("date_", F.to_date("block_time"))
        .withColumn("hour_", F.date_format("block_time", "HH").cast("int"))
    )


# Liquidate
@dlt.table(
    comment="Cleaned data for liquidate instructions",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "block_time",
    },
    partition_cols=["date_", "underlying"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "cleaned-ix-liquidate"),
)
def cleaned_ix_liquidate_geyser():
    markets_df = dlt.read("cleaned_markets_v")
    return (
        dlt.read_stream("cleaned_transactions_geyser")
        .withWatermark("block_time", "1 hour")
        .select(
            "*", F.posexplode("instructions").alias("instruction_index", "instruction")
        )
        .filter("instruction.name == 'liquidate'")
        .withColumn("event", F.col("instruction.events")[0])
        .join(
            markets_df,
            (F.col("instruction.accounts.named.market") == markets_df.market_pub_key)
            & F.col("block_time").between(
                markets_df.active_timestamp, markets_df.expiry_timestamp
            ),
        )
        .select(
            "signature",
            "instruction_index",
            "underlying",
            F.col("expiry_timestamp").alias("expiry"),
            "strike",
            "kind",
            "instruction.name",
            (F.col("instruction.args.size") / SIZE_FACTOR).alias("desired_size"),
            (F.col("event.event.liquidator_reward") / PRICE_FACTOR).alias(
                "liquidator_reward"
            ),
            (F.col("event.event.insurance_reward") / PRICE_FACTOR).alias(
                "insurance_reward"
            ),
            (F.col("event.event.cost_of_trades") / PRICE_FACTOR).alias(
                "cost_of_trades"
            ),
            (F.col("event.event.size") / SIZE_FACTOR).alias(
                "liquidated_size"
            ),
            (F.col("event.event.remaining_liquidatee_balance") / PRICE_FACTOR).alias(
                "remaining_liquidatee_balance"
            ),
            (F.col("event.event.remaining_liquidator_balance") / PRICE_FACTOR).alias(
                "remaining_liquidator_balance"
            ),
            (F.col("event.event.mark_price") / PRICE_FACTOR).alias("mark_price"),
            (F.col("event.event.underlying_price") / PRICE_FACTOR).alias(
                "underlying_price"
            ),
            F.col("instruction.accounts.named").alias("accounts"),
            "block_time",
            "slot",
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
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "block_time",
    },
    partition_cols=["date_", "underlying"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "cleaned-ix-trade"),
)
def cleaned_ix_trade_geyser():
    markets_df = dlt.read("cleaned_markets_v")
    df = (
        dlt.read_stream("cleaned_transactions_geyser")
        .withWatermark("block_time", "1 hour")
        .select(
            "*", F.posexplode("instructions").alias("instruction_index", "instruction")
        )
    )
    maker_df = (
        df.filter("instruction.name == 'crank_event_queue'")
        .withColumn("event", F.explode("instruction.events"))
        .filter(F.col("event.name").startswith("trade_event"))
        .withColumn("maker_taker", F.lit("maker"))
    )
    taker_df = (
        df.filter(F.col("instruction.name").rlike("^place_(perp_)?order(_v[0-9]+)?$"))
        .filter(
            (F.array_contains("instruction.events.name", F.lit("trade_event")))
            | (F.array_contains("instruction.events.name", F.lit("trade_event_v2")))
        )  # filter to only taker orders that trade
        .withColumn("event", place_trade_event_merge("instruction.events"))
        .withColumn("maker_taker", F.lit("taker"))
    )
    # Union all maker and taker
    return (
        maker_df.union(taker_df)
        .join(
            markets_df,
            (F.col("instruction.accounts.named.market") == markets_df.market_pub_key)
            & F.col("block_time").between(
                markets_df.active_timestamp, markets_df.expiry_timestamp
            ),
            how="left",
        )
        .select(
            "signature",
            "instruction_index",
            F.coalesce("underlying", "event.event.asset").alias("underlying"),
            F.col("expiry_timestamp").alias("expiry"),
            "strike",
            "kind",
            "event.name",
            "event.event.user",
            "event.event.margin_account",
            (
                (F.col("event.event.cost_of_trades") / F.col("event.event.size"))
                / (PRICE_FACTOR / SIZE_FACTOR)
            ).alias("price"),
            (F.col("event.event.size") / SIZE_FACTOR).alias("size"),
            F.when(F.col("event.event.is_bid").cast("boolean"), "bid")
            .otherwise("ask")
            .alias("side"),
            F.when(
                F.col("instruction.name").rlike("^place_(perp_)?order(_v[0-9]+)?$"),
                "taker",
            ).otherwise("maker")
            .alias("maker_taker"),
            F.col("event.event.index").cast("smallint").alias("market_index"),
            "event.event.client_order_id",
            "event.event.order_id",
            "event.event.sequenceNumber",
            "instruction.args.order_type",
            "instruction.args.tag",
            (F.col("event.event.fee") / PRICE_FACTOR).alias(
                "trading_fee"
            ),  # not instrumented for maker yet (but is 0 currently)
            (F.col("event.event.oracle_price") / PRICE_FACTOR).alias(
                "oracle_price"
            ),  # not instrumented for maker yet
            F.col("instruction.accounts.named").alias("accounts"),
            "block_time",
            "slot",
        )
        .withColumn("date_", F.to_date("block_time"))
        .withColumn("hour_", F.date_format("block_time", "HH").cast("int"))
    )


# Position Movement
@dlt.table(
    comment="Cleaned data for position movement instructions (strategy accounts)",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "block_time",
    },
    partition_cols=["date_", "underlying"],
    path=join(
        BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "cleaned-ix-position-movement"
    ),
)
def cleaned_ix_position_movement_geyser():
    zetagroup_mapping_df = dlt.read("zetagroup_mapping_v")
    return (
        dlt.read_stream("cleaned_transactions_geyser")
        .withWatermark("block_time", "1 hour")
        .select(
            "*", F.posexplode("instructions").alias("instruction_index", "instruction")
        )
        .filter("instruction.name == 'position_movement'")
        .withColumn("event", F.col("instruction.events")[0])
        .join(
            zetagroup_mapping_df,
            (
                F.col("instruction.accounts.named.zeta_group")
                == zetagroup_mapping_df.zetagroup_pub_key
            ),
            how="left",
        )
        .select(
            "signature",
            "instruction_index",
            "underlying",
            "instruction.name",
            "instruction.args.movement_type",
            "instruction.args.movements",  # need to parse this https://github.com/zetamarkets/zeta-options/blob/a273907e8d6e4fb44fc2c05c5e149d66e89b08cc/zeta/programs/zeta/src/context.rs#L1280-L1284
            (F.col("event.event.net_balance_transfer") / PRICE_FACTOR).alias(
                "net_balance_transfer"
            ),
            (F.col("event.event.margin_account_balance") / PRICE_FACTOR).alias(
                "margin_account_balance"
            ),
            (F.col("event.event.spread_account_balance") / PRICE_FACTOR).alias(
                "spread_account_balance"
            ),
            (F.col("event.event.movement_fees") / PRICE_FACTOR).alias("movement_fees"),
            F.col("instruction.accounts.named").alias("accounts"),
            "block_time",
            "slot",
        )
        .withColumn("date_", F.to_date("block_time"))
        .withColumn("hour_", F.date_format("block_time", "HH").cast("int"))
    )


# Settle Positions
@dlt.table(
    comment="Cleaned data for position settlement",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "block_time",
    },
    partition_cols=["date_", "underlying"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "cleaned-ix-settle-positions"),
)
def cleaned_ix_settle_positions_geyser():
    zetagroup_mapping_df = dlt.read("zetagroup_mapping_v")
    return (
        dlt.read_stream("cleaned_transactions_geyser")
        .withWatermark("block_time", "1 hour")
        .select(
            "*", F.posexplode("instructions").alias("instruction_index", "instruction")
        )
        .filter("instruction.name == 'settle_positions'")
        .join(
            zetagroup_mapping_df,
            (
                F.col("instruction.accounts.named.zeta_group")
                == zetagroup_mapping_df.zetagroup_pub_key
            ),
            how="left",
        )
        .select(
            "signature",
            "instruction_index",
            "underlying",
            F.col("instruction.args.expiry_ts")
            .cast("long")
            .cast("timestamp")
            .alias("expiry_ts"),
            F.col("instruction.accounts.named").alias("accounts"),
            "block_time",
            "slot",
        )
        .withColumn("date_", F.to_date("block_time"))
        .withColumn("hour_", F.date_format("block_time", "HH").cast("int"))
    )

# COMMAND ----------

# DBTITLE 1,Gold
@dlt.table(
    comment="Hourly aggregated data for funding rate",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "hour",  # change
    },
    partition_cols=["asset"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "agg-funding-rate-1h"),
)
def agg_funding_rate_1h():
    return (
        dlt.read_stream("cleaned_transactions_geyser")
        .withWatermark("block_time", "1 hour")
        .withColumn("instruction", F.explode("instructions"))
        .withColumn("event", F.explode("instruction.events"))
        .filter("event.name == 'apply_funding_event'")
        .groupBy(
            F.col("event.event.user").alias("pubkey"),
            "event.event.margin_account",
            F.date_trunc("hour", "block_time").alias(
                "hour"
            ),  # change to timestamp later
            "event.event.asset",
        )
        .agg(
            (F.sum(F.col("event.event.balance_change") / PRICE_FACTOR)).alias(
                "balance_change"
            )
        )
        .filter("balance_change <> 0")
    )

# COMMAND ----------

@dlt.table(
    comment="User-underlying-hourly aggregated data for deposits",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "timestamp_window",
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "agg-ix-deposit-u1h"),
)
def agg_ix_deposit_u1h_geyser():
    return (
        dlt.read_stream("cleaned_ix_deposit_geyser")
        .withWatermark("block_time", "1 hour")
        .groupBy(
            F.window("block_time", "1 hour").alias("timestamp_window"),
            F.col("accounts.authority").alias("owner_pub_key"),
            "underlying",
        )
        .agg(
            F.count("*").alias("deposit_count"),
            F.sum("deposit_amount").alias("deposit_amount"),
        )
        .withColumn(
            "date_",
            F.to_date(F.col("timestamp_window.end") - F.expr("INTERVAL 1 HOUR")),
        )
        .withColumn(
            "hour_",
            F.date_format(
                F.col("timestamp_window.end") - F.expr("INTERVAL 1 HOUR"), "HH"
            ).cast("int"),
        )
    )


@dlt.table(
    comment="User-underlying-hourly aggregated data for withdrawals",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "timestamp_window",
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "agg-ix-withdraw-u1h"),
)
def agg_ix_withdraw_u1h_geyser():
    return (
        dlt.read_stream("cleaned_ix_withdraw_geyser")
        .withWatermark("block_time", "1 hour")
        .groupBy(
            F.window("block_time", "1 hour").alias("timestamp_window"),
            F.col("accounts.authority").alias("owner_pub_key"),
            "underlying",
        )
        .agg(
            F.count("*").alias("withdraw_count"),
            F.sum("withdraw_amount").alias("withdraw_amount"),
        )
        .withColumn(
            "date_",
            F.to_date(F.col("timestamp_window.end") - F.expr("INTERVAL 1 HOUR")),
        )
        .withColumn(
            "hour_",
            F.date_format(
                F.col("timestamp_window.end") - F.expr("INTERVAL 1 HOUR"), "HH"
            ).cast("int"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## PnL

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
        "quality": "bronze",
    },
    path=join(BASE_PATH_TRANSFORMED, "pnl", "raw"),
    schema=pnl_schema,
)
def raw_pnl():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.region", "ap-southeast-1")
        .option("cloudFiles.includeExistingFiles", True)
        .option("cloudFiles.useNotifications", True)
        .option("partitionColumns", "year,month,day,hour")
        .option("mergeSchema", "true")
        .schema(pnl_schema)
        .load(join(BASE_PATH_LANDED, "margin-accounts-pnl", "data"))
    )

# COMMAND ----------

@dlt.table(
    comment="Cleaned data for margin account profit and loss",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "owner_pub_key",
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, PNL_TABLE, "cleaned"),
)
def cleaned_pnl_geyser():
    deposits_df = (
        dlt.read("agg_ix_deposit_u1h_geyser")
        #                  .withColumn("date_hour", F.date_trunc("hour", F.col("timestamp_window.start")))
        #                  .withWatermark("date_hour", "1 hour")
        .drop("date_", "hour_")
    )
    withdrawals_df = (
        dlt.read("agg_ix_withdraw_u1h_geyser")
        #                  .withColumn("date_hour", F.date_trunc("hour", F.col("timestamp_window.start")))
        #                  .withWatermark("date_hour", "1 hour")
        .drop("date_", "hour_")
    )
    return (
        dlt.read("raw_pnl")  # ideally would be read_stream (TODO)
        .withWatermark("timestamp", "1 hour")
        .dropDuplicates(["owner_pub_key", "underlying", "year", "month", "day", "hour"])
        .join(
            deposits_df.alias("d"),
            F.expr(
                """
               raw_pnl.underlying = d.underlying AND
               raw_pnl.owner_pub_key = d.owner_pub_key AND
               raw_pnl.timestamp >= d.timestamp_window.start + interval '1' hour AND raw_pnl.timestamp < d.timestamp_window.end + interval '1' hour
               """
            ),
            how="left",
        )
        .join(
            withdrawals_df.alias("w"),
            F.expr(
                """
               raw_pnl.underlying = w.underlying AND
               raw_pnl.owner_pub_key = w.owner_pub_key AND
               raw_pnl.timestamp >= w.timestamp_window.start + interval '1' hour AND raw_pnl.timestamp < w.timestamp_window.end + interval '1' hour
               """
            ),
            how="left",
        )
        .withColumnRenamed("timestamp", "indexed_timestamp")
        .withColumn("timestamp", F.date_trunc("hour", "indexed_timestamp"))
        .withColumn("deposit_amount", F.coalesce("deposit_amount", F.lit(0)))
        .withColumn("withdraw_amount", F.coalesce("withdraw_amount", F.lit(0)))
        .withColumn(
            "deposit_amount_cumsum",
            F.sum("deposit_amount").over(
                Window.partitionBy("raw_pnl.underlying", "raw_pnl.owner_pub_key")
                .orderBy("timestamp")
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
        .withColumn(
            "withdraw_amount_cumsum",
            F.sum("withdraw_amount").over(
                Window.partitionBy("raw_pnl.underlying", "raw_pnl.owner_pub_key")
                .orderBy("timestamp")
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
        .withColumn(
            "pnl",
            F.col("balance")
            + F.col("unrealized_pnl")
            - (F.col("deposit_amount_cumsum") - F.col("withdraw_amount_cumsum")),
        )
        .withColumn("date_", F.to_date("timestamp"))
        .withColumn("hour_", F.date_format("timestamp", "HH").cast("int"))
        .select(
            "timestamp",
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
            "hour_",
        )
    )

# COMMAND ----------

days = lambda i: i * 86400

windowSpec24h = (
    Window.partitionBy("owner_pub_key")
    .orderBy(F.unix_timestamp("timestamp"))
    .rangeBetween(-days(1), 0)
)
windowSpec7d = (
    Window.partitionBy("owner_pub_key")
    .orderBy(F.unix_timestamp("timestamp"))
    .rangeBetween(-days(7), 0)
)
windowSpec30d = (
    Window.partitionBy("owner_pub_key")
    .orderBy(F.unix_timestamp("timestamp"))
    .rangeBetween(-days(30), 0)
)

windowSpecRatioRank24h = Window.partitionBy("date_", "hour_").orderBy(
    F.desc("pnl_ratio_24h"), F.desc("pnl_diff_24h"), "owner_pub_key"
)
windowSpecRatioRank7d = Window.partitionBy("date_", "hour_").orderBy(
    F.desc("pnl_ratio_7d"), F.desc("pnl_diff_7d"), "owner_pub_key"
)
windowSpecRatioRank30d = Window.partitionBy("date_", "hour_").orderBy(
    F.desc("pnl_ratio_30d"), F.desc("pnl_diff_30d"), "owner_pub_key"
)
windowSpecDiffRank24h = Window.partitionBy("date_", "hour_").orderBy(
    F.desc("pnl_diff_24h"), F.desc("pnl_ratio_24h"), "owner_pub_key"
)
windowSpecDiffRank7d = Window.partitionBy("date_", "hour_").orderBy(
    F.desc("pnl_diff_7d"), F.desc("pnl_ratio_7d"), "owner_pub_key"
)
windowSpecDiffRank30d = Window.partitionBy("date_", "hour_").orderBy(
    F.desc("pnl_diff_30d"), F.desc("pnl_ratio_30d"), "owner_pub_key"
)

# Break ties in PnL ranking by pubkey alphabetically
@dlt.table(
    comment="User (24h, 7d, 30h) aggregated data for margin account profit and loss",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "owner_pub_key",
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, PNL_TABLE, "agg"),
)
def agg_pnl_geyser():
    return (
        dlt.read("cleaned_pnl_geyser")
        .withWatermark("timestamp", "1 hour")
        .groupBy("timestamp", "date_", "hour_", "owner_pub_key")
        .agg(
            F.sum("pnl").alias("pnl"),
            F.sum("balance").alias("balance"),
            # (F.sum("deposit_amount_cumsum") - F.sum("withdraw_amount_cumsum")).alias("net_inflow"),
            # F.sum("deposit_amount_cumsum").alias("deposit_amount_cumsum"),
            # F.sum("withdraw_amount_cumsum").alias("withdraw_amount_cumsum"),
        )
        .withColumn("pnl_lag_24h", F.first("pnl").over(windowSpec24h))
        .withColumn("pnl_lag_7d", F.first("pnl").over(windowSpec7d))
        .withColumn("pnl_lag_30d", F.first("pnl").over(windowSpec30d))
        .withColumn("balance_lag_24h", F.first("balance").over(windowSpec24h))
        .withColumn("balance_lag_7d", F.first("balance").over(windowSpec7d))
        .withColumn("balance_lag_30d", F.first("balance").over(windowSpec30d))
        .withColumn("pnl_diff_24h", F.col("pnl") - F.col("pnl_lag_24h"))
        .withColumn("pnl_diff_7d", F.col("pnl") - F.col("pnl_lag_7d"))
        .withColumn("pnl_diff_30d", F.col("pnl") - F.col("pnl_lag_30d"))
        .withColumn("pnl_ratio_24h", F.col("pnl_diff_24h") / F.col("balance_lag_24h"))
        .withColumn("pnl_ratio_7d", F.col("pnl_diff_7d") / F.col("balance_lag_7d"))
        .withColumn("pnl_ratio_30d", F.col("pnl_diff_30d") / F.col("balance_lag_30d"))
        .withColumn("pnl_ratio_24h_rank", F.rank().over(windowSpecRatioRank24h))
        .withColumn("pnl_ratio_7d_rank", F.rank().over(windowSpecRatioRank7d))
        .withColumn("pnl_ratio_30d_rank", F.rank().over(windowSpecRatioRank30d))
        .withColumn("pnl_diff_24h_rank", F.rank().over(windowSpecDiffRank24h))
        .withColumn("pnl_diff_7d_rank", F.rank().over(windowSpecDiffRank7d))
        .withColumn("pnl_diff_30d_rank", F.rank().over(windowSpecDiffRank30d))
        .filter(f"date_ = '{current_date}' and hour_ = {current_hour}")
    )

# COMMAND ----------


