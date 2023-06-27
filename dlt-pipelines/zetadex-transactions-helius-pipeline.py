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

PRICE_FACTOR = 1e6
SIZE_FACTOR = 1e3

TRANSACTIONS_TABLE = "transactions-helius"
MARKETS_TABLE = "markets"
PNL_TABLE = "pnl"

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
        program_id: string,
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
fee int
"""
# date_ DATE GENERATED ALWAYS AS (CAST(block_time AS DATE))

@dlt.table(
    comment="Raw data for platform transactions",
    table_properties={
        "quality": "bronze",
        # "delta.autoOptimize.optimizeWrite": "true", 
        # "delta.autoOptimize.autoCompact": "true",
    },
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "raw"),
    schema=transactions_schema,
)
def raw_transactions():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.region", "ap-southeast-1")
        .option("cloudFiles.includeExistingFiles", True)
        .option("cloudFiles.useNotifications", True)
        .option("partitionColumns", "year,month,day,hour")
        .schema(transactions_schema)
        .load(join(BASE_PATH_LANDED, TRANSACTIONS_TABLE, "data"))
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
    return spark.table(f"zetadex_{NETWORK}.zetagroup_mapping").withColumnRenamed(
        "underlying", "asset"
    )


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
def cleaned_transactions():
    return (
        dlt.read_stream("raw_transactions")
        .withWatermark("block_time", "10 minute")
        .filter("is_successful")
        .dropDuplicates(["signature", "block_time"])
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
    # partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "cleaned-ix-deposit"),
)
def cleaned_ix_deposit():
    zetagroup_mapping_df = dlt.read("zetagroup_mapping_v")
    return (
        dlt.read_stream("cleaned_transactions")
        .withWatermark("block_time", "1 minute")
        .select(
            "*", F.posexplode("instructions").alias("instruction_index", "instruction")
        )
        .filter(F.col("instruction.name").startswith("deposit"))
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
            "instruction.accounts.named.authority",
            (F.col("instruction.args.amount") / PRICE_FACTOR).alias("deposit_amount"),
            F.col("instruction.accounts.named").alias("accounts"),
            "asset",
            "block_time",
            "slot",
        )
    )


# Withdraw
@dlt.table(
    comment="Cleaned data for withdraw instructions",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "block_time",
    },
    # partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "cleaned-ix-withdraw"),
)
def cleaned_ix_withdraw():
    zetagroup_mapping_df = dlt.read("zetagroup_mapping_v")
    return (
        dlt.read_stream("cleaned_transactions")
        .withWatermark("block_time", "1 minute")
        .select(
            "*", F.posexplode("instructions").alias("instruction_index", "instruction")
        )
        .filter(F.col("instruction.name").startswith("withdraw"))
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
            "instruction.accounts.named.authority",
            (F.col("instruction.args.amount") / PRICE_FACTOR).alias("withdraw_amount"),
            F.col("instruction.accounts.named").alias("accounts"),
            "asset",
            "block_time",
            "slot",
        )
    )


# Place order
@dlt.table(
    comment="Cleaned data for placeOrder type instructions",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "block_time",
    },
    # partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "cleaned-ix-place-order"),
)
def cleaned_ix_place_order():
    markets_df = spark.table("zetadex_mainnet_tx.cleaned_markets")
    return (
        dlt.read_stream("cleaned_transactions")
        .withWatermark("block_time", "1 minute")
        .select(
            "*", F.posexplode("instructions").alias("instruction_index", "instruction")
        )
        .filter(
            F.col("instruction.name").rlike("^place_(perp_)?order(_v[0-9]+)?$")
        )  # place_order and place_perp_order variants
        .withColumn("event", F.explode("instruction.events"))
        .filter(F.col("event.name").startswith("place_order_event"))
        .join(
            markets_df,
            (F.col("instruction.accounts.named.market") == markets_df.market_pub_key),
            how="left",
        )
        .select(
            "signature",
            "instruction_index",
            "asset",
            "instruction.name",
            "instruction.accounts.named.authority",
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
    )


# Cancel order
@dlt.table(
    comment="Cleaned data for order completion, this includes CancelOrder variants as well as trade fill events",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "block_time",
    },
    # partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "cleaned-ix-order-complete"),
)
def cleaned_ix_order_complete():
    markets_df = spark.table("zetadex_mainnet_tx.cleaned_markets")
    return (
        dlt.read_stream("cleaned_transactions")
        .withWatermark("block_time", "1 minute")
        .select(
            "*", F.posexplode("instructions").alias("instruction_index", "instruction")
        )
        .filter(
            (F.col("instruction.name").startswith("crank_event_queue"))  # maker fill
            | F.col("instruction.name").rlike(
                "^place_(perp_)?order(_v[0-9]+)?$"
            )  # taker fill
            | F.col("instruction.name").contains("cancel")  # cancel
        )
        .withColumn("event", F.explode("instruction.events"))
        .filter(F.col("event.name").startswith("order_complete_event"))
        .join(
            markets_df,
            (F.col("instruction.accounts.named.market") == markets_df.market_pub_key),
            how="left",
        )
        .select(
            "signature",
            "instruction_index",
            "asset",
            "instruction.name",
            "instruction.accounts.named.authority",
            "event.event.order_complete_type",
            # "event.event.market_index",
            "event.event.side",
            (F.col("event.event.unfilled_size") / SIZE_FACTOR).alias("unfilled_size"),
            "event.event.order_id",
            "event.event.client_order_id",
            F.col("instruction.accounts.named").alias("accounts"),
            "block_time",
            "slot",
        )
    )


# Liquidate
@dlt.table(
    comment="Cleaned data for liquidate instructions",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "block_time",
    },
    # partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "cleaned-ix-liquidate"),
)
def cleaned_ix_liquidate():
    markets_df = spark.table("zetadex_mainnet_tx.cleaned_markets")
    return (
        dlt.read_stream("cleaned_transactions")
        .withWatermark("block_time", "1 minute")
        .select(
            "*", F.posexplode("instructions").alias("instruction_index", "instruction")
        )
        .filter(F.col("instruction.name").startswith("liquidate"))
        .withColumn("event", F.explode("instruction.events"))
        .filter(F.col("event.name").startswith("liquidation_event"))
        .join(
            markets_df,
            (F.col("instruction.accounts.named.market") == markets_df.market_pub_key),
            how="left",
        )
        .select(
            "signature",
            "instruction_index",
            F.coalesce("asset", F.upper("event.event.asset")).alias("asset"),
            "instruction.name",
            (F.col("instruction.args.size") / SIZE_FACTOR).alias("desired_size"),
            F.when(F.col("event.event.size") > 0, "bid").otherwise("ask").alias("side"),
            "event.event.liquidatee",
            "event.event.liquidator",
            (F.col("event.event.liquidator_reward") / PRICE_FACTOR).alias(
                "liquidator_reward"
            ),
            (F.col("event.event.insurance_reward") / PRICE_FACTOR).alias(
                "insurance_reward"
            ),
            (F.col("event.event.cost_of_trades") / PRICE_FACTOR).alias(
                "cost_of_trades"
            ),
            (F.abs("event.event.size") / SIZE_FACTOR).alias("liquidated_size"),
            (F.col("event.event.remaining_liquidatee_balance") / PRICE_FACTOR).alias(
                "remaining_liquidatee_balance"
            ),
            (F.col("event.event.remaining_liquidator_balance") / PRICE_FACTOR).alias(
                "remaining_liquidator_balance"
            ),
            (F.col("event.event.mark_price") / PRICE_FACTOR).alias("mark_price"),
            (F.col("event.event.underlying_price") / PRICE_FACTOR).alias("index_price"),
            F.col("instruction.accounts.named").alias("accounts"),
            F.col("instruction.accounts.named.liquidated_margin_account").alias(
                "liquidated_margin_account"
            ),
            "block_time",
            "slot",
        )
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
    # partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "cleaned-ix-trade"),
)
def cleaned_ix_trade():
    markets_df = spark.table("zetadex_mainnet_tx.cleaned_markets")
    df = (
        dlt.read_stream("cleaned_transactions")
        .withWatermark("block_time", "1 minute")
        .select(
            "*", F.posexplode("instructions").alias("instruction_index", "instruction")
        )
    )
    maker_df = (
        df.filter(F.col("instruction.name").startswith("crank_event_queue"))
        .withColumn("event", F.explode("instruction.events"))
        .filter(F.col("event.name").startswith("trade_event"))
        .withColumn("maker_taker", F.lit("maker"))
    )
    taker_df = (
        df.filter(F.col("instruction.name").rlike("^place_(perp_)?order(_v[0-9]+)?$"))
        .filter(
            (F.array_contains("instruction.events.name", F.lit("trade_event")))
            | (F.array_contains("instruction.events.name", F.lit("trade_event_v2")))
            | (F.array_contains("instruction.events.name", F.lit("trade_event_v3")))
        )  # filter to only taker orders that trade
        .withColumn("event", place_trade_event_merge("instruction.events"))
        .withColumn("maker_taker", F.lit("taker"))
    )
    # Union all maker and taker
    return (
        maker_df.union(taker_df)
        .join(
            markets_df,
            (F.col("instruction.accounts.named.market") == markets_df.market_pub_key),
            how="left",
        )
        .select(
            "signature",
            "instruction_index",
            F.coalesce("asset", F.upper("event.event.asset")).alias("asset"),
            "event.name",
            F.col("event.event.user").alias("authority"),
            "event.event.margin_account",
            (
                (F.col("event.event.cost_of_trades") / F.col("event.event.size"))
                * (SIZE_FACTOR / PRICE_FACTOR)
            ).alias("price"),
            (F.col("event.event.size") / SIZE_FACTOR).alias("size"),
            (F.col("event.event.cost_of_trades") / PRICE_FACTOR).alias("volume"),
            F.when(F.col("event.event.is_bid").cast("boolean"), "bid")
            .otherwise("ask")
            .alias("side"),
            F.when(
                F.col("instruction.name").rlike("^place_(perp_)?order(_v[0-9]+)?$"),
                "taker",
            )
            .otherwise("maker")
            .alias("maker_taker"),
            "event.event.client_order_id",
            "event.event.order_id",
            "event.event.sequence_number",
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
        .withColumn(
            "epoch", F.date_trunc("week", F.expr("block_time - interval 104 hours"))
        )
        .withColumn("epoch", F.expr("epoch + interval 104 hours"))
    )

# COMMAND ----------

# DBTITLE 1,Gold
@dlt.table(
    comment="Asset-hourly aggregated data for trades",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "timestamp",
    },
    # partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "agg-trade-asset-1h"),
)
def agg_ix_trade_asset_1h():
    return (
        dlt.read_stream("cleaned_ix_trade")
        .filter(F.col("maker_taker") == "taker")
        .withWatermark("block_time", "10 minutes")
        .groupBy(
            "asset",
            F.date_trunc("hour", "block_time").alias("timestamp"),
        )
        .agg(
            F.count("*").alias("trade_count"),
            F.sum("volume").alias("volume"),
        )
    )


@dlt.table(
    comment="Hourly aggregated data for trades",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "timestamp",
    },
    # partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "agg-trade-1h"),
)
def agg_ix_trade_1h():
    return (
        dlt.read_stream("cleaned_ix_trade")
        .filter(F.col("maker_taker") == "taker")
        .withWatermark("block_time", "10 minutes")
        .groupBy(F.date_trunc("hour", "block_time").alias("timestamp"))
        .agg(
            F.count("*").alias("trade_count"),
            F.sum("volume").alias("volume"),
        )
    )

@dlt.table(
    comment="24-hourly rolling window aggregated data for trades",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "timestamp",
    },
    # partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "agg-trade-24h-rolling"),
)
def agg_ix_trade_24h_rolling():
    # window_24h = Window.orderBy(F.col("timestamp").cast("long")).rangeBetween(-24 * 60 * 60, 0)
    window_24h = Window.orderBy(F.col("timestamp")).rowsBetween(-23, Window.currentRow)

    return (
        dlt.read("agg_ix_trade_1h")
        .withColumn("trade_count_24h", F.sum("trade_count").over(window_24h))
        .withColumn("volume_24h", F.sum("volume").over(window_24h))
        .drop("volume", "trade_count")
    )
    

@dlt.table(
    comment="User-asset-hourly aggregated data for deposits",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "timestamp",
    },
    # partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "agg-deposit-user-asset-1h"),
)
def agg_ix_deposit_user_asset_1h():
    return (
        dlt.read_stream("cleaned_ix_deposit")
        .withWatermark("block_time", "10 minutes")
        .groupBy(
            F.col("accounts.authority"),
            "asset",
            F.date_trunc("hour", "block_time").alias("timestamp"),
        )
        .agg(
            F.count("*").alias("deposit_count"),
            F.sum("deposit_amount").alias("deposit_amount"),
        )
    )


@dlt.table(
    comment="User-asset-hourly aggregated data for withdrawals",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "timestamp",
    },
    # partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "agg-withdraw-user-asset-1h"),
)
def agg_ix_withdraw_user_asset_1h():
    return (
        dlt.read_stream("cleaned_ix_withdraw")
        .withWatermark("block_time", "10 minutes")
        .groupBy(
            F.col("accounts.authority"),
            "asset",
            F.date_trunc("hour", "block_time").alias("timestamp"),
        )
        .agg(
            F.count("*").alias("withdraw_count"),
            F.sum("withdraw_amount").alias("withdraw_amount"),
        )
    )

@dlt.table(
    comment="User-asset-hourly aggregated data for funding rate",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "timestamp",
    },
    # partition_cols=["asset"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "agg-funding-rate-user-asset-1h"),
)
def agg_funding_rate_user_asset_1h():
    return (
        dlt.read_stream("cleaned_transactions")
        .withWatermark("block_time", "1 minute")
        .withColumn("instruction", F.explode("instructions"))
        .withColumn("event", F.explode("instruction.events"))
        .filter(F.col("event.name").startswith('apply_funding_event'))
        .groupBy(
            F.col("event.event.user").alias("authority"),
            F.upper("event.event.asset").alias("asset"),
            "event.event.margin_account",
            F.date_trunc("hour", "block_time").alias(
                "timestamp"
            )
        )
        .agg(
            (F.sum(F.col("event.event.balance_change") / PRICE_FACTOR)).alias(
                "balance_change"
            )
        )
        .filter("balance_change != 0")
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
        "pipelines.autoOptimize.zOrderCols": "authority",
    },
    # partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, PNL_TABLE, "cleaned"),
)
def cleaned_pnl():
    deposits_df = dlt.read("agg_ix_deposit_user_asset_1h")
    withdrawals_df = dlt.read("agg_ix_withdraw_user_asset_1h")
    return (
        dlt.read("raw_pnl")  # ideally would be read_stream (TODO)
        .withColumnRenamed("underlying", "asset")
        .withColumnRenamed("owner_pub_key", "authority")
        .withColumn("timestamp", F.date_trunc("hour", "timestamp"))
        .alias("p")
        .join(
            deposits_df.alias("d"),
            F.expr(
                """
                p.asset = d.asset AND
                p.authority = d.authority AND
                p.timestamp = d.timestamp + interval 1 hour
                """
            ),
            how="left",
        )
        .join(
            withdrawals_df.alias("w"),
            F.expr(
                """
                p.asset = w.asset AND
                p.authority = w.authority AND
                p.timestamp = w.timestamp + interval 1 hour
                """
            ),
            how="left",
        )
        .withColumn("deposit_amount", F.coalesce("deposit_amount", F.lit(0)))
        .withColumn("withdraw_amount", F.coalesce("withdraw_amount", F.lit(0)))
        .withColumn(
            "deposit_amount_cumsum",
            F.sum("deposit_amount").over(
                Window.partitionBy("p.asset", "p.authority")
                .orderBy("p.timestamp")
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
        .withColumn(
            "withdraw_amount_cumsum",
            F.sum("withdraw_amount").over(
                Window.partitionBy("p.asset", "p.authority")
                .orderBy("p.timestamp")
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
        .withColumn(
            "pnl",
            F.col("balance")
            + F.col("unrealized_pnl")
            - (F.col("deposit_amount_cumsum") - F.col("withdraw_amount_cumsum")),
        )
        .select(
            "p.timestamp",
            "p.asset",
            "p.authority",
            "balance",
            "unrealized_pnl",
            "pnl",
            "deposit_amount",
            "withdraw_amount",
            "deposit_amount_cumsum",
            "withdraw_amount_cumsum",
        )
    )

# COMMAND ----------

from datetime import datetime, timezone
current_date = str(datetime.now(timezone.utc).date())
current_hour = datetime.now(timezone.utc).hour

days = lambda i: i * 86400

windowSpec24h = (
    Window.partitionBy("authority")
    .orderBy(F.unix_timestamp("timestamp"))
    .rangeBetween(-days(1), 0)
)
windowSpec7d = (
    Window.partitionBy("authority")
    .orderBy(F.unix_timestamp("timestamp"))
    .rangeBetween(-days(7), 0)
)
windowSpec30d = (
    Window.partitionBy("authority")
    .orderBy(F.unix_timestamp("timestamp"))
    .rangeBetween(-days(30), 0)
)

windowSpecRatioRank24h = Window.partitionBy("timestamp").orderBy(
    F.desc("pnl_ratio_24h"), F.desc("pnl_diff_24h"), "authority"
)
windowSpecRatioRank7d = Window.partitionBy("timestamp").orderBy(
    F.desc("pnl_ratio_7d"), F.desc("pnl_diff_7d"), "authority"
)
windowSpecRatioRank30d = Window.partitionBy("timestamp").orderBy(
    F.desc("pnl_ratio_30d"), F.desc("pnl_diff_30d"), "authority"
)
windowSpecDiffRank24h = Window.partitionBy("timestamp").orderBy(
    F.desc("pnl_diff_24h"), F.desc("pnl_ratio_24h"), "authority"
)
windowSpecDiffRank7d = Window.partitionBy("timestamp").orderBy(
    F.desc("pnl_diff_7d"), F.desc("pnl_ratio_7d"), "authority"
)
windowSpecDiffRank30d = Window.partitionBy("timestamp").orderBy(
    F.desc("pnl_diff_30d"), F.desc("pnl_ratio_30d"), "authority"
)

# Break ties in PnL ranking by pubkey alphabetically
@dlt.table(
    comment="User (24h, 7d, 30h) aggregated data for margin account profit and loss",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "authority",
    },
    # partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, PNL_TABLE, "agg"),
)
def agg_pnl():
    return (
        dlt.read("cleaned_pnl")
        .withWatermark("timestamp", "10 minutes")
        .groupBy("timestamp", "authority")
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
        .filter(F.col("timestamp") == F.date_trunc("hour", F.current_timestamp()))
    )

# COMMAND ----------


