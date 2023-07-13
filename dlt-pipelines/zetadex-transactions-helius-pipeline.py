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

days = lambda i: i * 86400
hours = lambda i: i * 3600

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
        .withWatermark("block_time", "5 minutes")
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
        .withWatermark("block_time", "5 minutes")
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
            "instruction.accounts.named.margin_account",
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
        .withWatermark("block_time", "5 minutes")
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
            "instruction.accounts.named.margin_account",
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
        .withWatermark("block_time", "5 minutes")
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
            "instruction.accounts.named.margin_account",
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
        .withWatermark("block_time", "5 minutes")
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
            "event.event.margin_account",
            "event.event.order_complete_type",
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
        .withWatermark("block_time", "5 minutes")
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
            F.col("instruction.accounts.named.liquidated_account").alias("liquidatee_margin_account"),
            "event.event.liquidator",
            F.col("instruction.accounts.named.liquidator_account").alias("liquidator_margin_account"),
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
        .withWatermark("block_time", "5 minutes")
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
        .withWatermark("block_time", "10 minutes")
        .filter(F.col("maker_taker") == "taker")
        .groupBy(F.window("block_time", "1 hour"))
        .agg(
            F.count("*").alias("trade_count"),
            F.sum("volume").alias("volume"),
        )
        .withColumn("timestamp", F.col("window.start"))
        .drop("window")
    )

@dlt.table(
    comment="24-hourly-asset rolling window aggregated data for trades",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "timestamp",
    },
    # partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "agg-trade-asset-24h-rolling"),
)
def agg_ix_trade_asset_24h_rolling():
    trades_df = dlt.read("agg_ix_trade_asset_1h")
    min_time, max_time = trades_df.select(F.min("timestamp"), F.max("timestamp")).first()
    assets = trades_df.select("asset").distinct()

    # Create spine for dataframe of all hours x all assets
    time_df = spark.sql(f"SELECT explode(sequence(timestamp('{min_time}'), timestamp('{max_time}'), interval 1 hour)) as timestamp")
    spine_df = time_df.crossJoin(assets)
    df = spine_df.join(trades_df, ['timestamp','asset'], how='left')
    df = df.fillna({'volume': 0, 'trade_count': 0})

    # Apply the rolling sum calculation
    window_24h = Window.partitionBy("asset").orderBy(F.unix_timestamp("timestamp")).rangeBetween(-days(1), 0)
    df = (df.withColumn("trade_count_24h", F.sum("trade_count").over(window_24h))
    .withColumn('volume_24h', F.sum('volume').over(window_24h))
    .drop("volume", "trade_count")
    )
    return df
    

@dlt.table(
    comment="User-hourly aggregated data for deposits",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "timestamp",
    },
    # partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "agg-deposit-user-1h"),
)
def agg_ix_deposit_user_1h():
    return (
        dlt.read_stream("cleaned_ix_deposit")
        .withWatermark("block_time", "10 minutes")
        .groupBy(
            "authority",
            # "margin_account",
            F.date_trunc("hour", "block_time").alias("timestamp"),
        )
        .agg(
            F.count("*").alias("deposit_count"),
            F.sum("deposit_amount").alias("deposit_amount"),
        )
    )


@dlt.table(
    comment="User-hourly aggregated data for withdrawals",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "timestamp",
    },
    # partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "agg-withdraw-user-1h"),
)
def agg_ix_withdraw_user_1h():
    return (
        dlt.read_stream("cleaned_ix_withdraw")
        .withWatermark("block_time", "10 minutes")
        .groupBy(
            "authority",
            # "margin_account",
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
        .withWatermark("block_time", "10 minutes") # TODO: change to 10m
        .withColumn("instruction", F.explode("instructions"))
        .withColumn("event", F.explode("instruction.events"))
        .filter(F.col("event.name").startswith('apply_funding_event'))
        .groupBy(
            F.col("event.event.user").alias("authority"),
            "event.event.margin_account",
            F.upper("event.event.asset").alias("asset"),
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

# underlying, owner_pub_key - deprecated
pnl_schema = """
timestamp timestamp,
underlying string,
owner_pub_key string,
authority string,
margin_account string,
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

windowSpecCumulative = (
    Window.partitionBy("p.authority")  # , "p.margin_account")
    .orderBy("p.timestamp")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)


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
    deposits_df = dlt.read("agg_ix_deposit_user_1h")
    withdrawals_df = dlt.read("agg_ix_withdraw_user_1h")
    return (
        dlt.read("raw_pnl")  # ideally would be read_stream (TODO)
        .withColumnRenamed("underlying", "asset")
        .withColumn("authority", F.coalesce("authority", "owner_pub_key"))
        .drop("owner_pub_key")
        .withColumn("timestamp", F.date_trunc("hour", "timestamp"))
        .alias("p")
        .groupBy("timestamp", "authority")  # , "margin_account")
        .agg(
            F.sum("balance").alias("balance"),
            F.sum("unrealized_pnl").alias("unrealized_pnl"),
        )
        .join(
            deposits_df.alias("d"),
            F.expr(
                """
                p.authority = d.authority AND
                -- p.margin_account = d.margin_account AND
                p.timestamp = d.timestamp + interval 1 hour
                """
            ),
            how="left",
        )
        .join(
            withdrawals_df.alias("w"),
            F.expr(
                """
                p.authority = w.authority AND
                -- p.margin_account = w.margin_account AND
                p.timestamp = w.timestamp + interval 1 hour
                """
            ),
            how="left",
        )
        .withColumn("deposit_amount", F.coalesce("deposit_amount", F.lit(0)))
        .withColumn("withdraw_amount", F.coalesce("withdraw_amount", F.lit(0)))
        .withColumn("net_inflow", F.col("deposit_amount") - F.col("withdraw_amount"))
        .withColumn(
            "deposit_amount_cumsum",
            F.sum("deposit_amount").over(windowSpecCumulative),
        )
        .withColumn(
            "withdraw_amount_cumsum",
            F.sum("withdraw_amount").over(windowSpecCumulative),
        )
        .withColumn("equity", F.col("balance") + F.col("unrealized_pnl"))
        .withColumn(
            "cumulative_pnl",
            F.col("equity")
            - (F.col("deposit_amount_cumsum") - F.col("withdraw_amount_cumsum")),
        )
        .select(
            "p.timestamp",
            "p.authority",
            # "p.margin_account",
            "balance",
            "unrealized_pnl",
            "equity",
            "cumulative_pnl",
            "deposit_amount",
            "withdraw_amount",
            "net_inflow",
            "deposit_amount_cumsum",
            "withdraw_amount_cumsum",
        )
    )

# COMMAND ----------

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
    windowSpec24h = (
        Window.partitionBy("authority")
        # Window.partitionBy("authority", "margin_account")
        .orderBy(F.unix_timestamp("timestamp")).rangeBetween(-days(1), 0)
    )
    windowSpec7d = (
        Window.partitionBy("authority")
        # Window.partitionBy("authority", "margin_account")
        .orderBy(F.unix_timestamp("timestamp")).rangeBetween(-days(7), 0)
    )
    windowSpec30d = (
        Window.partitionBy("authority")
        # Window.partitionBy("authority", "margin_account")
        .orderBy(F.unix_timestamp("timestamp")).rangeBetween(-days(30), 0)
    )

    # Need to make start exclusive since net deposits are in between snapshots
    windowSpec24hExclusive = (
        Window.partitionBy("authority")
        # Window.partitionBy("authority", "margin_account")
        .orderBy(F.unix_timestamp("timestamp")).rangeBetween(-days(1) + hours(1), 0)
    )
    windowSpec7dExclusive = (
        Window.partitionBy("authority")
        # Window.partitionBy("authority", "margin_account")
        .orderBy(F.unix_timestamp("timestamp")).rangeBetween(-days(7) + hours(1), 0)
    )
    windowSpec30dExclusive = (
        Window.partitionBy("authority")
        # Window.partitionBy("authority", "margin_account")
        .orderBy(F.unix_timestamp("timestamp")).rangeBetween(-days(30) + hours(1), 0)
    )

    windowSpecRatioRank24h = Window.partitionBy("timestamp").orderBy(
        F.desc("roi_24h"),
        F.desc("pnl_24h"),
        "authority",  # "margin_account"
    )
    windowSpecRatioRank7d = Window.partitionBy("timestamp").orderBy(
        F.desc("roi_7d"),
        F.desc("pnl_7d"),
        "authority",  # "margin_account"
    )
    windowSpecRatioRank30d = Window.partitionBy("timestamp").orderBy(
        F.desc("roi_30d"),
        F.desc("pnl_30d"),
        "authority",  # "margin_account"
    )
    windowSpecDiffRank24h = Window.partitionBy("timestamp").orderBy(
        F.desc("pnl_24h"),
        F.desc("roi_24h"),
        "authority",  # "margin_account"
    )
    windowSpecDiffRank7d = Window.partitionBy("timestamp").orderBy(
        F.desc("pnl_7d"),
        F.desc("roi_7d"),
        "authority",  # "margin_account"
    )
    windowSpecDiffRank30d = Window.partitionBy("timestamp").orderBy(
        F.desc("pnl_30d"),
        F.desc("roi_30d"),
        "authority",  # "margin_account"
    )

    return (
        dlt.read("cleaned_pnl")
        .withWatermark("timestamp", "10 minutes")
        .filter(
            "timestamp >= current_timestamp - interval 30 days"  # max pnl lookback aggregation
        )
        .withColumn(
            "cumulative_pnl_lag_24h", F.first("cumulative_pnl").over(windowSpec24h)
        )
        .withColumn(
            "cumulative_pnl_lag_7d", F.first("cumulative_pnl").over(windowSpec7d)
        )
        .withColumn(
            "cumulative_pnl_lag_30d", F.first("cumulative_pnl").over(windowSpec30d)
        )
        .withColumn("equity_lag_24h", F.first("equity").over(windowSpec24h))
        .withColumn("equity_lag_7d", F.first("equity").over(windowSpec7d))
        .withColumn("equity_lag_30d", F.first("equity").over(windowSpec30d))
        # inflows
        # .withColumn("net_inflow_24h", F.sum("net_inflow").over(windowSpec24hExclusive))
        # .withColumn("net_inflow_7d", F.sum("net_inflow").over(windowSpec7dExclusive))
        # .withColumn("net_inflow_30d", F.sum("net_inflow").over(windowSpec30dExclusive))
        # Modified Dietz
        .withColumn(
            "w_24h",
            (
                F.unix_timestamp(F.date_trunc("hour", F.current_timestamp()))
                - F.unix_timestamp(F.col("timestamp"))
            )
            / days(1),
        )
        .withColumn(
            "w_7d",
            (
                F.unix_timestamp(F.date_trunc("hour", F.current_timestamp()))
                - F.unix_timestamp(F.col("timestamp"))
            )
            / days(7),
        )
        .withColumn(
            "w_30d",
            (
                F.unix_timestamp(F.date_trunc("hour", F.current_timestamp()))
                - F.unix_timestamp(F.col("timestamp"))
            )
            / days(30),
        )
        .withColumn(
            "deposit_amount_weighted_24h",
            F.sum(F.col("deposit_amount") * F.col("w_24h")).over(
                windowSpec24hExclusive
            ),
        )
        .withColumn(
            "deposit_amount_weighted_7d",
            F.sum(F.col("deposit_amount") * F.col("w_7d")).over(windowSpec7dExclusive),
        )
        .withColumn(
            "deposit_amount_weighted_30d",
            F.sum(F.col("deposit_amount") * F.col("w_30d")).over(
                windowSpec30dExclusive
            ),
        )
        .drop("w_24h", "w_7d", "w_30d")
        # PnL and ROI
        .withColumn(
            "pnl_24h", F.col("cumulative_pnl") - F.col("cumulative_pnl_lag_24h")
        )
        .withColumn("pnl_7d", F.col("cumulative_pnl") - F.col("cumulative_pnl_lag_7d"))
        .withColumn(
            "pnl_30d", F.col("cumulative_pnl") - F.col("cumulative_pnl_lag_30d")
        )
        # Simple Dietz ROI calculation, using safe div 0/0 => 0
        # Using a $100 fudge factor in denominator (binance does this) to reduce impact of small balances
        # https://www.binance.com/en/support/faq/introduction-to-binance-futures-leaderboard-a507bdb81ad0464e871e60d43fd21526
        .withColumn(
            "roi_24h",
            F.when(F.col("pnl_24h") == 0, F.lit(0)).otherwise(
                F.col("pnl_24h")
                / (
                    100 + F.col("equity_lag_24h") + F.col("deposit_amount_weighted_24h")
                )  # 0.5 * F.col("net_inflow_24h")
            ),
        )
        .withColumn(
            "roi_7d",
            F.when(F.col("pnl_7d") == 0, F.lit(0)).otherwise(
                F.col("pnl_7d")
                / (100 + F.col("equity_lag_7d") + F.col("deposit_amount_weighted_7d"))
            ),
        )
        .withColumn(
            "roi_30d",
            F.when(F.col("pnl_30d") == 0, F.lit(0)).otherwise(
                F.col("pnl_30d")
                / (100 + F.col("equity_lag_30d") + F.col("deposit_amount_weighted_30d"))
            ),
        )
        # ranks
        .withColumn("pnl_24h_rank", F.rank().over(windowSpecDiffRank24h))
        .withColumn("pnl_7d_rank", F.rank().over(windowSpecDiffRank7d))
        .withColumn("pnl_30d_rank", F.rank().over(windowSpecDiffRank30d))
        .withColumn("roi_24h_rank", F.rank().over(windowSpecRatioRank24h))
        .withColumn("roi_7d_rank", F.rank().over(windowSpecRatioRank7d))
        .withColumn("roi_30d_rank", F.rank().over(windowSpecRatioRank30d))
        .filter("timestamp == date_trunc('hour',  current_timestamp)")
    )

# COMMAND ----------


