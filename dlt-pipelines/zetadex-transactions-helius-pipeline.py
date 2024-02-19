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
COMPRESSED_NFT_BURN_TABLE = "nft-burns"
FEE_TIERS_TABLE = "feetiers"

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
# MAGIC
# MAGIC ## Compressed NFT Burns

# COMMAND ----------

# DBTITLE 1,Bronze
compressed_nft_burn_schema = """
accountData array<struct<
        account string,
        nativeBalanceChange int,
        tokenBalanceChanges array<string>
    >>,
description string,
events struct<
    compressed array<struct<
            assetId string,
            innerInstructionIndex int,
            instructionIndex int,
            leafIndex int,
            newLeafDelegate string,
            newLeafOwner string,
            oldLeafDelegate string,
            oldLeafOwner string,
            seq int,
            treeDelegate string,
            treeId string,
            type string
    >>
>,
fee int,
feePayer string,
instructions array<struct<
        accounts array<string>,
        data string,
        innerInstructions array<struct<
                accounts array<string>,
                data string,
                programId string
            >>,
        programId string
    >>,
nativeTransfers array<string>,
signature string,
slot bigint,
source string,
timestamp timestamp,
tokenTransfers array<string>,
transactionError string,
type string
"""

@dlt.table(
    comment="Raw data for burnt compressed nfts",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "timestamp",
    },
    path=join(BASE_PATH_TRANSFORMED, COMPRESSED_NFT_BURN_TABLE, "raw"),
    schema=compressed_nft_burn_schema,
)
def raw_compressed_nft_burn_events():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.region", "ap-southeast-1")
        .option("cloudFiles.includeExistingFiles", True)
        .option("cloudFiles.useNotifications", True)
        .option("partitionColumns", "year,month,day,hour")
        .schema(compressed_nft_burn_schema)
        .load(join(BASE_PATH_LANDED, COMPRESSED_NFT_BURN_TABLE, "data"))
    )

# COMMAND ----------

# DBTITLE 1,Silver
@dlt.view
def dim_zpass_nfts():
    return spark.table("zscore.dim_zpass_nfts")


# Processed Burn Events
@dlt.table(
    comment="Cleaned data for compressed nft burn events",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "timestamp,authority",
    },
    path=join(
        BASE_PATH_TRANSFORMED,
        COMPRESSED_NFT_BURN_TABLE,
        "cleaned-compressed-nft-burn-events",
    ),
)
def cleaned_compressed_nft_burn_events():
    df_dim_zpass_nfts = dlt.read("dim_zpass_nfts")
    return (
        dlt.read_stream("raw_compressed_nft_burn_events")
        .withWatermark("timestamp", "5 minutes")
        .alias("a")
        .join(
            df_dim_zpass_nfts.alias("b"),
            F.expr(
                """
                b.mint = a.events.compressed['assetId'][0]
                """
            ),
            how="left",
        )
        .select(
            "signature",
            F.col("events.compressed.assetId").getItem(0).alias("mint"),
            F.col("feePayer").alias("authority"),
            "color",
            "multiplier",
            "season",
            F.col("duration").alias("duration_hours"),
            F.col("timestamp").alias("start_timestamp"),
            F.expr("timestamp + INTERVAL 1 HOURS * duration").alias("end_timestamp"),
        )
        .filter(
            F.col("signature")
            != "52wXWMicfpXLzvDKbxtmmPwN3Jv2gq6NF1GtchG8iMQB94PwexTNkGZNYi749u9s8QLta6GVkdd5hr2391n9JYZY"
        )
    )

# COMMAND ----------

# DBTITLE 1,Gold
# Exploded hours per burn event
@dlt.table(
    comment="Exploded out cleaned burn compressed nft events, one row for each hour",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "timestamp,authority",
    },
    path=join(
        BASE_PATH_TRANSFORMED,
        COMPRESSED_NFT_BURN_TABLE,
        "exploded-cleaned-compressed-nft-burn-events",
    ),
)
def agg_compressed_nft_burn_events_hourly():
    return (
        dlt.read_stream("cleaned_compressed_nft_burn_events")
        .selectExpr(
            "EXPLODE(SEQUENCE(date_trunc('hour', start_timestamp), date_trunc('hour', end_timestamp - INTERVAL 1 HOUR), INTERVAL 1 HOUR)) as timestamp",
            "authority",
            "multiplier",
        )
        .withWatermark("timestamp", "5 minutes")
        .groupBy(F.date_trunc("hour", "timestamp").alias("timestamp"), "authority")
        .agg(
            F.max("multiplier").alias("multiplier")
        )  # take the max of overlapping multipliers
        .select(
            # F.col("window.start").alias("timestamp"),
            "timestamp",
            "authority",
            "multiplier"
        )
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
        "pipelines.autoOptimize.zOrderCols": "block_time",
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
from pyspark.sql import Row


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
        return Row(name="place_order_trade_event", event={**p.event, **t.event})
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
        # .dropDuplicates(["signature", "block_time"]) # super RAM intensive
        .drop("year", "month", "day", "hour")
        .withColumn("date_", F.to_date("block_time"))
        .withColumn("hour_", F.date_format("block_time", "HH").cast("int"))
    )


# Deposits
@dlt.table(
    comment="Cleaned data for deposit instructions",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "block_time,authority",
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
        "pipelines.autoOptimize.zOrderCols": "block_time,authority",
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
        "pipelines.autoOptimize.zOrderCols": "block_time,authority,asset",
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
            (
                F.col("instruction.name").rlike("^place_(perp_)?order(_v[0-9]+)?$")
                | (
                    F.col("instruction.name").rlike(
                        "^execute_trigger_order(_v[0-9]+)?$"
                    )
                )
            )
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
            F.coalesce("asset", F.upper("event.event.asset")).alias("asset"),
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
        "pipelines.autoOptimize.zOrderCols": "block_time,authority,asset",
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
            | (
                F.col("instruction.name").rlike("^execute_trigger_order(_v[0-9]+)?$")
            )  # trigger order
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
            F.coalesce("asset", F.upper("event.event.asset")).alias("asset"),
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
        "pipelines.autoOptimize.zOrderCols": "block_time,liquidatee,asset",
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
            F.col("instruction.accounts.named.liquidated_account").alias(
                "liquidatee_margin_account"
            ),
            "event.event.liquidator",
            F.col("instruction.accounts.named.liquidator_account").alias(
                "liquidator_margin_account"
            ),
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
        "pipelines.autoOptimize.zOrderCols": "block_time,authority,asset",
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
        df.filter(
            (F.col("instruction.name").rlike("^place_(perp_)?order(_v[0-9]+)?$"))
            | (F.col("instruction.name").rlike("^execute_trigger_order(_v[0-9]+)?$"))
        )
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
            F.col("maker_taker"),
            (F.col("event.event.pnl") / PRICE_FACTOR).alias("pnl"),
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


# Funding
@dlt.table(
    comment="Cleaned data for funding instructions",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "block_time,authority,asset",
    },
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "cleaned-ix-funding"),
)
def cleaned_ix_funding():
    return (
        dlt.read_stream("cleaned_transactions")
        .withWatermark("block_time", "5 minutes")
        .select(
            "*", F.posexplode("instructions").alias("instruction_index", "instruction")
        )
        .withColumn("event", F.explode("instruction.events"))
        .filter(F.col("event.name").startswith("apply_funding_event"))
        .filter("event.event.balance_change != 0")
        .select(
            "signature",
            "instruction_index",
            F.upper("event.event.asset").alias("asset"),
            "instruction.name",
            F.col("event.event.user").alias("authority"),
            "event.event.margin_account",
            (F.col("event.event.balance_change") / PRICE_FACTOR).alias(
                "balance_change"
            ),
            (F.col("event.event.funding_rate") / PRICE_FACTOR).alias("funding_rate"),
            (F.col("event.event.oracle_price") / PRICE_FACTOR).alias("oracle_price"),
            (F.col("event.event.position_size") / SIZE_FACTOR).alias("position_size"),
            F.col("instruction.accounts.named").alias("accounts"),
            "block_time",
            "slot",
        )
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from zetadex_mainnet_tx.agg_ix_trade_1h
# MAGIC where `timestamp` >= '2024-02-13T01:00:00'

# COMMAND ----------

# DBTITLE 1,Gold
@dlt.table(
    comment="Asset-hourly aggregated data for trades",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "timestamp,asset",
    },
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "agg-trade-asset-1h"),
)
def agg_ix_trade_asset_1h():
    return (
        dlt.read_stream("cleaned_ix_trade")
        .filter(F.col("maker_taker") == "taker")
        .withWatermark("block_time", "5 minutes")
        .groupBy(
            # F.window("block_time", "1 hour"),
            F.date_trunc("hour", "block_time").alias("timestamp"),
            "asset",
        )
        .agg(
            F.count("*").alias("trade_count"),
            F.sum("volume").alias("volume"),
        )
        # .withColumn("timestamp", F.col("window.start"))
        # .drop("window")
    )


@dlt.table(
    comment="Hourly aggregated data for trades",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "timestamp",
    },
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "agg-trade-1h"),
)
def agg_ix_trade_1h():
    return (
        dlt.read_stream("cleaned_ix_trade")
        .withWatermark("block_time", "5 minutes")
        .filter(F.col("maker_taker") == "taker")
        .groupBy(
            # F.window("block_time", "1 hour")
                 F.date_trunc("hour", "block_time").alias("timestamp"))
        .agg(
            F.count("*").alias("trade_count"),
            F.sum("volume").alias("volume"),
        )
        # .withColumn("timestamp", F.col("window.start"))
        # .drop("window")
    )


@dlt.table(
    comment="24-hourly-asset rolling window aggregated data for trades",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "timestamp,asset",
    },
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "agg-trade-asset-24h-rolling"),
)
def agg_ix_trade_asset_24h_rolling():
    trades_df = dlt.read("agg_ix_trade_asset_1h")
    min_time, max_time = trades_df.select(
        F.min("timestamp"), F.max("timestamp")
    ).first()
    assets = trades_df.select("asset").distinct()

    # Create spine for dataframe of all hours x all assets
    time_df = spark.sql(
        f"SELECT explode(sequence(timestamp('{min_time}'), timestamp('{max_time}'), interval 1 hour)) as timestamp"
    )
    spine_df = time_df.crossJoin(assets)
    df = spine_df.join(trades_df, ["timestamp", "asset"], how="left")
    df = df.fillna({"volume": 0, "trade_count": 0})

    # Apply the rolling sum calculation
    window_24h = (
        Window.partitionBy("asset")
        .orderBy(F.unix_timestamp("timestamp"))
        .rangeBetween(-days(1), 0)
    )
    df = (
        df.withColumn("trade_count_24h", F.sum("trade_count").over(window_24h))
        .withColumn("volume_24h", F.sum("volume").over(window_24h))
        .drop("volume", "trade_count")
    )
    return df


@dlt.table(
    comment="User-hourly aggregated data for deposits",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "timestamp,authority",
    },
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "agg-deposit-user-1h"),
)
def agg_ix_deposit_user_1h():
    return (
        dlt.read_stream("cleaned_ix_deposit")
        .withWatermark("block_time", "5 minutes")
        .groupBy(
            # F.window("block_time", "1 hour"),
            F.date_trunc("hour", "block_time").alias("timestamp"),
            "authority",
            "margin_account",
        )
        .agg(
            F.count("*").alias("deposit_count"),
            F.sum("deposit_amount").alias("deposit_amount"),
        )
        # .withColumn("timestamp", F.col("window.start"))
        # .drop("window")
    )


@dlt.table(
    comment="User-hourly aggregated data for withdrawals",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "timestamp,authority",
    },
    # partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "agg-withdraw-user-1h"),
)
def agg_ix_withdraw_user_1h():
    return (
        dlt.read_stream("cleaned_ix_withdraw")
        .withWatermark("block_time", "5 minutes")
        .groupBy(
            # F.window("block_time", "1 hour"),
            F.date_trunc("hour", "block_time").alias("timestamp"),
            "authority",
            "margin_account",
        )
        .agg(
            F.count("*").alias("withdraw_count"),
            F.sum("withdraw_amount").alias("withdraw_amount"),
        )
        # .withColumn("timestamp", F.col("window.start"))
        # .drop("window")
    )


@dlt.table(
    comment="User-asset-hourly aggregated data for funding rate",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "timestamp,authority,asset",
    },
    path=join(
        BASE_PATH_TRANSFORMED, TRANSACTIONS_TABLE, "agg-funding-rate-user-asset-1h"
    ),
)
def agg_funding_rate_user_asset_1h():
    return (
        dlt.read_stream("cleaned_ix_funding")
        .withWatermark("block_time", "5 minutes")  # TODO: change to 10m? Actually nah, causes pipeline to skip since cluster kicks off at 5-10m after the hour
        .groupBy(
            # F.window("block_time", "1 hour"),
            F.date_trunc("hour", "block_time").alias("timestamp"),
            "asset",
            "authority",
            "margin_account",
        )
        .agg(
            F.sum("balance_change").alias("balance_change"),
            F.avg("funding_rate").alias("funding_rate"),
            F.avg("oracle_price").alias("oracle_price"),
            F.avg("position_size").alias("position_size"),
        )
        # .withColumn("timestamp", F.col("window.start"))
        # .drop("window")
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

@dlt.table(
    comment="Cleaned data for margin account profit and loss",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "timestamp,authority",
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, PNL_TABLE, "cleaned"),
)
def cleaned_pnl():
    windowSpecCumulative = (
        Window.partitionBy("p.authority", "p.margin_account")
        .orderBy("p.timestamp")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    
    deposits_df = dlt.read("agg_ix_deposit_user_1h")
    withdrawals_df = dlt.read("agg_ix_withdraw_user_1h")
    return (
        dlt.read("raw_pnl")  # ideally would be read_stream (TODO)
        .filter("year || month || day >= 20231001") # V2 date filter - pnl indexing was problematic before this
        .filter(F.col("underlying").isNull()) # V2 filter
        # .filter("balance > 0") # remove 0 balances
        # .withColumnRenamed("underlying", "asset")
        .withColumn("authority", F.coalesce("authority", "owner_pub_key"))
        .drop("owner_pub_key")
        .withColumn("timestamp", F.date_trunc("hour", "timestamp"))
        .alias("p")
        .groupBy("timestamp", "authority", "margin_account")
        .agg(
            F.sum("balance").alias("balance"),
            F.sum("unrealized_pnl").alias("unrealized_pnl"),
        )
        .join(
            deposits_df.alias("d"),
            F.expr(
                """
                p.authority = d.authority AND
                p.margin_account = d.margin_account AND
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
                p.margin_account = w.margin_account AND
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
            "p.margin_account",
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
        .withColumn("date_", F.date_trunc("day", "timestamp"))
    )

# COMMAND ----------

# # Break ties in PnL ranking by pubkey alphabetically
# @dlt.table(
#     comment="User (24h, 7d, 30h) aggregated data for margin account profit and loss",
#     table_properties={
#         "quality": "gold",
#         "pipelines.autoOptimize.zOrderCols": "margin_account",
#     },
#     partition_cols=["date_"],
#     path=join(BASE_PATH_TRANSFORMED, PNL_TABLE, "agg"),
# )
# def agg_pnl():
#     windowSpec24h = (
#         Window.partitionBy("a.authority", "a.margin_account")
#         .orderBy(F.unix_timestamp("a.timestamp"))
#         .rangeBetween(-days(1), 0)
#     )
#     windowSpec7d = (
#         Window.partitionBy("a.authority", "a.margin_account")
#         .orderBy(F.unix_timestamp("a.timestamp"))
#         .rangeBetween(-days(7), 0)
#     )
#     windowSpec30d = (
#         Window.partitionBy("a.authority", "a.margin_account")
#         .orderBy(F.unix_timestamp("a.timestamp"))
#         .rangeBetween(-days(30), 0)
#     )
#     windowSpecAlltime = (
#         Window.partitionBy("a.authority", "a.margin_account")
#         .orderBy(F.unix_timestamp("a.timestamp"))
#         .rowsBetween(Window.unboundedPreceding, Window.currentRow)
#     )

#     # Need to make start exclusive since net deposits are in between snapshots
#     windowSpec24hExclusive = (
#         Window.partitionBy("authority", "margin_account")
#         .orderBy(F.unix_timestamp("timestamp"))
#         .rangeBetween(-days(1) + hours(1), 0)
#     )
#     windowSpec7dExclusive = (
#         Window.partitionBy("authority", "margin_account")
#         .orderBy(F.unix_timestamp("timestamp"))
#         .rangeBetween(-days(7) + hours(1), 0)
#     )
#     windowSpec30dExclusive = (
#         Window.partitionBy("authority", "margin_account")
#         .orderBy(F.unix_timestamp("timestamp"))
#         .rangeBetween(-days(30) + hours(1), 0)
#     )

#     windowSpecPnlRank24h = Window.partitionBy("timestamp").orderBy(
#         F.desc("pnl_24h"), F.desc("roi_24h"), "margin_account"
#     )
#     windowSpecPnlRank7d = Window.partitionBy("timestamp").orderBy(
#         F.desc("pnl_7d"), F.desc("roi_7d"), "margin_account"
#     )
#     windowSpecPnlRank30d = Window.partitionBy("timestamp").orderBy(
#         F.desc("pnl_30d"), F.desc("roi_30d"), "margin_account"
#     )
#     windowSpecPnlRankAlltime = Window.partitionBy("timestamp").orderBy(
#         F.desc("pnl_alltime"), "margin_account"
#     )
#     windowSpecRoiRank24h = Window.partitionBy("timestamp").orderBy(
#         F.desc("roi_24h"), F.desc("pnl_24h"), "margin_account"
#     )
#     windowSpecRoiRank7d = Window.partitionBy("timestamp").orderBy(
#         F.desc("roi_7d"), F.desc("pnl_7d"), "margin_account"
#     )
#     windowSpecRoiRank30d = Window.partitionBy("timestamp").orderBy(
#         F.desc("roi_30d"), F.desc("pnl_30d"), "margin_account"
#     )

#     mm_df = spark.table("zetadex_mainnet.pubkey_label").alias("mm")

#     leaderboard_df = (
#         dlt.read("cleaned_pnl")
#         .alias("a")
#         .withWatermark("timestamp", "5 minutes")
#         .filter(
#             "timestamp < '2024-01-01'"
#         )
#         .join(
#             mm_df,
#             on=F.expr("a.authority == mm.pub_key"),
#             how="left_anti",
#         )  # remove MMs
#         .withColumn(
#             "cumulative_pnl_lag_24h", F.first("cumulative_pnl").over(windowSpec24h)
#         )
#         .withColumn(
#             "cumulative_pnl_lag_7d", F.first("cumulative_pnl").over(windowSpec7d)
#         )
#         .withColumn(
#             "cumulative_pnl_lag_30d", F.first("cumulative_pnl").over(windowSpec30d)
#         )
#         .withColumn(
#             "cumulative_pnl_lag_alltime",
#             F.first("cumulative_pnl").over(windowSpecAlltime),
#         )
#         .withColumn("equity_lag_24h", F.first("equity").over(windowSpec24h))
#         .withColumn("equity_lag_7d", F.first("equity").over(windowSpec7d))
#         .withColumn("equity_lag_30d", F.first("equity").over(windowSpec30d))
#         # Modified Dietz
#         .withColumn(
#             "w_24h",
#             (
#                 F.unix_timestamp(F.date_trunc("hour", F.current_timestamp()))
#                 - F.unix_timestamp(F.col("timestamp"))
#             )
#             / days(1),
#         )
#         .withColumn(
#             "w_7d",
#             (
#                 F.unix_timestamp(F.date_trunc("hour", F.current_timestamp()))
#                 - F.unix_timestamp(F.col("timestamp"))
#             )
#             / days(7),
#         )
#         .withColumn(
#             "w_30d",
#             (
#                 F.unix_timestamp(F.date_trunc("hour", F.current_timestamp()))
#                 - F.unix_timestamp(F.col("timestamp"))
#             )
#             / days(30),
#         )
#         .withColumn(
#             "deposit_amount_weighted_24h",
#             F.sum(F.col("deposit_amount") * F.col("w_24h")).over(
#                 windowSpec24hExclusive
#             ),
#         )
#         .withColumn(
#             "deposit_amount_weighted_7d",
#             F.sum(F.col("deposit_amount") * F.col("w_7d")).over(windowSpec7dExclusive),
#         )
#         .withColumn(
#             "deposit_amount_weighted_30d",
#             F.sum(F.col("deposit_amount") * F.col("w_30d")).over(
#                 windowSpec30dExclusive
#             ),
#         )
#         .drop("w_24h", "w_7d", "w_30d")
#         # PnL and ROI
#         .withColumn(
#             "pnl_24h", F.col("cumulative_pnl") - F.col("cumulative_pnl_lag_24h")
#         )
#         .withColumn("pnl_7d", F.col("cumulative_pnl") - F.col("cumulative_pnl_lag_7d"))
#         .withColumn(
#             "pnl_30d", F.col("cumulative_pnl") - F.col("cumulative_pnl_lag_30d")
#         )
#         .withColumn(
#             "pnl_alltime", F.col("cumulative_pnl") - F.col("cumulative_pnl_lag_alltime")
#         )
#         # Simple Dietz ROI calculation, using safe div 0/0 => 0
#         # Using a $100 fudge factor in denominator (binance does this) to reduce impact of small balances
#         # https://www.binance.com/en/support/faq/introduction-to-binance-futures-leaderboard-a507bdb81ad0464e871e60d43fd21526
#         .withColumn(
#             "roi_24h",
#             F.when(F.col("pnl_24h") == 0, F.lit(0)).otherwise(
#                 F.col("pnl_24h")
#                 / (
#                     100 + F.col("equity_lag_24h") + F.col("deposit_amount_weighted_24h")
#                 )  # 0.5 * F.col("net_inflow_24h")
#             ),
#         )
#         .withColumn(
#             "roi_7d",
#             F.when(F.col("pnl_7d") == 0, F.lit(0)).otherwise(
#                 F.col("pnl_7d")
#                 / (100 + F.col("equity_lag_7d") + F.col("deposit_amount_weighted_7d"))
#             ),
#         )
#         .withColumn(
#             "roi_30d",
#             F.when(F.col("pnl_30d") == 0, F.lit(0)).otherwise(
#                 F.col("pnl_30d")
#                 / (100 + F.col("equity_lag_30d") + F.col("deposit_amount_weighted_30d"))
#             ),
#         )
#         # ranks
#         .withColumn("pnl_24h_rank", F.rank().over(windowSpecPnlRank24h))
#         .withColumn("pnl_7d_rank", F.rank().over(windowSpecPnlRank7d))
#         .withColumn("pnl_30d_rank", F.rank().over(windowSpecPnlRank30d))
#         .withColumn("pnl_alltime_rank", F.rank().over(windowSpecPnlRankAlltime))
#         .withColumn("roi_24h_rank", F.rank().over(windowSpecRoiRank24h))
#         .withColumn("roi_7d_rank", F.rank().over(windowSpecRoiRank7d))
#         .withColumn("roi_30d_rank", F.rank().over(windowSpecRoiRank30d))
#         .withColumn(
#             "pnl_24h_rank_change",
#             -(F.col("pnl_24h_rank") - F.first("pnl_24h_rank").over(windowSpec24h)),
#         )
#         .withColumn(
#             "pnl_7d_rank_change",
#             -(F.col("pnl_7d_rank") - F.first("pnl_7d_rank").over(windowSpec7d)),
#         )
#         .withColumn(
#             "pnl_30d_rank_change",
#             -(F.col("pnl_30d_rank") - F.first("pnl_30d_rank").over(windowSpec30d)),
#         )
#         .withColumn(
#             "pnl_alltime_rank_change",
#             -(
#                 F.col("pnl_alltime_rank")
#                 - F.first("pnl_alltime_rank").over(windowSpecAlltime)
#             ),
#         )
#         .withColumn(
#             "roi_24h_rank_change",
#             -(F.col("roi_24h_rank") - F.first("roi_24h_rank").over(windowSpec24h)),
#         )
#         .withColumn(
#             "roi_7d_rank_change",
#             -(F.col("roi_7d_rank") - F.first("roi_7d_rank").over(windowSpec7d)),
#         )
#         .withColumn(
#             "roi_30d_rank_change",
#             -(F.col("roi_30d_rank") - F.first("roi_30d_rank").over(windowSpec30d)),
#         )
#         .withColumn("date_", F.date_trunc("day", "timestamp"))
#         # .filter("timestamp == date_trunc('hour',  current_timestamp)")
#     )

#     # Z-Score
#     nft_df = dlt.read("agg_compressed_nft_burn_events_hourly").alias("nft")
#     s1_campaign_df = spark.table("zscore.season1_campaign_results").alias("s1")
#     # Taker volume
#     trades_df = (
#         dlt.read("cleaned_ix_trade")
#         # .withWatermark("block_time", "5 minutes")
#         # .join(
#         #     mm_df,
#         #     on=F.expr("cleaned_ix_trade.authority == mm.pub_key"),
#         #     how="left_anti",
#         # )
#         .filter(F.col("maker_taker") == "taker")
#         .withColumn("timestamp", F.date_trunc("hour", "block_time"))
#         # .filter("timestamp >= '2023-06-30' and timestamp < '2024-01-01'")
#         .filter("timestamp >= '2023-06-30' and timestamp < '2024-01-01'")
#         .groupBy("timestamp", "authority", "margin_account")
#         .agg(F.sum("volume").alias("volume"), F.sum("trading_fee").alias("trading_fee"))
#     ).alias("b")

#     windowSpecZscoreRank24h = Window.partitionBy("a.timestamp").orderBy(
#         F.desc("z_score_24h"), F.desc("pnl_24h"), "a.margin_account"
#     )
#     windowSpecZscoreRank7d = Window.partitionBy("a.timestamp").orderBy(
#         F.desc("z_score_7d"), F.desc("pnl_7d"), "a.margin_account"
#     )
#     windowSpecZscoreRank30d = Window.partitionBy("a.timestamp").orderBy(
#         F.desc("z_score_30d"), F.desc("pnl_30d"), "a.margin_account"
#     )
#     windowSpecZscoreRankAlltime = Window.partitionBy("a.timestamp").orderBy(
#         F.desc("z_score_alltime"), F.desc("pnl_alltime"), "a.margin_account"
#     )

#     df = (
#         leaderboard_df.join(
#             trades_df,
#             on=F.expr(
#                 "a.timestamp = b.timestamp + interval 1 hour and a.margin_account = b.margin_account and a.authority = b.authority"
#             ),
#             how="left",
#         )  # + interval 1 hour if we want to use start or end of hour
#         .join(nft_df, on=["timestamp", "authority"], how="left")
#         .join(
#             s1_campaign_df, on=["timestamp", "authority", "margin_account"], how="left"
#         )
#         .withColumn("s1_campaign_z_score", F.coalesce(F.col("s1.z_score"), F.lit(0)))
#         .withColumn("z_multiplier_nft", F.coalesce(F.col("nft.multiplier"), F.lit(1)))
#         .withColumn("volume", F.coalesce("volume", F.lit(0)))
#         .withColumn(
#             "z_multiplier",
#             F.when(
#                 F.col("pnl_24h_rank") <= 100, 2 - (F.col("pnl_24h_rank") - 1) * 0.01
#             ).otherwise(1),
#         )
#         .withColumn(
#             "z_score",
#             F.coalesce(
#                 ((F.col("z_multiplier") * F.col("z_multiplier_nft")) * F.col("volume"))
#                 + F.col("s1_campaign_z_score"),
#                 F.lit(0),
#             ),
#         )
#         # volume
#         .withColumn("volume_24h", F.sum("volume").over(windowSpec24h))
#         .withColumn("volume_7d", F.sum("volume").over(windowSpec7d))
#         .withColumn("volume_30d", F.sum("volume").over(windowSpec30d))
#         .withColumn("volume_alltime", F.sum("volume").over(windowSpecAlltime))
#         # multiplier
#         .withColumn(
#             "z_multiplier_24h", F.col("z_multiplier")
#         )  # F.avg("z_multiplier").over(windowSpec24h))
#         .withColumn("z_multiplier_7d", F.avg("z_multiplier").over(windowSpec7d))
#         .withColumn("z_multiplier_30d", F.avg("z_multiplier").over(windowSpec30d))
#         .withColumn(
#             "z_multiplier_alltime", F.avg("z_multiplier").over(windowSpecAlltime)
#         )
#         # z-score
#         .withColumn("z_score_24h", F.sum("z_score").over(windowSpec24h))
#         .withColumn("z_score_7d", F.sum("z_score").over(windowSpec7d))
#         .withColumn("z_score_30d", F.sum("z_score").over(windowSpec30d))
#         .withColumn("z_score_alltime", F.sum("z_score").over(windowSpecAlltime))
#         # rank
#         .withColumn("z_score_24h_rank", F.rank().over(windowSpecZscoreRank24h))
#         .withColumn("z_score_7d_rank", F.rank().over(windowSpecZscoreRank7d))
#         .withColumn("z_score_30d_rank", F.rank().over(windowSpecZscoreRank30d))
#         .withColumn("z_score_alltime_rank", F.rank().over(windowSpecZscoreRankAlltime))
#         .withColumn(
#             "z_score_24h_rank_change",
#             -(
#                 F.col("z_score_24h_rank")
#                 - F.first("z_score_24h_rank").over(windowSpec24h)
#             ),
#         )
#         .withColumn(
#             "z_score_7d_rank_change",
#             -(F.col("z_score_7d_rank") - F.first("z_score_7d_rank").over(windowSpec7d)),
#         )
#         .withColumn(
#             "z_score_30d_rank_change",
#             -(
#                 F.col("z_score_30d_rank")
#                 - F.first("z_score_30d_rank").over(windowSpec30d)
#             ),
#         )
#         .withColumn(
#             "z_score_alltime_rank_change",
#             -(
#                 F.col("z_score_alltime_rank")
#                 - F.first("z_score_alltime_rank").over(windowSpecAlltime)
#             ),
#         )
#         .select(
#             "a.timestamp",
#             "a.authority",
#             "a.margin_account",
#             "a.balance",
#             "a.unrealized_pnl",
#             "a.equity",
#             "a.cumulative_pnl",
#             "a.deposit_amount_cumsum",
#             "a.withdraw_amount_cumsum",
#             # metrics
#             "pnl_24h",
#             "pnl_7d",
#             "pnl_30d",
#             "pnl_alltime",
#             "roi_24h",
#             "roi_7d",
#             "roi_30d",
#             "pnl_24h_rank",
#             "pnl_7d_rank",
#             "pnl_30d_rank",
#             "pnl_alltime_rank",
#             "roi_24h_rank",
#             "roi_7d_rank",
#             "roi_30d_rank",
#             "pnl_24h_rank_change",
#             "pnl_7d_rank_change",
#             "pnl_30d_rank_change",
#             "pnl_alltime_rank_change",
#             "roi_24h_rank_change",
#             "roi_7d_rank_change",
#             "roi_30d_rank_change",
#             # z-scores
#             "volume_24h",
#             "volume_7d",
#             "volume_30d",
#             "volume_alltime",
#             "z_multiplier_24h",
#             "z_multiplier_7d",
#             "z_multiplier_30d",
#             "z_multiplier_alltime",
#             "z_multiplier_nft",
#             "z_score_24h",
#             "z_score_7d",
#             "z_score_30d",
#             "z_score_alltime",
#             "z_score_24h_rank",
#             "z_score_7d_rank",
#             "z_score_30d_rank",
#             "z_score_alltime_rank",
#             "z_score_24h_rank_change",
#             "z_score_7d_rank_change",
#             "z_score_30d_rank_change",
#             "z_score_alltime_rank_change",
#             "date_",
#         )
#     )
#     return df

# COMMAND ----------

# Break ties in PnL ranking by pubkey alphabetically
@dlt.table(
    comment="User (24h, 7d, 30h) aggregated data for margin account profit and loss",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "authority,margin_account",
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, PNL_TABLE, "agg"),
)
def agg_pnl():
    windowSpec24h = (
        Window.partitionBy("a.authority", "a.margin_account")
        .orderBy(F.unix_timestamp("a.timestamp"))
        .rangeBetween(-days(1), 0)
    )
    windowSpec7d = (
        Window.partitionBy("a.authority", "a.margin_account")
        .orderBy(F.unix_timestamp("a.timestamp"))
        .rangeBetween(-days(7), 0)
    )
    windowSpec30d = (
        Window.partitionBy("a.authority", "a.margin_account")
        .orderBy(F.unix_timestamp("a.timestamp"))
        .rangeBetween(-days(30), 0)
    )
    windowSpecAlltime = (
        Window.partitionBy("a.authority", "a.margin_account")
        .orderBy(F.unix_timestamp("a.timestamp"))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    # Need to make start exclusive since net deposits are in between snapshots
    windowSpec24hExclusive = (
        Window.partitionBy("authority", "margin_account")
        .orderBy(F.unix_timestamp("timestamp"))
        .rangeBetween(-days(1) + hours(1), 0)
    )
    windowSpec7dExclusive = (
        Window.partitionBy("authority", "margin_account")
        .orderBy(F.unix_timestamp("timestamp"))
        .rangeBetween(-days(7) + hours(1), 0)
    )
    windowSpec30dExclusive = (
        Window.partitionBy("authority", "margin_account")
        .orderBy(F.unix_timestamp("timestamp"))
        .rangeBetween(-days(30) + hours(1), 0)
    )

    windowSpecPnlRank24h = Window.partitionBy("timestamp").orderBy(
        F.desc("pnl_24h"), F.desc("roi_24h"), "margin_account"
    )
    windowSpecPnlRank7d = Window.partitionBy("timestamp").orderBy(
        F.desc("pnl_7d"), F.desc("roi_7d"), "margin_account"
    )
    windowSpecPnlRank30d = Window.partitionBy("timestamp").orderBy(
        F.desc("pnl_30d"), F.desc("roi_30d"), "margin_account"
    )
    windowSpecPnlRankAlltime = Window.partitionBy("timestamp").orderBy(
        F.desc("pnl_alltime"), "margin_account"
    )
    windowSpecRoiRank24h = Window.partitionBy("timestamp").orderBy(
        F.desc("roi_24h"), F.desc("pnl_24h"), "margin_account"
    )
    windowSpecRoiRank7d = Window.partitionBy("timestamp").orderBy(
        F.desc("roi_7d"), F.desc("pnl_7d"), "margin_account"
    )
    windowSpecRoiRank30d = Window.partitionBy("timestamp").orderBy(
        F.desc("roi_30d"), F.desc("pnl_30d"), "margin_account"
    )

    mm_df = spark.table("zetadex_mainnet.pubkey_label").alias("mm")

    leaderboard_df = (
        dlt.read("cleaned_pnl")
        .alias("a")
        .withWatermark("timestamp", "5 minutes")
        .filter(
            "timestamp >= '2024-01-01'"
        ) # season 2
        .join(
            mm_df,
            on=F.expr("a.authority == mm.pub_key"),
            how="left_anti",
        )  # remove MMs
        .withColumn(
            "cumulative_pnl_lag_24h", F.first("cumulative_pnl").over(windowSpec24h)
        )
        .withColumn(
            "cumulative_pnl_lag_7d", F.first("cumulative_pnl").over(windowSpec7d)
        )
        .withColumn(
            "cumulative_pnl_lag_30d", F.first("cumulative_pnl").over(windowSpec30d)
        )
        .withColumn(
            "cumulative_pnl_lag_alltime",
            F.first("cumulative_pnl").over(windowSpecAlltime),
        )
        .withColumn("equity_lag_24h", F.first("equity").over(windowSpec24h))
        .withColumn("equity_lag_7d", F.first("equity").over(windowSpec7d))
        .withColumn("equity_lag_30d", F.first("equity").over(windowSpec30d))
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
        .withColumn(
            "pnl_alltime", F.col("cumulative_pnl") - F.col("cumulative_pnl_lag_alltime")
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
        .withColumn("pnl_24h_rank", F.rank().over(windowSpecPnlRank24h))
        .withColumn("pnl_7d_rank", F.rank().over(windowSpecPnlRank7d))
        .withColumn("pnl_30d_rank", F.rank().over(windowSpecPnlRank30d))
        .withColumn("pnl_alltime_rank", F.rank().over(windowSpecPnlRankAlltime))
        .withColumn("roi_24h_rank", F.rank().over(windowSpecRoiRank24h))
        .withColumn("roi_7d_rank", F.rank().over(windowSpecRoiRank7d))
        .withColumn("roi_30d_rank", F.rank().over(windowSpecRoiRank30d))
        .withColumn(
            "pnl_24h_rank_change",
            -(F.col("pnl_24h_rank") - F.first("pnl_24h_rank").over(windowSpec24h)),
        )
        .withColumn(
            "pnl_7d_rank_change",
            -(F.col("pnl_7d_rank") - F.first("pnl_7d_rank").over(windowSpec7d)),
        )
        .withColumn(
            "pnl_30d_rank_change",
            -(F.col("pnl_30d_rank") - F.first("pnl_30d_rank").over(windowSpec30d)),
        )
        .withColumn(
            "pnl_alltime_rank_change",
            -(
                F.col("pnl_alltime_rank")
                - F.first("pnl_alltime_rank").over(windowSpecAlltime)
            ),
        )
        .withColumn(
            "roi_24h_rank_change",
            -(F.col("roi_24h_rank") - F.first("roi_24h_rank").over(windowSpec24h)),
        )
        .withColumn(
            "roi_7d_rank_change",
            -(F.col("roi_7d_rank") - F.first("roi_7d_rank").over(windowSpec7d)),
        )
        .withColumn(
            "roi_30d_rank_change",
            -(F.col("roi_30d_rank") - F.first("roi_30d_rank").over(windowSpec30d)),
        )
        .withColumn("date_", F.date_trunc("day", "timestamp"))
        # .filter("timestamp == date_trunc('hour',  current_timestamp)")
    )

    # Z-Score
    nft_df = dlt.read("agg_compressed_nft_burn_events_hourly").alias("nft")
    campaign_df = spark.table("zscore.campaign_results").alias("cr")
    # Taker volume
    trades_df = (
        dlt.read("cleaned_ix_trade")
        # .withWatermark("block_time", "5 minutes")
        # .join(
        #     mm_df,
        #     on=F.expr("cleaned_ix_trade.authority == mm.pub_key"),
        #     how="left_anti",
        # )
        # .filter(F.col("maker_taker") == "taker")
        .withColumn("timestamp", F.date_trunc("hour", "block_time"))
        .filter(F.col("timestamp") >= "2024-01-01T00:00:00") # 2023-06-30T00:00:00 for s1
        .groupBy("timestamp", "authority", "margin_account")
        .agg(
            F.sum(F.when(F.col("maker_taker") == 'taker', F.col("volume")).otherwise(0)).alias('taker_volume'),
            F.sum(F.when(F.col("maker_taker") == 'maker', F.col("volume")).otherwise(0)).alias('maker_volume'),
            F.sum("volume").alias("volume"), 
             F.sum("trading_fee").alias("trading_fee"))
    ).alias("b")

    windowSpecZscoreRank24h = Window.partitionBy("a.timestamp").orderBy(
        F.desc("z_score_24h"), F.desc("pnl_24h"), "a.margin_account"
    )
    windowSpecZscoreRank7d = Window.partitionBy("a.timestamp").orderBy(
        F.desc("z_score_7d"), F.desc("pnl_7d"), "a.margin_account"
    )
    windowSpecZscoreRank30d = Window.partitionBy("a.timestamp").orderBy(
        F.desc("z_score_30d"), F.desc("pnl_30d"), "a.margin_account"
    )
    windowSpecZscoreRankAlltime = Window.partitionBy("a.timestamp").orderBy(
        F.desc("z_score_alltime"), F.desc("pnl_alltime"), "a.margin_account"
    )

    df = (
        leaderboard_df.join(
            trades_df,
            on=F.expr(
                "a.timestamp = b.timestamp + interval 1 hour and a.margin_account = b.margin_account and a.authority = b.authority"
            ),
            how="left",
        )  # + interval 1 hour if we want to use start or end of hour
        .join(nft_df, on=["timestamp", "authority"], how="left")
        .join(campaign_df, on=["timestamp", "authority", "margin_account"], how="left")
        .withColumn("campaign_z_score", F.coalesce(F.col("cr.z_score"), F.lit(0)))
        .withColumn("z_multiplier_nft", F.coalesce(F.col("nft.multiplier"), F.lit(1)))
        # .withColumn("volume", F.coalesce("volume", F.lit(0)))
        .withColumn("maker_volume", F.coalesce("maker_volume", F.lit(0)))
        .withColumn("taker_volume", F.coalesce("taker_volume", F.lit(0)))
        .withColumn(
            "z_multiplier",
            F.when(
                F.col("pnl_24h_rank") <= 100, 2 - (F.col("pnl_24h_rank") - 1) * 0.01
            ).otherwise(1),
        )
        .withColumn(
            "z_score",
            F.coalesce(
                ((F.col("z_multiplier") * F.col("z_multiplier_nft")) * (1.0 * F.col("taker_volume") + 0.2 * F.col("maker_volume")) )
                + F.col("campaign_z_score"),
                F.lit(0),
            ),
        )
        # volume
        .withColumn("maker_volume_24h", F.sum("maker_volume").over(windowSpec24h))
        .withColumn("maker_volume_7d", F.sum("maker_volume").over(windowSpec7d))
        .withColumn("maker_volume_30d", F.sum("maker_volume").over(windowSpec30d))
        .withColumn("maker_volume_alltime", F.sum("maker_volume").over(windowSpecAlltime))
        .withColumn("taker_volume_24h", F.sum("taker_volume").over(windowSpec24h))
        .withColumn("taker_volume_7d", F.sum("taker_volume").over(windowSpec7d))
        .withColumn("taker_volume_30d", F.sum("taker_volume").over(windowSpec30d))
        .withColumn("taker_volume_alltime", F.sum("taker_volume").over(windowSpecAlltime))
        # multiplier
        .withColumn(
            "z_multiplier_24h", F.col("z_multiplier")
        )  # F.avg("z_multiplier").over(windowSpec24h))
        .withColumn("z_multiplier_7d", F.avg("z_multiplier").over(windowSpec7d))
        .withColumn("z_multiplier_30d", F.avg("z_multiplier").over(windowSpec30d))
        .withColumn(
            "z_multiplier_alltime", F.avg("z_multiplier").over(windowSpecAlltime)
        )
        # z-score
        .withColumn("z_score_24h", F.sum("z_score").over(windowSpec24h))
        .withColumn("z_score_7d", F.sum("z_score").over(windowSpec7d))
        .withColumn("z_score_30d", F.sum("z_score").over(windowSpec30d))
        .withColumn("z_score_alltime", F.sum("z_score").over(windowSpecAlltime))
        # rank
        .withColumn("z_score_24h_rank", F.rank().over(windowSpecZscoreRank24h))
        .withColumn("z_score_7d_rank", F.rank().over(windowSpecZscoreRank7d))
        .withColumn("z_score_30d_rank", F.rank().over(windowSpecZscoreRank30d))
        .withColumn("z_score_alltime_rank", F.rank().over(windowSpecZscoreRankAlltime))
        .withColumn(
            "z_score_24h_rank_change",
            -(
                F.col("z_score_24h_rank")
                - F.first("z_score_24h_rank").over(windowSpec24h)
            ),
        )
        .withColumn(
            "z_score_7d_rank_change",
            -(F.col("z_score_7d_rank") - F.first("z_score_7d_rank").over(windowSpec7d)),
        )
        .withColumn(
            "z_score_30d_rank_change",
            -(
                F.col("z_score_30d_rank")
                - F.first("z_score_30d_rank").over(windowSpec30d)
            ),
        )
        .withColumn(
            "z_score_alltime_rank_change",
            -(
                F.col("z_score_alltime_rank")
                - F.first("z_score_alltime_rank").over(windowSpecAlltime)
            ),
        )
        .select(
            "a.timestamp",
            "a.authority",
            "a.margin_account",
            "a.balance",
            "a.unrealized_pnl",
            "a.equity",
            "a.cumulative_pnl",
            "a.deposit_amount_cumsum",
            "a.withdraw_amount_cumsum",
            # metrics
            "pnl_24h",
            "pnl_7d",
            "pnl_30d",
            "pnl_alltime",
            "roi_24h",
            "roi_7d",
            "roi_30d",
            "pnl_24h_rank",
            "pnl_7d_rank",
            "pnl_30d_rank",
            "pnl_alltime_rank",
            "roi_24h_rank",
            "roi_7d_rank",
            "roi_30d_rank",
            "pnl_24h_rank_change",
            "pnl_7d_rank_change",
            "pnl_30d_rank_change",
            "pnl_alltime_rank_change",
            "roi_24h_rank_change",
            "roi_7d_rank_change",
            "roi_30d_rank_change",
            # z-scores
            "maker_volume_24h",
            "maker_volume_7d",
            "maker_volume_30d",
            "maker_volume_alltime",
            "taker_volume_24h",
            "taker_volume_7d",
            "taker_volume_30d",
            "taker_volume_alltime",
            "z_multiplier_24h",
            "z_multiplier_7d",
            "z_multiplier_30d",
            "z_multiplier_alltime",
            "z_multiplier_nft",
            "z_score_24h",
            "z_score_7d",
            "z_score_30d",
            "z_score_alltime",
            "z_score_24h_rank",
            "z_score_7d_rank",
            "z_score_30d_rank",
            "z_score_alltime_rank",
            "z_score_24h_rank_change",
            "z_score_7d_rank_change",
            "z_score_30d_rank_change",
            "z_score_alltime_rank_change",
            "date_",
        )
    )
    return df

# COMMAND ----------

@dlt.table(
    comment="User-30d volume and calculated fee tiers",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "authority",
    },
    path=join(BASE_PATH_TRANSFORMED, FEE_TIERS_TABLE),
)
def fee_tiers():
    windowSpec30dFees = (
        Window.partitionBy("authority")
        .orderBy(F.unix_timestamp("timestamp"))
        .rangeBetween(-days(30), 0)
    )

    windowLatestTimestamp = Window.partitionBy("authority").orderBy(F.desc("timestamp"))

    df = (
        dlt.read("cleaned_ix_trade")
        .withColumn("timestamp", F.date_trunc("hour", "block_time"))
        # We only need the last 30 days for fee tier calcs
        .filter(F.col("timestamp") >= F.date_sub(F.current_date(), 35))
        .groupBy("timestamp", "authority")
        .agg(F.sum("volume").alias("volume"))
        .withColumn("total_volume_30d", F.sum("volume").over(windowSpec30dFees))
        .withColumn(
            "fee_tier",
            F.when(F.col("total_volume_30d") >= 50_000_000, 7)
            .when(F.col("total_volume_30d") >= 20_000_000, 6)
            .when(F.col("total_volume_30d") >= 10_000_000, 5)
            .when(F.col("total_volume_30d") >= 5_000_000, 4)
            .when(F.col("total_volume_30d") >= 1_000_000, 3)
            .when(F.col("total_volume_30d") >= 500_000, 2)
            .when(F.col("total_volume_30d") >= 100_000, 1)
            .otherwise(0),
        )
        .withColumn(
            "fee_multiplier",
            F.when(F.col("fee_tier") == 7, 0.3)
            .when(F.col("fee_tier") == 6, 0.4)
            .when(F.col("fee_tier") == 5, 0.5)
            .when(F.col("fee_tier") == 4, 0.6)
            .when(F.col("fee_tier") == 3, 0.7)
            .when(F.col("fee_tier") == 2, 0.8)
            .when(F.col("fee_tier") == 1, 0.9)
            .otherwise(1),
        )
        .select(
            "timestamp",
            "authority",
            "volume",
            "total_volume_30d",
            "fee_tier",
            "fee_multiplier",
        )
    )

    # Only store the latest timestamp for each authority
    # We don't need the historical fee tier for each authority
    df_latest = (
        df.withColumn("row_num", F.row_number().over(windowLatestTimestamp))
        .where(F.col("row_num") == 1)
        .drop("row_num")
    )

    return df_latest
