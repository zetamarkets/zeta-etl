# Databricks notebook source
dbutils.widgets.dropdown("network", "devnet", ["devnet", "mainnet"], "Network")
# NETWORK = dbutils.widgets.get("network")
NETWORK = spark.conf.get("pipeline.network")

# COMMAND ----------

from pyspark.sql import functions as F, Window as W, types as T
from os.path import join
from itertools import chain
import dlt

# COMMAND ----------

ORDERBOOK_TABLE = "orderbook-snapshotter-py"
MARKET_MAKER_TABLE = "market-maker-program"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # ETL

# COMMAND ----------

S3_BUCKET_LANDED = f"zetadex-{NETWORK}-landing"
BASE_PATH_LANDED = join("/mnt", S3_BUCKET_LANDED)
S3_BUCKET_TRANSFORMED = f"zetadex-{NETWORK}"
BASE_PATH_TRANSFORMED = join("/mnt", S3_BUCKET_TRANSFORMED)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orderbook Snapshotting

# COMMAND ----------

# DBTITLE 1,Bronze
orderbook_snapshot_schema = """
asset string,
local_timestamp timestamp,
exchange_timestamp timestamp,
midpoint double,
mark_price double,
bids array<
    struct<
        price double,
        size double,
        open_order_address string,
        authority string
    >
>,
asks array<
    struct<
        price double,
        size double,
        open_order_address string,
        authority string
    >
>
"""


@dlt.table(
    comment="Raw data for orderbook snapshots",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "local_timestamp",
    },
    path=join(BASE_PATH_TRANSFORMED, ORDERBOOK_TABLE, "raw"),
    schema=orderbook_snapshot_schema,
)
def raw_orderbook_snapshot():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.region", "ap-southeast-1")
        .option("cloudFiles.includeExistingFiles", True)
        .option("cloudFiles.useNotifications", True)
        .option("partitionColumns", "year,month,day,hour")
        .schema(orderbook_snapshot_schema)
        .load(join(BASE_PATH_LANDED, ORDERBOOK_TABLE, "data"))
    )

# COMMAND ----------

# DBTITLE 1,Silver
# Bids
@dlt.view(
    comment="Cleaned data for bids",
)
def cleaned_orderbook_bids():
    market_maker_labels_df = spark.table(f"zetadex_mainnet.pubkey_label").alias("mm")
    return (
        dlt.read_stream("raw_orderbook_snapshot")
        .withWatermark("local_timestamp", "1 minutes")
        .withColumn("bid", F.explode("bids"))
        .join(
            market_maker_labels_df,
            (F.col("bid.authority") == market_maker_labels_df.pub_key),
            how="inner",
        )
        .select(
            "asset",
            "mm.label",
            "mm.organisation",
            "local_timestamp",
            "exchange_timestamp",
            "mark_price",
            "midpoint",
            "bid.price",
            (
                F.abs(F.col("bid.price") - F.col("midpoint"))
                / F.col("midpoint")
                * 10000
            ).alias("bps_from_mid"),
            "bid.size",
            (F.col("bid.price") * F.col("bid.size")).alias("depth"),
            "bid.open_order_address",
            "bid.authority",
        )
        .withColumn(
            "spread_group",
            F.when(F.col("bps_from_mid") <= 2.50, "(0) <= 2.5 bps")
            .when(
                (F.col("bps_from_mid") > 2.50) & (F.col("bps_from_mid") <= 5.00),
                "(1) 2.5 bps < x <= 5.0 bps",
            )
            .when(
                (F.col("bps_from_mid") > 5.00) & (F.col("bps_from_mid") <= 10.00),
                "(2) 5.0 bps < x <= 10.0 bps",
            )
            .when(
                (F.col("bps_from_mid") > 10.00) & (F.col("bps_from_mid") <= 25.00),
                "(3) 10.0 bps < x <= 25.0 bps",
            )
            .when(
                (F.col("bps_from_mid") > 25.00) & (F.col("bps_from_mid") <= 50.00),
                "(4) 25.0 bps < x <= 50.0 bps",
            )
            .when((F.col("bps_from_mid") > 50.00), "(5) > 50.0 bps"),
        )
        .withColumn("side", F.lit("bid"))
    )


# Asks
@dlt.view(
    comment="Cleaned data for asks",
)
def cleaned_orderbook_asks():
    market_maker_labels_df = spark.table(f"zetadex_mainnet.pubkey_label").alias("mm")
    return (
        dlt.read_stream("raw_orderbook_snapshot")
        .withWatermark("local_timestamp", "1 minutes")
        .withColumn("ask", F.explode("asks"))
        .join(
            market_maker_labels_df,
            (F.col("ask.authority") == market_maker_labels_df.pub_key),
            how="inner",
        )
        .select(
            "asset",
            "mm.label",
            "mm.organisation",
            "local_timestamp",
            "exchange_timestamp",
            "mark_price",
            "midpoint",
            "ask.price",
            (
                F.abs(F.col("ask.price") - F.col("midpoint"))
                / F.col("midpoint")
                * 10000
            ).alias("bps_from_mid"),
            "ask.size",
            (F.col("ask.price") * F.col("ask.size")).alias("depth"),
            "ask.open_order_address",
            "ask.authority",
        )
        .withColumn(
            "spread_group",
            F.when(F.col("bps_from_mid") <= 2.50, "(0) <= 2.5 bps")
            .when(
                (F.col("bps_from_mid") > 2.50) & (F.col("bps_from_mid") <= 5.00),
                "(1) 2.5 bps < x <= 5.0 bps",
            )
            .when(
                (F.col("bps_from_mid") > 5.00) & (F.col("bps_from_mid") <= 10.00),
                "(2) 5.0 bps < x <= 10.0 bps",
            )
            .when(
                (F.col("bps_from_mid") > 10.00) & (F.col("bps_from_mid") <= 25.00),
                "(3) 10.0 bps < x <= 25.0 bps",
            )
            .when(
                (F.col("bps_from_mid") > 25.00) & (F.col("bps_from_mid") <= 50.00),
                "(4) 25.0 bps < x <= 50.0 bps",
            )
            .when((F.col("bps_from_mid") > 50.00), "(5) > 50.0 bps"),
        )
        .withColumn("side", F.lit("ask"))
    )


# All
@dlt.table(
    comment="Cleaned data for all orders",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "local_timestamp,asset",
    },
    path=join(BASE_PATH_TRANSFORMED, ORDERBOOK_TABLE, "cleaned-orderbook-all"),
)
def cleaned_orderbook_all():
    bids = dlt.read_stream("cleaned_orderbook_bids").withWatermark(
        "local_timestamp", "1 minute"
    )
    asks = dlt.read_stream("cleaned_orderbook_asks").withWatermark(
        "local_timestamp", "1 minute"
    )
    return bids.unionAll(asks)

# COMMAND ----------

# DBTITLE 1,Gold
@dlt.table(
    comment="Aggregated orderbook data by market, market maker, spread group, and time",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "timestamp,asset",
    },
    path=join(BASE_PATH_TRANSFORMED, ORDERBOOK_TABLE, "agg-orderbook-all"),
)
def agg_orderbook_all():
    return (
        dlt.read_stream("cleaned_orderbook_all")
        .withWatermark("local_timestamp", "1 minute")
        .groupBy(
            F.date_trunc("hour", "local_timestamp").alias("timestamp"),
            "asset",
            "label",
            "spread_group",
            "side",
        )
        .agg(
            F.avg("depth").alias("total_liquidity"),  # avg over hour
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Market Maker Program

# COMMAND ----------

from datetime import datetime

### Constants of MM Program
# Define the constants of the MM program
# Needs to be a Friday 8am UTC to align with the epochs used for trade calcs (in v1 rewards)
PROGRAM_START_DATE = "2024-02-18 00:00:00"

# Test with weekly epochs
DAYS_IN_EPOCH = 7
SECONDS_IN_EPOCH = 24 * 3600 * DAYS_IN_EPOCH
SAMPLES_IN_EPOCH = SECONDS_IN_EPOCH / 60  # 60s samples

# Spread / depth requirements
CORE_SPREAD_BPS = 20.0
OTHER_SPREAD_BPS = 40.0
CORE_DEPTH = 5000.0
OTHER_DEPTH = 1000.0

# Filtering constants
VOLUME_THRESHOLD_PCT = 0.01  # 1% of maker volume on a given market

# Exponents
D_EXP = 0.15
MAKER_VOLUME_EXP = 0.85
UPTIME_EXP = 5

# Reward weights
SOL_WEIGHT = 0.25
MAJORS_WEIGHT = 0.1  # BTC, ETH
REMAINING_WEIGHT = 1 - (SOL_WEIGHT + MAJORS_WEIGHT * 2)


@dlt.view()
def mm_market_params():
    # Define the schema
    schema = T.StructType(
        [
            T.StructField("asset", T.StringType(), False),
            T.StructField("min_depth", T.FloatType(), False),
            T.StructField("max_spread_bps", T.FloatType(), False),
            T.StructField("min_volume_pct", T.FloatType(), False),
            T.StructField("listing_date", T.TimestampType(), False),
            T.StructField("reward_weighting", T.FloatType(), False),
        ]
    )

    market_params = [
        # asset, min_depth, max_spread, min_volume_pct, reward_weighting, listing_date
        [
            "SOL",
            CORE_DEPTH,
            CORE_SPREAD_BPS,
            VOLUME_THRESHOLD_PCT,
            datetime.strptime(PROGRAM_START_DATE, "%Y-%m-%d %H:%M:%S"),
        ],
        [
            "BTC",
            CORE_DEPTH,
            CORE_SPREAD_BPS,
            VOLUME_THRESHOLD_PCT,
            datetime.strptime(PROGRAM_START_DATE, "%Y-%m-%d %H:%M:%S"),
        ],
        [
            "ETH",
            CORE_DEPTH,
            CORE_SPREAD_BPS,
            VOLUME_THRESHOLD_PCT,
            datetime.strptime(PROGRAM_START_DATE, "%Y-%m-%d %H:%M:%S"),
        ],
        [
            "APT",
            OTHER_DEPTH,
            OTHER_SPREAD_BPS,
            VOLUME_THRESHOLD_PCT,
            datetime.strptime(PROGRAM_START_DATE, "%Y-%m-%d %H:%M:%S"),
        ],
        [
            "ARB",
            OTHER_DEPTH,
            OTHER_SPREAD_BPS,
            VOLUME_THRESHOLD_PCT,
            datetime.strptime(PROGRAM_START_DATE, "%Y-%m-%d %H:%M:%S"),
        ],
        [
            "BNB",
            OTHER_DEPTH,
            OTHER_SPREAD_BPS,
            VOLUME_THRESHOLD_PCT,
            datetime.strptime(PROGRAM_START_DATE, "%Y-%m-%d %H:%M:%S"),
        ],
        [
            "JTO",
            OTHER_DEPTH,
            OTHER_SPREAD_BPS,
            VOLUME_THRESHOLD_PCT,
            datetime.strptime(PROGRAM_START_DATE, "%Y-%m-%d %H:%M:%S"),
        ],
        [
            "ONEMBONK",
            OTHER_DEPTH,
            OTHER_SPREAD_BPS,
            VOLUME_THRESHOLD_PCT,
            datetime.strptime(PROGRAM_START_DATE, "%Y-%m-%d %H:%M:%S"),
        ],
        [
            "PYTH",
            OTHER_DEPTH,
            OTHER_SPREAD_BPS,
            VOLUME_THRESHOLD_PCT,
            datetime.strptime(PROGRAM_START_DATE, "%Y-%m-%d %H:%M:%S"),
        ],
        [
            "SEI",
            OTHER_DEPTH,
            OTHER_SPREAD_BPS,
            VOLUME_THRESHOLD_PCT,
            datetime.strptime(PROGRAM_START_DATE, "%Y-%m-%d %H:%M:%S"),
        ],
        [
            "TIA",
            OTHER_DEPTH,
            OTHER_SPREAD_BPS,
            VOLUME_THRESHOLD_PCT,
            datetime.strptime(PROGRAM_START_DATE, "%Y-%m-%d %H:%M:%S"),
        ],
        [
            "JUP",
            OTHER_DEPTH,
            OTHER_SPREAD_BPS,
            VOLUME_THRESHOLD_PCT,
            datetime.strptime("2024-01-31 16:57:45", "%Y-%m-%d %H:%M:%S"),
        ],
        [
            "DYM",
            OTHER_DEPTH,
            OTHER_SPREAD_BPS,
            VOLUME_THRESHOLD_PCT,
            datetime.strptime("2024-02-13 09:30:57", "%Y-%m-%d %H:%M:%S"),
        ],
        # Add more rows as needed
    ]
    # Programatically create the weightings based on number of assets
    market_params_final = []
    for m in market_params:
        m_final = m
        if m[0] == "SOL":
            m_final.append(SOL_WEIGHT)
        elif m[0] in ["BTC", "ETH"]:
            m_final.append(MAJORS_WEIGHT)
        else:
            m_final.append(round(REMAINING_WEIGHT / (len(market_params) - 3), 5))
        market_params_final.append(m_final)
    # assert that the sum of weightings is 1.0
    assert abs(sum([m[-1] for m in market_params_final]) - 1.0) < 0.0001

    # Create a DataFrame
    market_params_df = spark.createDataFrame(market_params_final, schema)

    # Create a temporary view
    # market_params_df.createOrReplaceTempView("mm_market_params")
    # market_params_df.write.saveAsTable("zetadex_mainnet.mm_market_params")
    return market_params_df

# COMMAND ----------

@dlt.table(
    comment="Orderbook level data with epoch information",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "timestamp,epoch_start,asset,authority",
    },
    path=join(BASE_PATH_TRANSFORMED, MARKET_MAKER_TABLE, "cleaned-mm-orderbook"),
)
def cleaned_mm_orderbook():
    base_df = (
        dlt.read_stream("cleaned_orderbook_all")
        .withColumnRenamed("exchange_timestamp", "timestamp")
        .withWatermark("timestamp", "1 minutes")
        .filter(F.col("timestamp") >= F.lit(PROGRAM_START_DATE))
        .withColumn(
            "epoch_number",
            (
                (
                    F.unix_timestamp("timestamp")
                    - F.unix_timestamp(F.lit(PROGRAM_START_DATE))
                )
                / F.lit(SECONDS_IN_EPOCH)
            ).cast("int"),
        )
        .withColumn(
            "epoch_start",
            F.to_timestamp(
                F.unix_timestamp(F.lit(PROGRAM_START_DATE))
                + F.col("epoch_number") * F.lit(SECONDS_IN_EPOCH)
            ),
        )
        .drop("spread_group")
    )
    return base_df

# COMMAND ----------

@dlt.table(
    comment="Orderbook level data with epoch information",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "timestamp,epoch_start,asset,authority",
    },
    path=join(BASE_PATH_TRANSFORMED, MARKET_MAKER_TABLE, "cleaned-mm-scores"),
)
def cleaned_mm_scores():
    market_params_df = dlt.read("mm_market_params")
    df = (
        dlt.read("cleaned_mm_orderbook")
        # .withWatermark("timestamp", "1 minutes")
        .join(market_params_df, on=["asset"], how="left")
        # filter out based on thresholds
        .filter(F.col("bps_from_mid") <= F.col("max_spread_bps"))
        # Need to add 1 bps to spread_bps if doing square distance in bps.
        .withColumn(
            "d_score", F.col("depth") / ((F.col("bps_from_mid") + F.lit(0.0001)) ** 2)
        )
    )

    level_agg_df = (
        df.groupBy(
            "timestamp",
            "epoch_number",
            "epoch_start",
            "min_depth",
            "asset",
            "side",
            "authority",
        )
        .agg(
            F.first("midpoint").alias("midpoint"),
            F.first("mark_price").alias("mark_price"),
            (F.sum(F.col("price") * F.col("size")) / F.sum("size")).alias("vwap_price"),
            (F.sum(F.col("bps_from_mid") * F.col("size")) / F.sum("size")).alias(
                "vwap_spread"
            ),
            F.sum("depth").alias("depth"),
            F.sum("d_score").alias("d_score"),
            F.first("reward_weighting").alias("reward_weighting"),
            F.first("listing_date").alias("listing_date"),
        )
        .filter(F.col("depth") >= F.col("min_depth"))
    )

    bids_df = level_agg_df.filter("side == 'bid'")
    asks_df = level_agg_df.filter("side == 'ask'")

    combined_bid_ask_df = (
        bids_df.alias("b")
        .join(
            asks_df.alias("a"),
            on=["timestamp", "asset", "authority"],
            how="inner",
        )
        .select(
            "a.timestamp",
            "a.epoch_number",
            "a.epoch_start",
            "a.asset",
            "a.authority",
            F.col("a.d_score").alias("d_score_ask"),
            F.col("b.d_score").alias("d_score_bid"),
            "a.reward_weighting",
            "a.listing_date",
        )
        .withColumn("d_score_min", F.least("d_score_bid", "d_score_ask"))
    )
    return combined_bid_ask_df

# COMMAND ----------

@dlt.view(
    comment="Maker volume data for whitelisted MMs",
)
def agg_mm_maker_volume_1h():
    volume_df = (
        spark.table("zetadex_mainnet_tx.cleaned_ix_trade")
        .alias("t")
        .join(
            spark.table("zetadex_mainnet.pubkey_label").alias("l"),
            F.expr("t.authority == l.pub_key"),
            how="inner",
        )  # filter to just MMs
        .filter("maker_taker == 'maker'")
        .groupBy(F.date_trunc("hour", "block_time").alias("timestamp"), "authority")
        .agg(F.sum("volume").alias("maker_volume"))
    )
    return volume_df

# COMMAND ----------

@dlt.table(
    comment="Orderbook level data with epoch information",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "timestamp,epoch_start,asset,authority",
    },
    path=join(BASE_PATH_TRANSFORMED, MARKET_MAKER_TABLE, "agg-mm-scores-1h"),
)
def agg_mm_scores_1h():
    epoch_asset_authority_window = (
        W.partitionBy("epoch_start", "asset", "authority")
        .orderBy("timestamp")
        .rowsBetween(W.unboundedPreceding, W.currentRow)
    )

    # TODO: filter volume min_volume_pct
    time_agg_df = (
        dlt.read(
            "cleaned_mm_scores"
        )  # streaming doesn't support non-time-based window functions
        # .withWatermark("timestamp", "1 minutes")
        .groupBy(
            # F.window("timestamp", "1 hour"),
            F.date_trunc("hour", "timestamp").alias("timestamp"),
            "epoch_number",
            "epoch_start",
            "asset",
            "authority",
        )
        .agg(
            F.sum("d_score_min").alias("d_score_min"),
            F.count(F.when(F.col("d_score_min") > 0, 1)).alias("uptime_instances"),
            F.first("reward_weighting").alias("reward_weighting"),
            F.first("listing_date").alias("listing_date"),
        )
        # .withColumn("timestamp", F.col("window.start"))
        .withColumn(
            "d_score_cumsum", F.sum("d_score_min").over(epoch_asset_authority_window)
        )
        .withColumn(
            "uptime_instances_cumsum",
            F.sum("uptime_instances").over(epoch_asset_authority_window),
        )
        .withColumn(
            "epoch_samples_approx_cumsum",
            (
                # F.unix_timestamp("window.end")
                F.unix_timestamp(F.expr("timestamp + interval 1 hour"))
                - F.greatest(
                    F.unix_timestamp("epoch_start"), F.unix_timestamp("listing_date")
                )
            )
            / SECONDS_IN_EPOCH
            * SAMPLES_IN_EPOCH
            + 1,
        )
        .withColumn(
            "uptime",
            F.col("uptime_instances_cumsum") / F.col("epoch_samples_approx_cumsum"),
        )
        # .filter("uptime >= 0.8") # TODO:
        # .filter # TODO: filter out based on min volume %? - perhaps add this to the final payouts table?
    )

    volume_df = dlt.read("agg_mm_maker_volume_1h")

    final_df = (
        time_agg_df.join(volume_df, on=["timestamp", "authority"])
        .withColumn(
            "maker_volume_cumsum",
            F.sum("maker_volume").over(epoch_asset_authority_window),
        )
        .withColumn("Q_depth", F.col("d_score_cumsum") ** D_EXP)
        .withColumn("Q_uptime", F.col("uptime") ** UPTIME_EXP)
        .withColumn("Q_volume", F.col("maker_volume_cumsum") ** MAKER_VOLUME_EXP)
        .withColumn("Q", F.col("Q_depth") * F.col("Q_uptime") * F.col("Q_volume"))
        # .drop("window")
    )
    return final_df

# COMMAND ----------


