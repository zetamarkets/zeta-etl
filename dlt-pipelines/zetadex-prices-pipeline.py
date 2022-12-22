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

TRADES_TABLE = "trades"
PRICES_TABLE = "prices"
REWARDS_TABLE = "rewards"

# COMMAND ----------

# MAGIC %md
# MAGIC # ETL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spot Prices

# COMMAND ----------

coin_prices_schema = """
underlying string,
timestamp timestamp,
price_usd double
"""


@dlt.table(
    comment="Raw data for crypto prices (scraped from CoinGecko)",
    table_properties={
        "quality": "bronze",
    },
    path="/mnt/zetamarkets-market-data/coingecko-prices/raw",
    schema=coin_prices_schema,
)
def raw_coingecko_prices():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.region", "ap-southeast-1")
        .option("cloudFiles.includeExistingFiles", True)
        .option("cloudFiles.useNotifications", True)
        .option("partitionColumns", "date_")
        .schema(coin_prices_schema)
        .load("/mnt/zetamarkets-market-data-landing/coingecko-prices/data")
    )

# COMMAND ----------

@dlt.table(
    comment="Cleaned data for crypto prices",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "timestamp",
    },
    partition_cols=["underlying"],
    path="/mnt/zetamarkets-market-data/coingecko-prices/cleaned",
)
def cleaned_coingecko_prices():
    return (
        dlt.read_stream("raw_coingecko_prices")
        .withWatermark("timestamp", "1 hour")
        .withColumn("date_", F.to_date("timestamp"))
        .withColumn("hour_", F.date_format("timestamp", "HH").cast("int"))
        .dropDuplicates(["underlying", "date_", "hour_"])
    )

# COMMAND ----------

S3_BUCKET_LANDED = f"zetadex-{NETWORK}-landing"
BASE_PATH_LANDED = join("/mnt", S3_BUCKET_LANDED)
S3_BUCKET_TRANSFORMED = f"zetadex-{NETWORK}"
BASE_PATH_TRANSFORMED = join("/mnt", S3_BUCKET_TRANSFORMED)

# COMMAND ----------

spark.conf.set("params.TAKER_BONUS_PER_EPOCH", 2000)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trades

# COMMAND ----------

# DBTITLE 1,Bronze
trades_schema = """
seq_num int,
timestamp timestamp,
owner_pub_key string,
underlying string,
expiry_series_index int,
expiry_timestamp timestamp,
market_index int,
strike double,
kind string,
is_maker boolean,
is_bid boolean,
price double,
size double,
order_id string,
client_order_id string,
year string,
month string,
day string,
hour string
"""


@dlt.table(
    comment="Raw data for trades",
    table_properties={
        "quality": "bronze",
    },
    path=join(BASE_PATH_TRANSFORMED, TRADES_TABLE, "raw"),
    schema=trades_schema,
)
def raw_trades():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.region", "ap-southeast-1")
        .option("cloudFiles.includeExistingFiles", True)
        .option("cloudFiles.useNotifications", True)
        .option("partitionColumns", "year,month,day,hour")
        .schema(trades_schema)
        .load(join(BASE_PATH_LANDED, TRADES_TABLE, "data"))
    )

# COMMAND ----------

# DBTITLE 1,Silver
@dlt.table(
    comment="Cleaned data for trades",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "timestamp",
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, TRADES_TABLE, "cleaned"),
)
def cleaned_trades():
    prices_df = (
        dlt.read_stream("cleaned_coingecko_prices")
        .withColumn("date_hour", F.date_trunc("hour", "timestamp"))
        .withWatermark("date_hour", "1 hour")
    )
    return (
        dlt.read_stream("raw_trades")
        .withWatermark("timestamp", "1 hour")
        .dropDuplicates(["seq_num", "market_index", "expiry_timestamp", "underlying"])
        .withColumn("side", F.when(F.col("is_bid"), "bid").otherwise("ask"))
        .withColumn("premium", F.col("price") * F.col("size"))
        .join(
            prices_df,
            F.expr(
                """
                raw_trades.underlying = cleaned_coingecko_prices.underlying AND
                raw_trades.timestamp >= date_hour and raw_trades.timestamp < date_hour + interval 1 hour
                """
            ),
            how="left",
        )
        .withColumn("notional", F.col("price_usd") * F.col("size"))
        .withColumn(
            "approx_fees",
            F.when(
                ~F.col("is_maker"),
                F.when(
                    F.col("kind").isin("call", "put"),
                    F.least(
                        0.3 / 100 * F.col("notional"),
                        0.1 * F.col("premium"),  # min(30bps notional, 10% premium)
                    ),
                ).otherwise(0.05 / 100 * F.col("notional")), # 5bps
            ).otherwise(0),
        )
        .select(
            "raw_trades.timestamp",
            "raw_trades.underlying",
            "expiry_timestamp",
            "strike",
            "kind",
            "side",
            "is_maker",
            "owner_pub_key",
            "price",
            "size",
            "premium",
            "price_usd",
            "notional",
            "approx_fees",
            "market_index",
            "seq_num",
            "order_id",
            "client_order_id",
        )
        .withColumn("date_", F.to_date("timestamp"))
        .withColumn("hour_", F.date_format("timestamp", "HH").cast("int"))
    )

# COMMAND ----------

# DBTITLE 1,Gold
@dlt.table(
    comment="Market-hourly aggregated data for trades",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "timestamp",
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, TRADES_TABLE, "agg-m1h"),
)
def agg_trades_market_1h():
    return (
        dlt.read_stream("cleaned_trades")
        .where("is_maker")
        .withWatermark("timestamp", "1 hour")
        .groupBy(
            F.window("timestamp", "1 hour").alias("timestamp_window"),
            "underlying",
            "expiry_timestamp",
            "strike",
            "kind",
            "side",
        )
        .agg(
            F.count("*").alias("trades_count"),
            F.sum("size").alias("volume"),
            F.sum("notional").alias("notional_volume"),
            F.sum("premium").alias("premium_sum"),
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
    comment="Hourly aggregated data for trades",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "timestamp",
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, TRADES_TABLE, "agg-1h"),
)
def agg_trades_1h():
    return (
        dlt.read_stream("cleaned_trades")
        .where("is_maker")
        .withWatermark("timestamp", "1 hour")
        .groupBy(F.window("timestamp", "1 hour").alias("timestamp_window"))
        .agg(
            F.count("*").alias("trades_count"),
            F.sum("size").alias("volume"),
            F.sum("notional").alias("notional_volume"),
            F.sum("premium").alias("premium_sum"),
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
# MAGIC ## Prices

# COMMAND ----------

# DBTITLE 1,Bronze
prices_schema = """
timestamp timestamp,
expiry_timestamp timestamp,
underlying string,
strike double,
kind string,
market_index int,
expiry_series_index int,
theo double,
delta double,
vega double,
sigma double,
open_interest double,
perp_latest_midpoint double,
perp_funding_delta double,
perp_latest_funding_rate double,
slot long,
year string,
month string,
day string,
hour string
"""


@dlt.table(
    comment="Raw data for platform mark prices, greeks and OI",
    table_properties={
        "quality": "bronze",
    },
    path=join(BASE_PATH_TRANSFORMED, PRICES_TABLE, "raw"),
    schema=prices_schema,
)
def raw_prices():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.region", "ap-southeast-1")
        .option("cloudFiles.includeExistingFiles", True)
        .option("cloudFiles.useNotifications", True)
        .option("partitionColumns", "year,month,day,hour")
        .schema(prices_schema)
        .load(join(BASE_PATH_LANDED, PRICES_TABLE, "data"))
    )

# COMMAND ----------

# DBTITLE 1,Silver
@dlt.table(
    comment="Cleaned data for platform mark prices, greeks and OI",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "timestamp",
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, PRICES_TABLE, "cleaned"),
)
def cleaned_prices():
    prices_df = (
        dlt.read("cleaned_coingecko_prices")
        .withColumn("date_hour", F.date_trunc("hour", "timestamp"))
        .withWatermark("date_hour", "1 hour")
    )
    return (
        dlt.read_stream("raw_prices")
        .withWatermark("timestamp", "1 minute")
        #      .dropDuplicates(["slot", "market_index", "expiry_timestamp", "underlying"])
        .withColumn("open_interest_usd", F.col("open_interest") * F.col("theo"))
        .join(
            prices_df,
            F.expr(
                """
            raw_prices.underlying = cleaned_coingecko_prices.underlying AND
            raw_prices.timestamp >= date_hour and raw_prices.timestamp < date_hour + interval 1 hour
            """
            ),
            how="left",
        )
        .withColumn(
            "open_interest_notional", F.col("open_interest") * F.col("price_usd")
        )
        .select(
            "raw_prices.timestamp",
            "raw_prices.underlying",
            "expiry_timestamp",
            "strike",
            "kind",
            "market_index",
            "theo",
            "delta",
            "vega",
            "sigma",
            "open_interest",
            "open_interest_usd",
            "price_usd",
            "open_interest_notional",
            "perp_latest_midpoint",
            "perp_funding_delta",
            "perp_latest_funding_rate",
            "slot",
        )
        .withColumn("date_", F.to_date("timestamp"))
        .withColumn("hour_", F.date_format("timestamp", "HH").cast("int"))
    )

# COMMAND ----------

# DBTITLE 1,Gold
@dlt.table(
    comment="Market-hourly aggregated data for prices, greeks",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "timestamp",
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, PRICES_TABLE, "agg-m1h"),
)
def agg_prices_market_1h():
    return (
        dlt.read_stream("cleaned_prices")
        .withWatermark("timestamp", "1 hour")
        .groupBy(
            F.window("timestamp", "1 hour").alias("timestamp_window"),
            "underlying",
            "expiry_timestamp",
            "strike",
            "kind",
        )
        .agg(
            F.first("theo", ignorenulls=True).alias("theo"),
            F.first("delta", ignorenulls=True).alias("delta"),
            F.first("vega", ignorenulls=True).alias("vega"),
            F.first("sigma", ignorenulls=True).alias("sigma"),
            F.first("open_interest", ignorenulls=True).alias("open_interest"),
            F.first("open_interest_usd", ignorenulls=True).alias("open_interest_usd"),
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
        .withColumn("timestamp", F.col("timestamp_window.end"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trading Rewards

# COMMAND ----------

# DBTITLE 1,Silver
@dlt.view(comment="Trade data used for rewards")
def cleaned_trades_rewards_v():
    df = (
        dlt.read("cleaned_trades")
        .withColumn(
            "epoch", F.date_trunc("week", F.expr("timestamp - interval 104 hours"))
        )
        .withColumn("epoch", F.expr("epoch + interval 104 hours"))
        .withColumn(
            "market",
            F.when(
                F.col("kind") == "perp",
                F.concat("underlying", F.lit("-"), F.upper("kind")),
            )
            .when(
                F.col("kind") == "future",
                F.concat(
                    "underlying",
                    F.lit("-"),
                    F.upper(F.date_format("expiry_timestamp", "dMMMyy")),
                ),
            )
            .otherwise(
                F.concat(
                    "underlying",
                    F.lit("-"),
                    F.upper(F.date_format("expiry_timestamp", "dMMMyy")),
                    F.lit("-"),
                    F.col("strike").cast("int"),
                    F.lit("-"),
                    F.substring(F.upper("kind"), 0, 1),
                )
            ),
        )  # standardising to Deribit naming convention
        .withColumnRenamed("owner_pub_key", "user")
        .withColumn(
            "maker_taker", F.when(F.col("is_maker"), "maker").otherwise("taker")
        )
        .groupBy("epoch", "kind", "market", "user", "maker_taker")
        .agg(
            F.sum("notional").alias("volume"),
            F.sum("approx_fees").alias("fees"),
        )
    )
    return df

# COMMAND ----------

# DBTITLE 1,Gold
@dlt.table(
    comment="Maker rewards by epoch-user-market",
    table_properties={"quality": "gold", "pipelines.autoOptimize.zOrderCols": "user"},
    partition_cols=["epoch"],
    path=join(BASE_PATH_TRANSFORMED, REWARDS_TABLE, "agg-maker-epoch-user-market"),
)
def agg_maker_rewards_epoch_user_market():
    cleaned_trades_rewards_v = dlt.read("cleaned_trades_rewards_v")

    w_market = Window.partitionBy("epoch", "market").orderBy(F.desc("volume"))
    w_user = Window.partitionBy("epoch", "user")
    w = Window.partitionBy("epoch")

    labels = spark.table("zetadex_mainnet.pubkey_label")
    maker_rewards = (
        cleaned_trades_rewards_v.filter("maker_taker == 'maker'")
        .filter(F.col("kind").isin(["perp", "future"]))  # todo options later
        .filter("epoch >= '2022-11-04'")
        .filter(
            "!(epoch between '2022-11-11 08' and '2022-12-09 08')"
        )  # exclude platform downtime
        .join(
            labels, F.col("user") == F.col("pub_key"), how="inner"
        )  # whitelisted MMs only
        .withColumn("maker_market_volume_rank", F.rank().over(w_market))
        .withColumn(
            "maker_volume_share", F.sum("volume").over(w_user) / F.sum("volume").over(w)
        )
        .withColumnRenamed("volume", "maker_volume")
        .withColumn(
            "maker_tier",
            F.when(F.col("maker_volume_share") >= 0.15, 3)
            .when(F.col("maker_volume_share") >= 0.1, 2)
            .when(F.col("maker_volume_share") >= 0.05, 1)
            .otherwise(0),
        )
        .withColumn(
            "maker_rebate",
            F.when(F.col("maker_tier") == 3, 0.03 / 100 * F.col("maker_volume"))
            .when(F.col("maker_tier") == 2, 0.02 / 100 * F.col("maker_volume"))
            .when(F.col("maker_tier") == 1, 0.01 / 100 * F.col("maker_volume"))
            .otherwise(0),
        )
        .withColumn(
            "maker_bonus",
            F.when(
                (F.col("maker_market_volume_rank") == 1) & (F.col("kind") == "perp"),
                1000,
            )
            .when(
                (F.col("maker_market_volume_rank") == 1) & (F.col("kind") == "future"),
                250,
            )
            .otherwise(0),
        )
        .select(
            "epoch",
            "market",
            "user",
            "maker_volume",
            "maker_tier",
            "maker_rebate",
            "maker_bonus",
        )
    )
    return maker_rewards


@dlt.table(
    comment="Taker rewards by epoch-user-market",
    table_properties={"quality": "gold", "pipelines.autoOptimize.zOrderCols": "user"},
    partition_cols=["epoch"],
    path=join(BASE_PATH_TRANSFORMED, REWARDS_TABLE, "agg-taker-epoch-user-market"),
)
def agg_taker_rewards_epoch_user_market():
    cleaned_trades_rewards_v = dlt.read("cleaned_trades_rewards_v")

    w_user = Window.partitionBy("epoch", "user")
    w = Window.partitionBy("epoch")

    taker_rewards = (
        cleaned_trades_rewards_v.filter("maker_taker == 'taker'")
        .filter(F.col("kind").isin(["perp", "future"]))  # todo options later
        .filter("epoch >= '2022-11-04'")
        .filter(
            "!(epoch between '2022-11-11 08' and '2022-12-09 08')"
        )  # exclude platform downtime
        .withColumnRenamed("volume", "taker_volume")
        .withColumnRenamed("fees", "taker_fees")
        .withColumn(
            "taker_fee_share",
            F.sum("taker_fees").over(w_user) / F.sum("taker_fees").over(w),
        )
        .withColumn(
            "taker_bonus",
            F.least("taker_fee_share", F.lit(0.1))
            * float(spark.conf.get("params.TAKER_BONUS_PER_EPOCH"))
            / F.count("market").over(w_user),
        )
        .select("epoch", "market", "user", "taker_volume", "taker_fees", "taker_bonus")
    )

    return taker_rewards


@dlt.table(
    comment="Maker rewards by epoch-user",
    table_properties={"quality": "gold", "pipelines.autoOptimize.zOrderCols": "user"},
    partition_cols=["epoch"],
    path=join(BASE_PATH_TRANSFORMED, REWARDS_TABLE, "agg-maker-epoch-user"),
)
def agg_maker_rewards_epoch_user():
    w_cumsum = (
        Window.partitionBy("user")
        .orderBy("epoch")
        .rangeBetween(Window.unboundedPreceding, 0)
    )
    df = (
        dlt.read("agg_maker_rewards_epoch_user_market")
        .groupBy("epoch", "user")
        .agg(
            F.sum("maker_volume").alias("maker_volume"),
            F.max("maker_tier").alias("maker_tier"),
            F.sum("maker_rebate").alias("maker_rebate"),
            F.sum("maker_bonus").alias("maker_bonus"),
        )
        .select(
            "*",
            F.sum("maker_volume").over(w_cumsum).alias("maker_volume_cumsum"),
            F.sum("maker_rebate").over(w_cumsum).alias("maker_rebate_cumsum"),
            F.sum("maker_bonus").over(w_cumsum).alias("maker_bonus_cumsum"),
            (
                F.sum("maker_rebate").over(w_cumsum)
                + F.sum("maker_bonus").over(w_cumsum)
            ).alias("maker_total_rewards_cumsum"),
        )
    )
    return df


@dlt.table(
    comment="Taker rewards by epoch-user",
    table_properties={"quality": "gold", "pipelines.autoOptimize.zOrderCols": "user"},
    partition_cols=["epoch"],
    path=join(BASE_PATH_TRANSFORMED, REWARDS_TABLE, "agg-taker-epoch-user"),
)
def agg_taker_rewards_epoch_user():
    w_cumsum = (
        Window.partitionBy("user")
        .orderBy("epoch")
        .rangeBetween(Window.unboundedPreceding, 0)
    )
    df = (
        dlt.read("agg_taker_rewards_epoch_user_market")
        .groupBy("epoch", "user")
        .agg(
            F.sum("taker_volume").alias("taker_volume"),
            F.sum("taker_fees").alias("taker_fees"),
            F.sum("taker_bonus").alias("taker_bonus"),
        )
        .select(
            "*",
            F.sum("taker_volume").over(w_cumsum).alias("taker_volume_cumsum"),
            F.sum("taker_fees").over(w_cumsum).alias("taker_fees_cumsum"),
            F.sum("taker_bonus").over(w_cumsum).alias("taker_bonus_cumsum"),
            F.sum("taker_bonus").over(w_cumsum).alias("taker_total_rewards_cumsum"),
        )
    )
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Referral Rewards

# COMMAND ----------

# DBTITLE 1,Gold
# TODO: sort out options referral fees (since not 5bps)
@dlt.table(
    comment="Referrer rewards by epoch-user",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "referrer",
    },
    partition_cols=["epoch"],
    path=join(BASE_PATH_TRANSFORMED, REWARDS_TABLE, "agg-referrer-epoch-user"),
)
def agg_referrer_rewards_epoch_user():
    cleaned_trades_rewards_v = dlt.read("cleaned_trades_rewards_v")
    referrals = spark.table("zetadex_mainnet.cleaned_referrals")

    days = lambda i: i * 86400
    w_referrer_30d = (
        Window.partitionBy("referrer")
        .orderBy(F.unix_timestamp("epoch"))
        .rangeBetween(-days(30), Window.currentRow)
    )
    w_cumsum = (
        Window.partitionBy("referrer")
        .orderBy("epoch")
        .rangeBetween(Window.unboundedPreceding, 0)
    )
    referrer_rewards = (
        cleaned_trades_rewards_v.filter("epoch >= '2022-09-01'")
        #  .filter(F.col("kind").isin(["perp", "future"]))  # todo options later
        .join(
            referrals.alias("r1"),
            [
                cleaned_trades_rewards_v.user == referrals.referral,
                cleaned_trades_rewards_v.epoch >= referrals.timestamp,
            ],
            how="inner",
        )
        .withColumnRenamed("timestamp", "referral_timestamp")
        .withColumn("referral_volume_30d", F.sum("volume").over(w_referrer_30d))
        .groupBy("epoch", "referrer", "alias")
        .agg(
            F.sum("volume").alias("referral_volume"),
            F.sum("referral_volume_30d").alias("referral_volume_30d"),
            F.sum("fees").alias("referral_fees"),
        )
        .join(referrals.alias("r2"), on="referrer", how="left")
        .groupBy(
            "epoch",
            "r1.referrer",
            "r1.alias",
            "referral_volume",
            "referral_volume_30d",
            "referral_fees",
        )
        .agg(
            F.sum(
                (F.col("epoch") >= F.date_trunc("week", "r2.timestamp")).cast("int")
            ).alias("referral_count")
        )
        .withColumn(
            "referrer_tier",
            F.when(
                (F.col("referral_volume_30d") >= 2500000)
                & (F.col("referral_count") >= 15),
                3,
            )
            .when(
                (F.col("referral_volume_30d") >= 1000000)
                & (F.col("referral_count") >= 10),
                2,
            )
            .otherwise(1),
        )
        .withColumn(
            "referrer_fee_rebate",
            F.when(
                F.col("referrer_tier") == 3,
                F.col("referral_fees") * 0.2,  # 50% of 2bps = 1bp
            )
            .when(
                F.col("referrer_tier") == 2,
                F.col("referral_fees") * 0.15,  # 0.75bps
            )
            .when(
                F.col("referrer_tier") == 1,
                F.col("referral_fees") * 0.1,  # 0.5bp
            )
            .otherwise(0),
        )
        .withColumn(
            "referrer_fee_rebate_cumsum", F.sum("referrer_fee_rebate").over(w_cumsum)
        )
        .withColumn(
            "referral_fees_cumsum", F.sum("referral_fees").over(w_cumsum)
        )
        .withColumn(
            "referral_volume_cumsum", F.sum("referral_volume").over(w_cumsum)
        )
    )
    return referrer_rewards


@dlt.table(
    comment="Referee rewards by epoch-user",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "referee",
    },
    partition_cols=["epoch"],
    path=join(BASE_PATH_TRANSFORMED, REWARDS_TABLE, "agg-referee-epoch-user"),
)
def agg_referee_rewards_epoch_user():
    cleaned_trades_rewards_v = dlt.read("cleaned_trades_rewards_v")
    referrals = spark.table("zetadex_mainnet.cleaned_referrals")
    referrer_rewards = dlt.read("agg_referrer_rewards_epoch_user")
    w_cumsum = (
        Window.partitionBy("referee")
        .orderBy("epoch")
        .rangeBetween(Window.unboundedPreceding, 0)
    )
    referee_rewards = (
        cleaned_trades_rewards_v.filter("epoch >= '2022-09-01'")
        .join(
            referrals,
            [
                cleaned_trades_rewards_v.user == referrals.referral,
                cleaned_trades_rewards_v.epoch >= referrals.timestamp,
            ],
            how="inner",
        )
        .withColumnRenamed("alias", "referrer_alias")
        .withColumnRenamed("referral", "referee")
        .groupBy("epoch", "referee", "referrer", "referrer_alias")
        .agg(F.sum("volume").alias("volume"), F.sum("fees").alias("fees"))
        .join(referrer_rewards, on=["epoch", "referrer"], how="left")
        .withColumn(
            "referee_fee_rebate",
            F.when(F.col("referrer_tier") == 3, F.col("fees") * 0.02)  # 5% of 2bps
            .when(F.col("referrer_tier") == 2, F.col("fees") * 0.02)
            .when(F.col("referrer_tier") == 1, F.col("fees") * 0.02)
            .otherwise(0),
        )
        .select(
            "epoch",
            "referee",
            "referrer",
            "referrer_alias",
            "volume",
            "fees",
            "referee_fee_rebate",
            F.sum("referee_fee_rebate").over(w_cumsum).alias("referee_fee_rebate_cumsum"),
            F.sum("fees").over(w_cumsum).alias("fees_cumsum"),
            F.sum("volume").over(w_cumsum).alias("volume_cumsum")
        )
        .orderBy("epoch", "referee")
    )
    return referee_rewards
