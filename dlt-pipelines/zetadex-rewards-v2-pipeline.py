# Databricks notebook source
dbutils.widgets.dropdown("network", "devnet", ["devnet", "mainnet"], "Network")
# NETWORK = dbutils.widgets.get("network")
NETWORK = spark.conf.get("pipeline.network")

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from os.path import join
import dlt

# COMMAND ----------

REWARDS_TABLE = "rewards-v2"

# COMMAND ----------

# MAGIC %md
# MAGIC # ETL

# COMMAND ----------

S3_BUCKET_LANDED = f"zetadex-{NETWORK}-landing"
BASE_PATH_LANDED = join("/mnt", S3_BUCKET_LANDED)
S3_BUCKET_TRANSFORMED = f"zetadex-{NETWORK}"
BASE_PATH_TRANSFORMED = join("/mnt", S3_BUCKET_TRANSFORMED)

# COMMAND ----------

spark.conf.set("params.TAKER_BONUS_PER_EPOCH", 0)
spark.conf.set("params.MAKER_BONUS_PER_EPOCH", 3000)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trading Rewards

# COMMAND ----------

# DBTITLE 1,Silver
@dlt.table(comment="Trade data used for rewards", temporary=True)
def cleaned_trades_rewards():
    df = (
        # dlt.read("cleaned_trades")
        spark.table("zetadex_mainnet_tx.cleaned_ix_trade")
        .select("block_time", "epoch", "asset", "authority", "maker_taker", "volume", "trading_fee")
        .filter(F.col("epoch") >= "2023-04-07T08:00:00")
    )
    return df

# COMMAND ----------

# DBTITLE 1,Gold
@dlt.view(comment="Trade data used for rewards aggregated by epoch")
def agg_trades_rewards_epoch_user_asset_v():
    return (
        dlt.read("cleaned_trades_rewards")
        .groupBy("epoch", "asset", "authority", "maker_taker")
        .agg(
            F.sum("volume").alias("volume"),
            F.sum("trading_fee").alias("trading_fee"),
        )
    )


@dlt.table(
    comment="Maker rewards by epoch-user-asset",
    table_properties={"quality": "gold", "pipelines.autoOptimize.zOrderCols": "authority"},
    # partition_cols=["epoch"],
    path=join(BASE_PATH_TRANSFORMED, REWARDS_TABLE, "agg-maker-epoch-user-asset"),
)
def agg_maker_rewards_epoch_user_asset():
    agg_trades_rewards_epoch_user_asset_v = dlt.read(
        "agg_trades_rewards_epoch_user_asset_v"
    )

    w_asset = Window.partitionBy("epoch", "asset").orderBy(F.desc("volume"))
    w_user = Window.partitionBy("epoch", "authority")
    w = Window.partitionBy("epoch")

    num_assets = agg_trades_rewards_epoch_user_asset_v.groupBy(
        "epoch"
    ).agg(F.countDistinct("asset").alias("num_assets"))

    labels = spark.table("zetadex_mainnet.pubkey_label")
    maker_rewards = (
        agg_trades_rewards_epoch_user_asset_v.filter("maker_taker == 'maker'")
        .join(
            labels, F.col("authority") == F.col("pub_key"), how="inner"
        )  # whitelisted MMs only
        .withColumn("maker_asset_volume_rank", F.rank().over(w_asset))
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
        .join(num_assets, on=["epoch"], how="left")
        .withColumn(
            "maker_bonus",
            F.when(
                (F.col("maker_asset_volume_rank") == 1),
                float(spark.conf.get("params.MAKER_BONUS_PER_EPOCH")) / F.col("num_assets"),
            )
            .otherwise(0),
        )
        .select(
            "epoch",
            "asset",
            "authority",
            "maker_volume",
            "maker_tier",
            "maker_rebate",
            "maker_bonus",
        )
    )
    return maker_rewards


@dlt.table(
    comment="Taker rewards by epoch-user-asset",
    table_properties={"quality": "gold", "pipelines.autoOptimize.zOrderCols": "authority"},
    # partition_cols=["epoch"],
    path=join(BASE_PATH_TRANSFORMED, REWARDS_TABLE, "agg-taker-epoch-user-asset"),
)
def agg_taker_rewards_epoch_user_asset():
    agg_trades_rewards_epoch_user_asset_v = dlt.read(
        "agg_trades_rewards_epoch_user_asset_v"
    )

    w_user = Window.partitionBy("epoch", "authority")
    w = Window.partitionBy("epoch")

    taker_rewards = (
        agg_trades_rewards_epoch_user_asset_v.filter("maker_taker == 'taker'")
        .withColumnRenamed("volume", "taker_volume")
        .withColumnRenamed("trading_fee", "taker_fee")
        .withColumn(
            "taker_fee_share",
            F.sum("taker_fee").over(w_user) / F.sum("taker_fee").over(w),
        )
        .withColumn(
            "taker_bonus",
            F.least("taker_fee_share", F.lit(0.1))
            * float(spark.conf.get("params.TAKER_BONUS_PER_EPOCH"))
            / F.count("asset").over(w_user),
        )
        .select("epoch", "asset", "authority", "taker_volume", "taker_fee", "taker_bonus")
    )

    return taker_rewards


@dlt.table(
    comment="Maker rewards by epoch-user",
    table_properties={"quality": "gold", "pipelines.autoOptimize.zOrderCols": "authority"},
    # partition_cols=["epoch"],
    path=join(BASE_PATH_TRANSFORMED, REWARDS_TABLE, "agg-maker-epoch-user"),
)
def agg_maker_rewards_epoch_user():
    w_cumsum = (
        Window.partitionBy("authority")
        .orderBy("epoch")
        .rangeBetween(Window.unboundedPreceding, 0)
    )
    df = (
        dlt.read("agg_maker_rewards_epoch_user_asset")
        .groupBy("epoch", "authority")
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
    table_properties={"quality": "gold", "pipelines.autoOptimize.zOrderCols": "authority"},
    # partition_cols=["epoch"],
    path=join(BASE_PATH_TRANSFORMED, REWARDS_TABLE, "agg-taker-epoch-user"),
)
def agg_taker_rewards_epoch_user():
    w_cumsum = (
        Window.partitionBy("authority")
        .orderBy("epoch")
        .rangeBetween(Window.unboundedPreceding, 0)
    )
    df = (
        dlt.read("agg_taker_rewards_epoch_user_asset")
        .groupBy("epoch", "authority")
        .agg(
            F.sum("taker_volume").alias("taker_volume"),
            F.sum("taker_fee").alias("taker_fee"),
            F.sum("taker_bonus").alias("taker_bonus"),
        )
        .select(
            "*",
            F.sum("taker_volume").over(w_cumsum).alias("taker_volume_cumsum"),
            F.sum("taker_fee").over(w_cumsum).alias("taker_fee_cumsum"),
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
@dlt.view
def agg_trades_rewards_epoch_referee_referrer_v():
    cleaned_trades_rewards_v = dlt.read("cleaned_trades_rewards")
    referrals = spark.table("zetadex_mainnet.cleaned_referrals")

    return (
        referrals.withColumnRenamed("timestamp", "referral_timestamp")
        .withColumnRenamed("referral", "referee")
        .alias("r1")
        .join(
            cleaned_trades_rewards_v.alias("t").filter("block_time >= '2022-09-02T08'"),
            F.expr(
                """
                t.authority == r1.referee
                and t.block_time >= r1.referral_timestamp
            """
            ),
            how="inner",
        )
        .groupBy("epoch", "referee", "referrer", "alias")
        .agg(
            F.sum("volume").alias("volume"),
            F.sum("trading_fee").alias("trading_fee"),
        )
    )


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
    agg_trades_rewards_epoch_referee_referrer_v = dlt.read(
        "agg_trades_rewards_epoch_referee_referrer_v"
    )
    referrals = spark.table("zetadex_mainnet.cleaned_referrals")

    days = lambda i: i * 86400
    w_referee_referrer_30d = (
        Window.partitionBy("referee", "referrer")
        .orderBy(F.unix_timestamp("epoch"))
        .rangeBetween(-days(30), 0)  # last 30 days
    )
    w_cumsum = (
        Window.partitionBy("referrer")
        .orderBy("epoch")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)  # cumulative
    )

    referrer_rewards = (
        agg_trades_rewards_epoch_referee_referrer_v.alias("r1").withColumn(
            "referral_volume_30d", F.sum("volume").over(w_referee_referrer_30d)
        )
        .groupBy("epoch", "referrer", "alias")
        .agg(
            F.sum("volume").alias("referral_volume"),
            F.sum("referral_volume_30d").alias("referral_volume_30d"),
            F.sum("trading_fee").alias("referral_fee"),
        )
        .join(referrals.alias("r2"), on="referrer", how="left")
        .groupBy(
            "epoch",
            "r1.referrer",
            "r1.alias",
            "referral_volume",
            "referral_volume_30d",
            "referral_fee",
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
                F.col("referral_fee") * 0.2/2,  # 50% of 2bps = 1bp # I've divided all by 2 for new fees
            )
            .when(
                F.col("referrer_tier") == 2,
                F.col("referral_fee") * 0.15/2,  # 0.75bps
            )
            .when(
                F.col("referrer_tier") == 1,
                F.col("referral_fee") * 0.1/2,  # 0.5bp
            )
            .otherwise(0),
        )
        .withColumn(
            "referrer_fee_rebate_cumsum", F.sum("referrer_fee_rebate").over(w_cumsum)
        )
        .withColumn("referral_fee_cumsum", F.sum("referral_fee").over(w_cumsum))
        .withColumn("referral_volume_cumsum", F.sum("referral_volume").over(w_cumsum))
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
    agg_trades_rewards_epoch_referee_referrer_v = dlt.read(
        "agg_trades_rewards_epoch_referee_referrer_v"
    )
    referrer_rewards = dlt.read("agg_referrer_rewards_epoch_user")

    w_cumsum = (
        Window.partitionBy("referee")
        .orderBy("epoch")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)  # cumulative
    )

    referee_rewards = (
        agg_trades_rewards_epoch_referee_referrer_v.withColumnRenamed(
            "alias", "referrer_alias"
        )
        .join(referrer_rewards, on=["epoch", "referrer"], how="left")
        .withColumn(
            "referee_fee_rebate",
            F.when(F.col("referrer_tier") == 3, F.col("trading_fee") * 0.02/2)  # 5% of 2bps
            .when(F.col("referrer_tier") == 2, F.col("trading_fee") * 0.02/2)
            .when(F.col("referrer_tier") == 1, F.col("trading_fee") * 0.02/2)
            .otherwise(0),
        )
        .select(
            "epoch",
            "referee",
            "referrer",
            "referrer_alias",
            "volume",
            "trading_fee",
            "referee_fee_rebate",
            F.sum("referee_fee_rebate")
            .over(w_cumsum)
            .alias("referee_fee_rebate_cumsum"),
            F.sum("trading_fee").over(w_cumsum).alias("trading_fee_cumsum"),
            F.sum("volume").over(w_cumsum).alias("volume_cumsum"),
        )
        .orderBy("epoch", "referee")
    )
    return referee_rewards

# COMMAND ----------


