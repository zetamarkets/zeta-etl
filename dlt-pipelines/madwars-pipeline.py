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

COMPETITION_START_DATE = "2023-07-01 00:00"

# COMMAND ----------

S3_BUCKET_LANDED = f"zetadex-{NETWORK}-landing"
BASE_PATH_LANDED = join("/mnt", S3_BUCKET_LANDED)
S3_BUCKET_TRANSFORMED = f"zetadex-{NETWORK}"
BASE_PATH_TRANSFORMED = join("/mnt", S3_BUCKET_TRANSFORMED)

PNL_TABLE = "madwars/pnl"

# COMMAND ----------

# Teams
# [
#     "Mad Swords",
#     "Mad Androids",
#     "Mad Ballers",
#     "Mad Skulls",
#     "Mad Lassies",
#     "Mad Peacemakers",
#     "Mad Busters",
#     "The Scouts",
#     "Mad Naked",
#     "SOL Samurais",
#     "AI Lads",
#     "Mad Galaxies",
#     "Mad Alberts",
#     "Mad Kings",
#     "Mad Classic",
#     "Nights Watch",
#     "Mad Generals",
#     "Mad Vitaliks"
# ]


@dlt.view()
def teams():
    return (
        spark.table("zetadex_mainnet_tx.agg_pnl")
        .select("authority")
        .distinct()
        .withColumn("r", F.rand(42))
        .withColumn(
            "team",
            F.when(F.col("r") < 0.05, "Mad Swords")
            .when(F.col("r") < 0.1, "Mad Swords")
            .when(F.col("r") < 0.15, "Mad Androids")
            .when(F.col("r") < 0.2, "Mad Ballers")
            .when(F.col("r") < 0.25, "Mad Skulls")
            .when(F.col("r") < 0.3, "Mad Lassies")
            .when(F.col("r") < 0.35, "Mad Peacemakers")
            .when(F.col("r") < 0.4, "Mad Busters")
            .when(F.col("r") < 0.45, "The Scouts")
            .when(F.col("r") < 0.5, "Mad Naked")
            .when(F.col("r") < 0.55, "SOL Samurais")
            .when(F.col("r") < 0.6, "AI Lads")
            .when(F.col("r") < 0.65, "Mad Galaxies")
            .when(F.col("r") < 0.7, "Mad Alberts")
            .when(F.col("r") < 0.75, "Mad Kings")
            .when(F.col("r") < 0.8, "Mad Classic")
            .when(F.col("r") < 0.85, "Nights Watch")
            .when(F.col("r") < 0.9, "Mad Generals")
            .when(F.col("r") < 0.95, "Mad Vitaliks")
            .otherwise("No Team"),
        )
        .withColumn(
            "backpack_username",
            F.concat_ws(
                "-", F.lit("user"), F.row_number().over(Window.orderBy("authority"))
            ),
        )
        .withColumn("multiplier", F.col("r") + 1)
        .drop("r")
    )


# teams_df = teams()

# COMMAND ----------

@dlt.view()
def volume():
    return (
        spark.table("zetadex_mainnet_tx.cleaned_ix_trade")
        .filter(f"block_time >= '{COMPETITION_START_DATE}'")
        .groupBy("authority")
        .agg(F.sum("volume").alias("volume"))
    )

# COMMAND ----------

# Break ties in PnL ranking by pubkey alphabetically
@dlt.table(
    comment="User aggregated data for margin account profit and loss",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "authority",
    },
    # partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, PNL_TABLE, "individual"),
)
def pnl_individual():
    windowSpec = (
        Window.partitionBy("authority")
        # Window.partitionBy("authority", "margin_account")
        .orderBy(F.unix_timestamp("timestamp")).rowsBetween(
            Window.unboundedPreceding, 0
        )
    )

    # Need to make start exclusive since net deposits are in between snapshots
    windowSpecExclusive = (
        Window.partitionBy("authority")
        # Window.partitionBy("authority", "margin_account")
        .orderBy(F.unix_timestamp("timestamp")).rowsBetween(
            Window.unboundedPreceding + 1, 0
        )
    )

    windowSpecRatioRank = Window.partitionBy("timestamp").orderBy(
        F.desc("roi"),
        F.desc("pnl"),
        "authority",  # "margin_account"
    )
    windowSpecDiffRank = Window.partitionBy("timestamp").orderBy(
        F.desc("pnl"),
        F.desc("roi"),
        "authority",  # "margin_account"
    )

    windowSpecRatioRankTeam = Window.partitionBy("timestamp", "team").orderBy(
        F.desc("roi"),
        F.desc("pnl"),
        "authority",  # "margin_account"
    )
    windowSpecDiffRankTeam = Window.partitionBy("timestamp", "team").orderBy(
        F.desc("pnl"),
        F.desc("roi"),
        "authority",  # "margin_account"
    )

    teams_df = dlt.read("teams")
    volume_df = dlt.read("volume")
    return (
        # dlt.read("cleaned_pnl")
        spark.table("zetadex_mainnet_tx.cleaned_pnl")
        # .withWatermark("timestamp", "10 minutes")
        .filter(
            f"timestamp >= '{COMPETITION_START_DATE}'"  # max pnl lookback aggregation
        )
        .join(teams_df, on="authority", how="inner")
        .join(volume_df, on="authority", how="left")
        .withColumn("volume", F.coalesce("volume", F.lit(0)))
        .withColumn("cumulative_pnl_lag", F.first("cumulative_pnl").over(windowSpec))
        .withColumn("equity_lag", F.first("equity").over(windowSpec))
        .withColumn(
            "w",
            (
                F.unix_timestamp(F.date_trunc("hour", F.current_timestamp()))
                - F.unix_timestamp(F.col("timestamp"))
            )
            / (
                F.unix_timestamp(F.date_trunc("hour", F.current_timestamp()))
                - F.unix_timestamp(F.to_timestamp(F.lit(COMPETITION_START_DATE)))
            ),
        )
        .withColumn(
            "deposit_amount_weighted",
            F.sum(F.col("deposit_amount") * F.col("w")).over(windowSpecExclusive),
        )
        .drop("w")
        # PnL and ROI
        .withColumn("pnl", F.col("cumulative_pnl") - F.col("cumulative_pnl_lag"))
        # Simple Dietz ROI calculation, using safe div 0/0 => 0
        # Using a $100 fudge factor in denominator (binance does this) to reduce impact of small balances
        # https://www.binance.com/en/support/faq/introduction-to-binance-futures-leaderboard-a507bdb81ad0464e871e60d43fd21526
        .withColumn(
            "roi",  # "roi_modified_dietz",
            F.when(F.col("pnl") == 0, F.lit(0)).otherwise(
                F.col("pnl")
                / (100 + F.col("equity_lag") + F.col("deposit_amount_weighted"))
            ),
        )
        # ranks
        .withColumn("pnl_rank_global", F.rank().over(windowSpecDiffRank))
        .withColumn("roi_rank_global", F.rank().over(windowSpecRatioRank))
        .withColumn("pnl_rank_team", F.rank().over(windowSpecDiffRankTeam))
        .withColumn("roi_rank_team", F.rank().over(windowSpecRatioRankTeam))
        .filter("timestamp == date_trunc('hour',  current_timestamp)")
    )


# individual_pnl_df = pnl_individual()

# COMMAND ----------

@dlt.table(
    comment="User aggregated data for margin account profit and loss",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "team",
    },
    # partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED, PNL_TABLE, "team"),
)
def pnl_team():
    windowSpec = (
        Window.partitionBy("team")
        .orderBy(F.unix_timestamp("timestamp"))
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    # Need to make start exclusive since net deposits are in between snapshots
    windowSpecExclusive = (
        Window.partitionBy("team")
        .orderBy(F.unix_timestamp("timestamp"))
        .rowsBetween(Window.unboundedPreceding + 1, 0)
    )

    windowSpecRatioRank = Window.partitionBy("timestamp").orderBy(
        F.desc("roi"),
        F.desc("pnl"),
        "team",
    )
    windowSpecDiffRank = Window.partitionBy("timestamp").orderBy(
        F.desc("pnl"),
        F.desc("roi"),
        "team",
    )

    teams_df = dlt.read("teams")
    volume_df = dlt.read("volume")
    return (
        # dlt.read("cleaned_pnl")
        spark.table("zetadex_mainnet_tx.cleaned_pnl")
        .withWatermark("timestamp", "10 minutes")
        .filter(
            f"timestamp >= '{COMPETITION_START_DATE}'"  # max pnl lookback aggregation
        )
        .join(teams_df, on="authority", how="inner")
        .join(volume_df, on="authority", how="left")
        .withColumn("volume", F.coalesce("volume", F.lit(0)))
        .groupBy("timestamp", "team")
        .agg(
            F.sum("cumulative_pnl").alias("cumulative_pnl"),
            F.sum("equity").alias("equity"),
            F.sum("deposit_amount").alias("deposit_amount"),
            F.sum("volume").alias("volume"),
        )
        .withColumn("cumulative_pnl_lag", F.first("cumulative_pnl").over(windowSpec))
        .withColumn("equity_lag", F.first("equity").over(windowSpec))
        .withColumn(
            "w",
            (
                F.unix_timestamp(F.date_trunc("hour", F.current_timestamp()))
                - F.unix_timestamp(F.col("timestamp"))
            )
            / (
                F.unix_timestamp(F.date_trunc("hour", F.current_timestamp()))
                - F.unix_timestamp(F.to_timestamp(F.lit(COMPETITION_START_DATE)))
            ),
        )
        .withColumn(
            "deposit_amount_weighted",
            F.sum(F.col("deposit_amount") * F.col("w")).over(windowSpecExclusive),
        )
        .drop("w")
        # PnL and ROI
        .withColumn("pnl", F.col("cumulative_pnl") - F.col("cumulative_pnl_lag"))
        .withColumn(
            "roi",  # "roi_modified_dietz",
            F.when(F.col("pnl") == 0, F.lit(0)).otherwise(
                F.col("pnl")
                / (100 + F.col("equity_lag") + F.col("deposit_amount_weighted"))
            ),
        )
        # ranks
        .withColumn("pnl_rank", F.rank().over(windowSpecDiffRank))
        .withColumn("roi_rank", F.rank().over(windowSpecRatioRank))
        .filter("timestamp == date_trunc('hour',  current_timestamp - interval 1 hour)")
    )


# pnl_team_df = pnl_team()

# COMMAND ----------


