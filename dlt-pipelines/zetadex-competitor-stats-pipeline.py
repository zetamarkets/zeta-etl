# Databricks notebook source
dbutils.widgets.dropdown("network", "devnet", ["devnet", "mainnet"], "Network")
NETWORK = spark.conf.get("pipeline.network")

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from os.path import join
from itertools import chain
import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC # ETL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Competitor Volume

# COMMAND ----------

exchange_volume_schema = '''
exchange string,
asset string,
volume double,
timestamp timestamp
'''

# COMMAND ----------

# DBTITLE 1,Bronze
@dlt.table(
    comment="Raw data for competitor data",
    table_properties={
        "quality": "bronze",
    },
    path="/mnt/zetamarkets-market-data/competitor-stats/raw",
    schema=exchange_volume_schema,
)
def raw_competitor_data():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.region", "ap-southeast-1")
        .option("cloudFiles.includeExistingFiles", True)
        .option("cloudFiles.useNotifications", True)
        .option("partitionColumns", "exchange")
        .schema(exchange_volume_schema)
        .load("/mnt/zetamarkets-market-data-landing/competitor-stats/data")
    )

# COMMAND ----------

# DBTITLE 1,Silver
# Rolling 24h
qry_zeta_trade_asset_24h_rolling = """
select
    t.timestamp
    , 'zeta' as exchange
    , t.asset
    , t.volume_24h as volume
from zetadex_mainnet_tx.agg_ix_trade_asset_24h_rolling t
where
  timestamp >= date('2024-01-11')
"""


@dlt.table(
    comment="Cleaned data for competitor data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "timestamp,exchange,asset",
    },
    path=join("/mnt/zetamarkets-market-data/competitor-stats/cleaned"),
)
def cleaned_competitor_data():
    zeta = spark.sql(qry_zeta_trade_asset_24h_rolling)

    competitors = dlt.read("raw_competitor_data").select(
        F.date_trunc("hour", F.col("timestamp")).alias("timestamp"),
        "exchange",
        F.when(F.col("asset") == "1MBONK-PERP", "ONEMBONK")
        .otherwise(F.expr("replace(asset, '-PERP', '')"))
        .alias("asset"),
        "volume",
    )
    return (
        zeta.unionAll(competitors)
        .withColumn("date_", F.to_date("timestamp"))
        .withColumn("hour_", F.date_format("timestamp", "HH").cast("int"))
    )

# COMMAND ----------


