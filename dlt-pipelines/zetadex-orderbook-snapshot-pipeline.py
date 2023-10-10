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

ORDERBOOK_TABLE = "orderbook-snapshotter"

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

# DBTITLE 1,Bronze
orderbook_snapshot_schema = """
asset string,
localTimestamp bigint,
exchangeTimestamp bigint,
midpoint double,
markPrice double,
bids array<
    struct<
        price double,
        size double,
        openOrdersPubkey string,
        authority string
    >
>,
asks array<
    struct<
        price double,
        size double,
        openOrdersPubkey string,
        authority string
    >
>
"""

@dlt.table(
     comment="Raw data for orderbook snapshots",
     table_properties={
         "quality": "bronze"
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

#df_raw_orderbook_snapshot = spark.read.schema(orderbook_snapshot_schema).json(join(BASE_PATH_LANDED, ORDERBOOK_TABLE, "data"))
#display(df_raw_orderbook_snapshot)

# COMMAND ----------

# DBTITLE 1,Silver
@dlt.view
def market_maker_labels():
    return spark.table(f"zetadex_mainnet.pubkey_label")

# Bids
@dlt.table(
     comment="Cleaned data for bids",
     table_properties={
         "quality": "silver",
     },
     path=join(BASE_PATH_TRANSFORMED, ORDERBOOK_TABLE, "cleaned-orderbook-bids"),
)
def cleaned_orderbook_bids():
    market_maker_labels_df = dlt.read("market_maker_labels")
    return (
        dlt.read("raw_orderbook_snapshot")
        .select(
            "*", F.explode("bids").alias("bid")
        )
        .join(
            market_maker_labels_df,
            (
                F.col("bid.authority")
                == market_maker_labels_df.pub_key
            ),
            how="inner"
        )
        .select(
            "asset",
            market_maker_labels_df.label,
            market_maker_labels_df.organisation,
            F.from_unixtime("localTimestamp").alias("local_timestamp"),
            F.from_unixtime("exchangeTimestamp").alias("exchange_timestamp"),
            F.col("markPrice").alias("mark_price"),
            "midpoint",
            F.col("bid.price").alias("price"),
            (F.abs(F.col("bid.price") - F.col("midpoint")) / F.col("midpoint") * 10000).alias("bps_from_mid"),
            F.col("bid.size").alias("size"),
            (F.col("bid.price")*F.col("bid.size")).alias("volume"),
            F.col("bid.openOrdersPubkey").alias("open_orders_pubkey"),
            F.col("bid.authority")
        )
        .withColumn(
            "spread_group",
            F.when(F.col("bps_from_mid") <= 2.50, '(0) <= 2.5 bps')
            .when((F.col("bps_from_mid") > 2.50) & (F.col("bps_from_mid") <= 5.00),'(1) 2.5 bps < x <= 5.0 bps')
            .when((F.col("bps_from_mid") > 5.00) & (F.col("bps_from_mid") <= 10.00),'(2) 5.0 bps < x <= 10.0 bps')
            .when((F.col("bps_from_mid") > 10.00) & (F.col("bps_from_mid") <= 25.00),'(3) 10.0 bps < x <= 25.0 bps')
            .when((F.col("bps_from_mid") > 25.00) & (F.col("bps_from_mid") <= 50.00),'(4) 25.0 bps < x <= 50.0 bps')
            .when((F.col("bps_from_mid") > 50.00), '(5) > 50.0 bps')
        )
        .withColumn("side", F.lit("bid"))
    )

# Asks
@dlt.table(
     comment="Cleaned data for asks",
     table_properties={
         "quality": "silver",
     },
     path=join(BASE_PATH_TRANSFORMED, ORDERBOOK_TABLE, "cleaned-orderbook-asks"),
)
def cleaned_orderbook_asks():
    market_maker_labels_df = dlt.read("market_maker_labels")
    return (
        dlt.read("raw_orderbook_snapshot")
        .select(
            "*", F.explode("asks").alias("ask")
        )
        .join(
            market_maker_labels_df,
            (
                F.col("ask.authority")
                == market_maker_labels_df.pub_key
            ),
            how="inner"
        )
        .select(
            "asset",
            market_maker_labels_df.label,
            market_maker_labels_df.organisation,
            F.from_unixtime("localTimestamp").alias("local_timestamp"),
            F.from_unixtime("exchangeTimestamp").alias("exchange_timestamp"),
            F.col("markPrice").alias("mark_price"),
            "midpoint",
            F.col("ask.price").alias("price"),
           (F.abs(F.col("ask.price") - F.col("midpoint")) / F.col("midpoint") * 10000).alias("bps_from_mid"),
            F.col("ask.size").alias("size"),
            (F.col("ask.price")*F.col("ask.size")).alias("volume"),
            F.col("ask.openOrdersPubkey").alias("open_orders_pubkey"),
            F.col("ask.authority")
        )
        .withColumn(
            "spread_group",
            F.when((F.abs(F.col("price") - F.col("midpoint")) / F.col("midpoint") * 10000) <= 2.50, '(0) <= 2.5 bps')
            .when(((F.abs(F.col("price") - F.col("midpoint")) / F.col("midpoint") * 10000) > 2.50) & ((F.abs(F.col("price") - F.col("midpoint")) / F.col("midpoint") * 10000) <= 5.00),'(1) 2.5 bps < x <= 5.0 bps')
            .when(((F.abs(F.col("price") - F.col("midpoint")) / F.col("midpoint") * 10000) > 5.00) & ((F.abs(F.col("price") - F.col("midpoint")) / F.col("midpoint") * 10000) <= 10.00),'(2) 5.0 bps < x <= 10.0 bps')
            .when(((F.abs(F.col("price") - F.col("midpoint")) / F.col("midpoint") * 10000) > 10.00) & ((F.abs(F.col("price") - F.col("midpoint")) / F.col("midpoint") * 10000) <= 25.00),'(3) 10.0 bps < x <= 25.0 bps')
            .when(((F.abs(F.col("price") - F.col("midpoint")) / F.col("midpoint") * 10000) > 25.00) & ((F.abs(F.col("price") - F.col("midpoint")) / F.col("midpoint") * 10000) <= 50.00),'(4) 25.0 bps < x <= 50.0 bps')
            .when(((F.abs(F.col("price") - F.col("midpoint")) / F.col("midpoint") * 10000) > 50.00), '(5) > 50.0 bps')
        )
        .withColumn("side", F.lit("ask"))
    )

# All
@dlt.table(
     comment="Cleaned data for all orders",
     table_properties={
         "quality": "silver",
     },
     path=join(BASE_PATH_TRANSFORMED, ORDERBOOK_TABLE, "cleaned-orderbook-all"),
)
def cleaned_orderbook_all():
    bids = dlt.read("cleaned_orderbook_bids")
    asks = dlt.read("cleaned_orderbook_asks")
    return (
        bids.union(asks)
    )

# COMMAND ----------

# DBTITLE 1,Gold
@dlt.table(
    comment="Aggregated orderbook data by market, market maker, spread group, and time",
    table_properties={
        "quality": "gold",
    },
    path=join(BASE_PATH_TRANSFORMED, ORDERBOOK_TABLE, "agg-orderbook-all"),
)
def agg_orderbook_all():
    return (
        dlt.read("cleaned_orderbook_all")
        .groupBy(
            "local_timestamp",
            "asset",
            "label",
            "spread_group"
        )
        .agg(
            F.sum("volume").alias("volume"),
        )
)
