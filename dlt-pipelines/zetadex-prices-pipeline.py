# Databricks notebook source
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
        "quality":"bronze",
    },
    path="/mnt/zetamarkets-market-data/coingecko-prices/raw",
    schema=coin_prices_schema
)
def raw_coingecko_prices():
    return (spark
           .readStream
           .format("cloudFiles")
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
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["underlying"],
    path="/mnt/zetamarkets-market-data/coingecko-prices/cleaned"
)
def cleaned_coingecko_prices():
    return (dlt.read_stream("raw_coingecko_prices")
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
        "quality":"bronze", 
    },
    path=join(BASE_PATH_TRANSFORMED,TRADES_TABLE,"raw"),
    schema=trades_schema
)
def raw_trades():
    return (spark
           .readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "json")
           .option("cloudFiles.region", "ap-southeast-1")
           .option("cloudFiles.includeExistingFiles", True)
           .option("cloudFiles.useNotifications", True)
           .option("partitionColumns", "year,month,day,hour")
           .schema(trades_schema)
           .load(join(BASE_PATH_LANDED,TRADES_TABLE,"data"))
          )

# COMMAND ----------

@dlt.table(
    comment="Cleaned data for trades",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED,TRADES_TABLE,"cleaned")
)
def cleaned_trades():
    prices_df = (dlt.read_stream("cleaned_coingecko_prices")
                 .withColumn("date_hour", F.date_trunc("hour", "timestamp"))
                 .withWatermark("date_hour", "1 hour")
                )
    return (dlt.read_stream("raw_trades")
           .withWatermark("timestamp", "1 hour")
           .dropDuplicates(["seq_num", "market_index", "expiry_timestamp", "underlying"])
           .withColumn("side", F.when(F.col("is_bid"), "bid").otherwise("ask"))
           .withColumn("premium", F.col("price") * F.col("size"))
           .join(prices_df,
               F.expr("""
                raw_trades.underlying = cleaned_coingecko_prices.underlying AND
                raw_trades.timestamp >= date_hour and raw_trades.timestamp < date_hour + interval 1 hour
                """), 
               how="left")
           .withColumn("notional", F.col("price_usd") * F.col("size"))
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
               "market_index",
               "seq_num",
               "order_id",
               "client_order_id",
           )
           .withColumn("date_", F.to_date("timestamp"))
           .withColumn("hour_", F.date_format("timestamp", "HH").cast("int"))
          )

# COMMAND ----------

@dlt.table(
    comment="Market-hourly aggregated data for trades",
    table_properties={
        "quality":"gold", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED,TRADES_TABLE,"agg-m1h")
)
def agg_trades_market_1h():
    return (dlt.read_stream("cleaned_trades")
                 .where("is_maker")
                 .withWatermark("timestamp", "1 hour")
                 .groupBy(F.window("timestamp", "1 hour").alias("timestamp_window"), "underlying", "expiry_timestamp", "strike", "kind", "side")
                 .agg(
                     F.count("*").alias("trades_count"),
                     F.sum("size").alias("volume"),
                     F.sum("notional").alias("notional_volume"),
                     F.sum("premium").alias("premium_sum"),
                  )
                 .withColumn("date_", F.to_date(F.col("timestamp_window.end") - F.expr('INTERVAL 1 HOUR')))
                 .withColumn("hour_", F.date_format(F.col("timestamp_window.end") - F.expr('INTERVAL 1 HOUR'), "HH").cast("int"))
            )
    
@dlt.table(
    comment="Hourly aggregated data for trades",
    table_properties={
        "quality":"gold", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED,TRADES_TABLE,"agg-1h"),
)
def agg_trades_1h():
    return (dlt.read_stream("cleaned_trades")
                 .where("is_maker")
                 .withWatermark("timestamp", "1 hour")
                 .groupBy(F.window("timestamp", "1 hour").alias("timestamp_window"))
                 .agg(
                     F.count("*").alias("trades_count"),
                     F.sum("size").alias("volume"),
                     F.sum("notional").alias("notional_volume"),
                     F.sum("premium").alias("premium_sum"),
                  )
                 .withColumn("date_", F.to_date(F.col("timestamp_window.end") - F.expr('INTERVAL 1 HOUR')))
                 .withColumn("hour_", F.date_format(F.col("timestamp_window.end") - F.expr('INTERVAL 1 HOUR'), "HH").cast("int"))
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prices

# COMMAND ----------

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
slot long,
year string,
month string,
day string,
hour string
"""

@dlt.table(
    comment="Raw data for platform mark prices, greeks and OI",
    table_properties={
        "quality":"bronze", 
    },
    path=join(BASE_PATH_TRANSFORMED,PRICES_TABLE,"raw"),
    schema=prices_schema
)
def raw_prices():
    return (spark
           .readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "json")
           .option("cloudFiles.region", "ap-southeast-1")
           .option("cloudFiles.includeExistingFiles", True)
           .option("cloudFiles.useNotifications", True)
           .option("partitionColumns", "year,month,day,hour")
           .schema(prices_schema)
           .load(join(BASE_PATH_LANDED,PRICES_TABLE,"data"))
          )

# COMMAND ----------

@dlt.table(
    comment="Cleaned data for platform mark prices, greeks and OI",
    table_properties={
        "quality":"silver", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED,PRICES_TABLE,"cleaned")
)
def cleaned_prices():
    prices_df = (dlt.read("cleaned_coingecko_prices")
                 .withColumn("date_hour", F.date_trunc("hour", "timestamp"))
                 .withWatermark("date_hour", "1 hour")
                )
    return (dlt.read_stream("raw_prices")
     .withWatermark("timestamp", "1 minute")
#      .dropDuplicates(["slot", "market_index", "expiry_timestamp", "underlying"])
     .withColumn("open_interest_usd", F.col("open_interest") * F.col("theo"))
     .join(prices_df,
           F.expr("""
            raw_prices.underlying = cleaned_coingecko_prices.underlying AND
            raw_prices.timestamp >= date_hour and raw_prices.timestamp < date_hour + interval 1 hour
            """), 
           how="left")
     .withColumn("open_interest_notional", F.col("open_interest") * F.col("price_usd"))
     .select("raw_prices.timestamp", 
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
             "slot",
            )
      .withColumn("date_", F.to_date("timestamp"))
      .withColumn("hour_", F.date_format("timestamp", "HH").cast("int"))
    )

# COMMAND ----------

@dlt.table(
    comment="Market-hourly aggregated data for prices, greeks",
    table_properties={
        "quality":"gold", 
        "pipelines.autoOptimize.zOrderCols":"timestamp"
    },
    partition_cols=["date_"],
    path=join(BASE_PATH_TRANSFORMED,PRICES_TABLE,"agg-m1h")
)
def agg_prices_market_1h():
    return (dlt.read_stream("cleaned_prices")
               .withWatermark("timestamp", "1 hour")
               .groupBy(F.window("timestamp", "1 hour").alias("timestamp_window"), "underlying", "expiry_timestamp", "strike", "kind")
               .agg(
                   F.first("theo", ignorenulls=True).alias("theo"),
                   F.first("delta", ignorenulls=True).alias("delta"),
                   F.first("vega", ignorenulls=True).alias("vega"),
                   F.first("sigma", ignorenulls=True).alias("sigma"),
                   F.first("open_interest", ignorenulls=True).alias("open_interest"),
                   F.first("open_interest_usd", ignorenulls=True).alias("open_interest_usd"),
                )
               .withColumn("date_", F.to_date(F.col("timestamp_window.end") - F.expr('INTERVAL 1 HOUR')))
               .withColumn("hour_", F.date_format(F.col("timestamp_window.end") - F.expr('INTERVAL 1 HOUR'), "HH").cast("int"))
               .withColumn("timestamp", F.col("timestamp_window.end"))
            )

# COMMAND ----------


