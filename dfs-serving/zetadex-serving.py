# Databricks notebook source
# Databricks notebook source
dbutils.widgets.dropdown("network", "devnet", ["devnet", "mainnet"], "Network")
# NETWORK = dbutils.widgets.get("network")
NETWORK = dbutils.widgets.get("pipeline.network")

# COMMAND ----------

from databricks import feature_store
from databricks.feature_store.online_store_spec import AmazonDynamoDBSpec
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pytz import timezone
from datetime import datetime, timezone

# COMMAND ----------

# DBTITLE 1,Delta Table Registry to FS
# Don't need to run these again since they're once-off FS definition creations

# fs.create_table(
#     name='zetadex_feature_store.agg_prices_market_1h',
#     primary_keys=["timestamp"],
#     df=agg_trades_24h_rolling_df,
#     partition_columns="date_",
#     description="Rolling 24hr trade summary metrics",
# )

# COMMAND ----------

current_date = str(datetime.now(timezone.utc).date())
current_hour = datetime.now(timezone.utc).hour

# COMMAND ----------

fs = feature_store.FeatureStoreClient()

online_store = AmazonDynamoDBSpec(
  region='ap-southeast-1',
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Feature Store Update Routines

# COMMAND ----------

# DBTITLE 1,zetadex_mainnet.agg_pnl
# fs.register_table(
#   delta_table="zetadex_mainnet.agg_pnl",
#   primary_keys='owner_pub_key',
#   description='Zeta DEX Account PNLs'
# )

fs.publish_table(
  name='zetadex_mainnet.agg_pnl',
  online_store=online_store,
  filter_condition=f"date_ = '{current_date}' and hour_ = '{current_hour}'",
  features=[
    'timestamp',
    'owner_pub_key',
    'pnl',
    'pnl_diff_24h',
    'pnl_diff_7d',
    'pnl_diff_30d',
    'pnl_ratio_24h',
    'pnl_ratio_7d',
    'pnl_ratio_30d',
    'pnl_ratio_24h_rank',
    'pnl_ratio_7d_rank',
    'pnl_ratio_30d_rank',
    'pnl_diff_24h_rank',
    'pnl_diff_7d_rank',
    'pnl_diff_30d_rank'],
  mode='merge'
)

# COMMAND ----------

# DBTITLE 1,zetadex_feature_store.agg_trades_24h_rolling_df (BTC+SOL)
# On the fly transformation to get the last 24hrs volume from our 1h agg'ed tables
agg_trades_24h_rolling_df = \
    (spark.table("zetadex_mainnet.agg_trades_1h")
     .filter(F.col("timestamp_window.start") >= (F.date_trunc("hour", F.current_timestamp()) - F.expr('INTERVAL 24 HOUR')))
     .agg(
         F.sum("trades_count").alias("trades_count"),
         F.sum("volume").alias("volume"),
         F.sum("notional_volume").alias("notional_volume"),
         F.sum("premium_sum").alias("premium_sum"),
      )
      .withColumn("timestamp", F.date_trunc("hour", F.current_timestamp()))
      .withColumn("date_", F.to_date("timestamp"))
      .withColumn("hour_", F.date_format("timestamp", "HH").cast("int"))
     )
agg_trades_24h_rolling_df.show()
# Write new results to table
fs.write_table(
  name='zetadex_feature_store.agg_trades_24h_rolling',
  df=agg_trades_24h_rolling_df,
  mode="merge",
)

fs.publish_table(
  name='zetadex_feature_store.agg_trades_24h_rolling',
  online_store=online_store,
#   filter_condition=f"date_ = '{current_date}' and hour_ = '{current_hour}'",
  features=[
    'trades_count',
    'volume',
    'notional_volume',
    'premium_sum',
    'timestamp'],
  mode='merge'
)

# COMMAND ----------

# DBTITLE 1,zetadex_feature_store.agg_trades_24h_rolling_df (SOL)
# On the fly transformation to get the last 24hrs volume from our 1h agg'ed tables
agg_trades_24h_rolling_df_sol = \
    (spark.table("zetadex_mainnet.agg_trades_market_1h")
     .filter(F.col("timestamp_window.start") >= (F.date_trunc("hour", F.current_timestamp()) - F.expr('INTERVAL 24 HOUR')))
     .filter(F.col("underlying") == "SOL")
     .agg(
         F.sum("trades_count").alias("trades_count"),
         F.sum("volume").alias("volume"),
         F.sum("notional_volume").alias("notional_volume"),
         F.sum("premium_sum").alias("premium_sum"),
      )
      .withColumn("timestamp", F.date_trunc("hour", F.current_timestamp()))
      .withColumn("date_", F.to_date("timestamp"))
      .withColumn("hour_", F.date_format("timestamp", "HH").cast("int"))
     )
agg_trades_24h_rolling_df_sol.show()

# fs.create_table(
#     name='zetadex_feature_store.agg_trades_24h_rolling_sol',
#     primary_keys=["timestamp"],
#     df=agg_trades_24h_rolling_df_sol,
#     partition_columns="date_",
#     description="Rolling 24hr trade summary metrics",
# )

# Write new results to table
fs.write_table(
  name='zetadex_feature_store.agg_trades_24h_rolling_sol',
  df=agg_trades_24h_rolling_df_sol,
  mode="merge",
)

fs.publish_table(
  name='zetadex_feature_store.agg_trades_24h_rolling_sol',
  online_store=online_store,
#   filter_condition=f"date_ = '{current_date}' and hour_ = '{current_hour}'",
  features=[
    'trades_count',
    'volume',
    'notional_volume',
    'premium_sum',
    'timestamp'],
  mode='merge'
)

# COMMAND ----------

# DBTITLE 1,zetadex_feature_store.agg_trades_24h_rolling_df (BTC)
# On the fly transformation to get the last 24hrs volume from our 1h agg'ed tables
agg_trades_24h_rolling_df_btc = \
    (spark.table("zetadex_mainnet.agg_trades_market_1h")
     .filter(F.col("timestamp_window.start") >= (F.date_trunc("hour", F.current_timestamp()) - F.expr('INTERVAL 24 HOUR')))
     .filter(F.col("underlying") == "BTC")
     .agg(
         F.sum("trades_count").alias("trades_count"),
         F.sum("volume").alias("volume"),
         F.sum("notional_volume").alias("notional_volume"),
         F.sum("premium_sum").alias("premium_sum"),
      )
      .withColumn("timestamp", F.date_trunc("hour", F.current_timestamp()))
      .withColumn("date_", F.to_date("timestamp"))
      .withColumn("hour_", F.date_format("timestamp", "HH").cast("int"))
     )
agg_trades_24h_rolling_df_btc.show()

# fs.create_table(
#     name='zetadex_feature_store.agg_trades_24h_rolling_btc',
#     primary_keys=["timestamp"],
#     df=agg_trades_24h_rolling_df_btc,
#     partition_columns="date_",
#     description="Rolling 24hr trade summary metrics",
# )

# Write new results to table
fs.write_table(
  name='zetadex_feature_store.agg_trades_24h_rolling_btc',
  df=agg_trades_24h_rolling_df_btc,
  mode="merge",
)

fs.publish_table(
  name='zetadex_feature_store.agg_trades_24h_rolling_btc',
  online_store=online_store,
#   filter_condition=f"date_ = '{current_date}' and hour_ = '{current_hour}'",
  features=[
    'trades_count',
    'volume',
    'notional_volume',
    'premium_sum',
    'timestamp'],
  mode='merge'
)

# COMMAND ----------

# DBTITLE 1,zetadex_feature_store.agg_prices_market_oi
row1 = (spark.table("zetadex_mainnet.agg_prices_market_1h").agg({"timestamp": "max"}).collect())[0]
print(row1["max(timestamp)"])
agg_prices_market_total_df = \
(spark.table("zetadex_mainnet.agg_prices_market_1h").filter(F.col("timestamp") == row1["max(timestamp)"]).groupBy().sum("open_interest", "open_interest_usd").drop("sum(strike)", "sum(theo)", "sum(delta)", "sum(vega)", "sum(sigma)", "sum(hour_)").withColumn("timestamp", F.lit(row1["max(timestamp)"])).withColumnRenamed("sum(open_interest)", "total_open_interest").withColumnRenamed("sum(open_interest_usd)", "total_open_interest_usd"))

agg_prices_market_total_puts = \
(spark.table("zetadex_mainnet.agg_prices_market_1h").filter(F.col("timestamp") == row1["max(timestamp)"]).filter(F.col("kind") == "put").groupBy().sum("open_interest", "open_interest_usd").drop("sum(strike)", "sum(theo)", "sum(delta)", "sum(vega)", "sum(sigma)", "sum(hour_)").withColumn("timestamp", F.lit(row1["max(timestamp)"])).withColumnRenamed("sum(open_interest)", "total_open_interest").withColumnRenamed("sum(open_interest_usd)", "total_open_interest_usd"))

agg_prices_market_total_calls = \
(spark.table("zetadex_mainnet.agg_prices_market_1h").filter(F.col("timestamp") == row1["max(timestamp)"]).filter(F.col("kind") == "call").groupBy().sum("open_interest", "open_interest_usd").drop("sum(strike)", "sum(theo)", "sum(delta)", "sum(vega)", "sum(sigma)", "sum(hour_)").withColumn("timestamp", F.lit(row1["max(timestamp)"])).withColumnRenamed("sum(open_interest)", "total_open_interest").withColumnRenamed("sum(open_interest_usd)", "total_open_interest_usd"))

put_call_ratio = agg_prices_market_total_puts.collect()[0]['total_open_interest'] / agg_prices_market_total_calls.collect()[0]['total_open_interest']
print(put_call_ratio)
# agg_prices_market_total_puts.show()
# agg_prices_market_total_calls.show()

agg_prices_market_total_df = agg_prices_market_total_df.withColumn("put_call_ratio", F.lit(put_call_ratio))
agg_prices_market_total_df.show()

fs.create_table(
    name='zetadex_feature_store.agg_prices_market_oi',
    primary_keys="timestamp",
    df=agg_prices_market_total_df,
    description="Aggregated total open interest",
)

# Write new results to table
fs.write_table(
  name='zetadex_feature_store.agg_prices_market_oi',
  df=agg_prices_market_total_df,
  mode="merge",
)

fs.publish_table(
  name='zetadex_feature_store.agg_prices_market_oi',
  online_store=online_store,
  mode='merge'
)

# COMMAND ----------

# DBTITLE 1,zetadex_feature_store.agg_prices_market_1h
# On the fly transformation forming unique primary key for ddb using a concatenation of market values
agg_prices_market_1h_df = \
    (spark.table("zetadex_mainnet.agg_prices_market_1h")
     .filter(F.col("timestamp") >= (F.date_trunc("hour", F.current_timestamp()) - F.expr('INTERVAL 1 HOUR')))
     .withColumn("ddb_key", F.concat(F.col("underlying"), F.lit("#"), F.unix_timestamp(F.col("expiry_timestamp")), F.lit("#"), F.col("kind"), F.lit("#"), F.col("strike")))
     )
agg_prices_market_1h_df = agg_prices_market_1h_df.drop("timestamp_window", "underlying", "strike", "kind", "date_", "hour_")
agg_prices_market_1h_df.show()

# fs.create_table(
#     name='zetadex_feature_store.agg_prices_market_1h_v3',
#     primary_keys="ddb_key",
#     df=agg_prices_market_1h_df,
#     description="Aggregated options markets 1h",
# )


# Write new results to table
fs.write_table(
  name='zetadex_feature_store.agg_prices_market_1h_v3',
  df=agg_prices_market_1h_df,
  mode="merge",
)

fs.publish_table(
  name='zetadex_feature_store.agg_prices_market_1h_v3',
  online_store=online_store,
#   filter_condition=f"date_ = '{current_date}' and hour_ = '{current_hour}'",
  features=[
      'ddb_key',
      'open_interest',
      'open_interest_usd',
      'theo',
      'delta',
      'vega',
      'sigma',
      'timestamp',
      'expiry_timestamp'],
  mode='merge'
)

# COMMAND ----------


