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
# from delta.tables import DeltaTable
# delta = DeltaTable.forPath(spark, "zetadex_mainnet.agg_trades_market_1h") # or DeltaTable.forName
# delta.upgradeTableProtocol(2, 5) # upgrades to readerVersion=2, writerVersion=5

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
print(current_date)
print(current_hour)

# COMMAND ----------

fs = feature_store.FeatureStoreClient()

online_store = AmazonDynamoDBSpec(
  region='ap-southeast-1',
)

underlyings = ['SOL', 'BTC', 'ETH', 'APT']

# COMMAND ----------

# MAGIC %md
# MAGIC # Feature Store Update Routines

# COMMAND ----------

# DBTITLE 1,zetadex_feature_store.agg_funding (ALL)
# No transformations, just use the 'agg_funding_rate_1h' table

table_name = 'zetadex_feature_store.agg_funding'

agg_funding_df = spark.table("zetadex_mainnet.agg_funding_rate_1h")\
    .withColumn("ddb_key", F.concat(F.col("margin_account"), F.lit("#"), F.col("hour")))
agg_funding_df.show()

try:
    result = fs.get_table(table_name)
    print(result)
    print('Table Already Exists...')
except Exception:
    print('Creating New Table...')
    fs.create_table(
        name=table_name,
        primary_keys="ddb_key",
        df=agg_funding_df,
        description=f"1h aggregated funding"
    )

# Write new results to table
fs.write_table(
  name=table_name,
  df=agg_funding_df,
  mode="merge",
)

fs.publish_table(
  name=table_name,
  online_store=online_store,
#   filter_condition=f"date_ = '{current_date}' and hour_ = '{current_hour}'",
  features=[
    'ddb_key',
    'asset',
    'margin_account',
    'pubkey',
    'balance_change',
    'hour'],
  mode='merge'
)

# COMMAND ----------

# DBTITLE 1,zetadex_mainnet.agg_pnl
# fs.register_table(
#   delta_table="zetadex_mainnet.agg_pnl_geyser",
#   primary_keys='owner_pub_key',
#   description='Zeta DEX Account PNLs'
# )

max_date_row = (spark.table('zetadex_mainnet.agg_pnl_geyser').agg({"date_": "max"}).collect())[0]
print(max_date_row["max(date_)"])
max_hour_row = (spark.table("zetadex_mainnet.agg_pnl_geyser").filter(F.col("date_") == max_date_row["max(date_)"]).agg({"hour_": "max"}).collect())[0]
print(max_hour_row["max(hour_)"])
# max_hour = (spark.table('zetadex_mainnet.agg_pnl_geyser').agg({"date_": "max"}).collect())[0]

fs.publish_table(
  name='zetadex_mainnet.agg_pnl_geyser',
  online_store=online_store,
  filter_condition=f"date_ = '{max_date_row['max(date_)']}' and hour_ = '{max_hour_row['max(hour_)']}'",
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

# DBTITLE 1,zetadex_feature_store.agg_trades_24h_rolling_df (ALL)
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

# DBTITLE 1,zetadex_feature_store.agg_trades_24h_rolling_df (underlying)
for underlying in underlyings:
    table_name = "zetadex_feature_store.agg_trades_24h_rolling_" + underlying.lower()
    print(f"Underlying: {underlying}")
    print(f"Table Name: {table_name}")

    # On the fly transformation to get the last 24hrs volume from our 1h agg'ed tables
    agg_trades_24h_rolling_df_underlying = \
        (spark.table("zetadex_mainnet.agg_trades_market_1h")
         .filter(F.col("timestamp_window.start") >= (F.date_trunc("hour", F.current_timestamp()) - F.expr('INTERVAL 24 HOUR')))
         .filter(F.col("underlying") == underlying)
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
    agg_trades_24h_rolling_df_underlying.show()

    try:
        result = fs.get_table(table_name)
        print(result)
        print('Table Already Exists...')
    except Exception:
        print('Creating New Table...')
        fs.create_table(
            name=table_name,
            primary_keys=["timestamp"],
            df=agg_trades_24h_rolling_df_underlying,
            partition_columns="date_",
            description=f"Rolling 24hr trade summary metrics {underlying}",
        )

    # Write new results to table
    fs.write_table(
      name=table_name,
      df=agg_trades_24h_rolling_df_underlying,
      mode="merge",
    )

    fs.publish_table(
      name=table_name,
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

# DBTITLE 1,zetadex_feature_store.agg_trades_24h_rolling_market
for underlying in underlyings:
    table_name = "zetadex_feature_store.agg_trades_24h_rolling_market_" + underlying.lower()
    print(f"Underlying: {underlying}")
    print(f"Table Name: {table_name}")

    # On the fly transformation to get the last 24hrs volume from our 1h agg'ed tables
    agg_trades_24h_rolling_df_market = \
        (spark.table("zetadex_mainnet.agg_trades_market_1h")
            .withColumn("ddb_key", F.concat(F.col("underlying"), F.lit("#"), F.unix_timestamp(F.col("expiry_timestamp")), F.lit("#"), F.col("kind"), F.lit("#"), F.col("strike")))
            .filter(F.col("underlying") == underlying)
            .filter(F.col("timestamp_window.start") >= (F.date_trunc("hour", F.current_timestamp()) - F.expr('INTERVAL 24 HOUR')))
            .groupby("ddb_key")
            .agg(
                F.sum("trades_count").alias("trades_count"),
                F.sum("volume").alias("volume"),
                F.sum("notional_volume").alias("notional_volume"),
                F.sum("premium_sum").alias("premium_sum"),
                F.first("underlying"),
                F.first("expiry_timestamp"),
            )
            .withColumnRenamed("first(underlying)", "underlying")
            .withColumnRenamed("first(expiry_timestamp)", "expiry_timestamp")
            .withColumn("timestamp", F.date_trunc("hour", F.current_timestamp()))
            .withColumn("date_", F.to_date("timestamp"))
            .withColumn("hour_", F.date_format("timestamp", "HH").cast("int"))
            )
    agg_trades_24h_rolling_df_market.show()

    try:
        result = fs.get_table(table_name)
        print(result)
        print('Table Already Exists...')
    except Exception:
        print('Creating New Table...')
        fs.create_table(
            name=table_name,
            primary_keys=["ddb_key"],
            df=agg_trades_24h_rolling_df_market,
            description=f"Rolling 24hr volumes per market {table_name}",
        )

    # Write new results to table
    fs.write_table(
        name=table_name,
        df=agg_trades_24h_rolling_df_market,
        mode="merge",
    )

    fs.publish_table(
        name=table_name,
        online_store=online_store,
        features=[
        'ddb_key',
        'trades_count',
        'volume',
        'notional_volume',
        'premium_sum',
        'timestamp',
        'underlying',
        'expiry_timestamp'],
        mode='merge'
    )

# COMMAND ----------

# DBTITLE 1,zetadex_feature_store.agg_surfaces_expiry_1h
table_name = "zetadex_feature_store.agg_surfaces_expiry_1h_v2"

row1 = (spark.table("zetadex_mainnet.agg_surfaces_expiry_1h").agg({"timestamp": "max"}).collect())[0]
print(row1["max(timestamp)"])

agg_surfaces_expiry_1h_df = \
    (spark.table("zetadex_mainnet.agg_surfaces_expiry_1h")
     .filter(F.col("timestamp") == row1["max(timestamp)"])
     .withColumn("ddb_key", F.concat(F.col("underlying"), F.lit("#"), F.unix_timestamp(F.col("expiry_timestamp"))))
     )
agg_surfaces_expiry_1h_df = agg_surfaces_expiry_1h_df.drop("timestamp_window", "underlying", "date_", "hour_")
agg_surfaces_expiry_1h_df.show()

try:
    result = fs.get_table(table_name)
    print(result)
    print('Table Already Exists...')
except Exception:
    print('Creating New Table...')
    fs.create_table(
        name=table_name,
        primary_keys="ddb_key",
        df=agg_surfaces_expiry_1h_df,
        description="Aggregated surfaces expiry 1h",
    )

# Write new results to table
fs.write_table(
  name=table_name,
  df=agg_surfaces_expiry_1h_df,
  mode="merge",
)

fs.publish_table(
  name=table_name,
  online_store=online_store,
#   filter_condition=f"date_ = '{current_date}' and hour_ = '{current_hour}'",
  features=[
      'ddb_key',
      'interest_rate',
      'vol_surface',
      'timestamp',
      'expiry_timestamp'
  ],
  mode='merge'
)

# COMMAND ----------

# DBTITLE 1,zetadex_feature_store.agg_prices_market_1h
row1 = (spark.table("zetadex_mainnet.agg_prices_market_1h").agg({"timestamp": "max"}).collect())[0]
print(row1["max(timestamp)"])

agg_prices_market_1h_df = \
    (spark.table("zetadex_mainnet.agg_prices_market_1h")
     .filter(F.col("timestamp") == row1["max(timestamp)"])
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

# DBTITLE 1,zetadex_feature_store.agg_prices_market_oi (ALL)
row1 = (spark.table("zetadex_mainnet.agg_prices_market_1h").agg({"timestamp": "max"}).collect())[0]
print(row1["max(timestamp)"])
agg_prices_market_total_df = \
(spark.table("zetadex_mainnet.agg_prices_market_1h").filter(F.col("timestamp") == row1["max(timestamp)"]).groupBy().sum("open_interest", "open_interest_usd").drop("sum(strike)", "sum(theo)", "sum(delta)", "sum(vega)", "sum(sigma)", "sum(hour_)").withColumn("timestamp", F.lit(row1["max(timestamp)"])).withColumnRenamed("sum(open_interest)", "total_open_interest").withColumnRenamed("sum(open_interest_usd)", "total_open_interest_usd"))

agg_prices_market_total_puts = \
(spark.table("zetadex_mainnet.agg_prices_market_1h").filter(F.col("timestamp") == row1["max(timestamp)"]).filter(F.col("kind") == "put").groupBy().sum("open_interest", "open_interest_usd").drop("sum(strike)", "sum(theo)", "sum(delta)", "sum(vega)", "sum(sigma)", "sum(hour_)").withColumn("timestamp", F.lit(row1["max(timestamp)"])).withColumnRenamed("sum(open_interest)", "total_open_interest").withColumnRenamed("sum(open_interest_usd)", "total_open_interest_usd"))

agg_prices_market_total_calls = \
(spark.table("zetadex_mainnet.agg_prices_market_1h").filter(F.col("timestamp") == row1["max(timestamp)"]).filter(F.col("kind") == "call").groupBy().sum("open_interest", "open_interest_usd").drop("sum(strike)", "sum(theo)", "sum(delta)", "sum(vega)", "sum(sigma)", "sum(hour_)").withColumn("timestamp", F.lit(row1["max(timestamp)"])).withColumnRenamed("sum(open_interest)", "total_open_interest").withColumnRenamed("sum(open_interest_usd)", "total_open_interest_usd"))

try:
    put_call_ratio = agg_prices_market_total_puts.collect()[0]['total_open_interest'] / agg_prices_market_total_calls.collect()[0]['total_open_interest']
except Exception:
    put_call_ratio = 0
print(put_call_ratio)

# put_call_ratio = agg_prices_market_total_puts.collect()[0]['total_open_interest'] / agg_prices_market_total_calls.collect()[0]['total_open_interest']
# print(put_call_ratio)
# agg_prices_market_total_puts.show()
# agg_prices_market_total_calls.show()



agg_prices_market_total_df = agg_prices_market_total_df.withColumn("put_call_ratio", F.lit(put_call_ratio).cast("double"))
agg_prices_market_total_df.show()

try:
    result = fs.get_table('zetadex_feature_store.agg_prices_market_oi')
    print(result)
    print('Table Already Exists...')
except Exception:
    print('Creating New Table...')
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

# DBTITLE 1,zetadex_feature_store.agg_prices_market_oi (underlying)
for underlying in underlyings:
    table_name = 'zetadex_feature_store.agg_prices_market_oi_' + underlying.lower()
    print(f"Underlying: {underlying}")
    print(f"Table Name: {table_name}")

    row1 = (spark.table("zetadex_mainnet.agg_prices_market_1h").agg({"timestamp": "max"}).collect())[0]
    print(row1["max(timestamp)"])

    agg_prices_market_total_df_underlying = \
    (spark.table("zetadex_mainnet.agg_prices_market_1h").filter(F.col("timestamp") == row1["max(timestamp)"]).filter(F.col("underlying") == underlying).groupBy().sum("open_interest", "open_interest_usd").drop("sum(strike)", "sum(theo)", "sum(delta)", "sum(vega)", "sum(sigma)", "sum(hour_)").withColumn("timestamp", F.lit(row1["max(timestamp)"])).withColumnRenamed("sum(open_interest)", "total_open_interest").withColumnRenamed("sum(open_interest_usd)", "total_open_interest_usd"))

    agg_prices_market_total_puts_underlying = \
    (spark.table("zetadex_mainnet.agg_prices_market_1h").filter(F.col("timestamp") == row1["max(timestamp)"]).filter(F.col("kind") == "put").filter(F.col("underlying") == underlying).groupBy().sum("open_interest", "open_interest_usd").drop("sum(strike)", "sum(theo)", "sum(delta)", "sum(vega)", "sum(sigma)", "sum(hour_)").withColumn("timestamp", F.lit(row1["max(timestamp)"])).withColumnRenamed("sum(open_interest)", "total_open_interest").withColumnRenamed("sum(open_interest_usd)", "total_open_interest_usd"))

    agg_prices_market_total_calls_underlying = \
    (spark.table("zetadex_mainnet.agg_prices_market_1h").filter(F.col("timestamp") == row1["max(timestamp)"]).filter(F.col("kind") == "call").filter(F.col("underlying") == underlying).groupBy().sum("open_interest", "open_interest_usd").drop("sum(strike)", "sum(theo)", "sum(delta)", "sum(vega)", "sum(sigma)", "sum(hour_)").withColumn("timestamp", F.lit(row1["max(timestamp)"])).withColumnRenamed("sum(open_interest)", "total_open_interest").withColumnRenamed("sum(open_interest_usd)", "total_open_interest_usd"))

    try:
        put_call_ratio_underlying = agg_prices_market_total_puts_underlying.collect()[0]['total_open_interest'] / agg_prices_market_total_calls_underlying.collect()[0]['total_open_interest']
    except Exception:
        put_call_ratio_underlying = 0.0
    print(put_call_ratio_underlying)

    agg_prices_market_total_df_underlying = agg_prices_market_total_df_underlying.withColumn("put_call_ratio", F.lit(put_call_ratio_underlying).cast("double"))
    agg_prices_market_total_df_underlying.withColumn("put_call_ratio", agg_prices_market_total_df_underlying.put_call_ratio.cast('double'))
    agg_prices_market_total_df_underlying.show()

    try:
        result = fs.get_table(table_name)
        print(result)
        print('Table Already Exists...')
    except Exception:
        print('Creating New Table...')
        fs.create_table(
            name=table_name,
            primary_keys="timestamp",
            df=agg_prices_market_total_df_underlying,
            description=f"Aggregated total open interest {underlying}",
        )

    # Write new results to table
    fs.write_table(
      name=table_name,
      df=agg_prices_market_total_df_underlying,
      mode="merge",
    )

    fs.publish_table(
      name=table_name,
      online_store=online_store,
      mode='merge'
    )

# COMMAND ----------

# DBTITLE 1,Perp 24h Volume (zetadex_feature_store.agg_trades_24h_rolling_perp_<underlying>)
for underlying in underlyings:
    table_name = "zetadex_feature_store.agg_trades_24h_rolling_perp_" + underlying.lower()
    print(f"Underlying: {underlying}")
    print(f"Table Name: {table_name}")

    # On the fly transformation to get the last 24hrs volume from our 1h agg'ed tables
    agg_trades_24h_rolling_df_perp_underlying = \
        (spark.table("zetadex_mainnet.agg_trades_market_1h")
         .filter(F.col("timestamp_window.start") >= (F.date_trunc("hour", F.current_timestamp()) - F.expr('INTERVAL 24 HOUR')))
         .filter(F.col("underlying") == underlying)
         .filter(F.col("kind") == "perp")
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
    agg_trades_24h_rolling_df_perp_underlying.show()

    try:
        result = fs.get_table(table_name)
        print(result)
        print('Table Already Exists...')
    except Exception:
        print('Creating New Table...')
        fs.create_table(
            name=table_name,
            primary_keys=["timestamp"],
            df=agg_trades_24h_rolling_df_perp_underlying,
            partition_columns="date_",
            description=f"Rolling 24hr trade summary metrics {underlying}",
        )

    # Write new results to table
    fs.write_table(
      name=table_name,
      df=agg_trades_24h_rolling_df_perp_underlying,
      mode="merge",
    )

    fs.publish_table(
      name=table_name,
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

# DBTITLE 1,Perp OI (zetadex_feature_store.agg_prices_market_oi_perp_<underlying>)
for underlying in underlyings:
    table_name = 'zetadex_feature_store.agg_prices_market_oi_perp_' + underlying.lower()
    print(f"Underlying: {underlying}")
    print(f"Table Name: {table_name}")

    row1 = (spark.table("zetadex_mainnet.agg_prices_market_1h").agg({"timestamp": "max"}).collect())[0]
    print(row1["max(timestamp)"])

    agg_prices_market_total_df_underlying = (spark.table("zetadex_mainnet.agg_prices_market_1h")
        .filter(F.col("kind") == "perp")
        .filter(F.col("timestamp") == row1["max(timestamp)"])
        .filter(F.col("underlying") == underlying).groupBy().sum("open_interest", "open_interest_usd").drop("sum(strike)", "sum(theo)", "sum(delta)", "sum(vega)", "sum(sigma)", "sum(hour_)").withColumn("timestamp", F.lit(row1["max(timestamp)"])).withColumnRenamed("sum(open_interest)", "total_open_interest").withColumnRenamed("sum(open_interest_usd)", "total_open_interest_usd"))

    agg_prices_market_total_df_underlying.show()

    try:
        result = fs.get_table(table_name)
        print(result)
        print('Table Already Exists...')
    except Exception:
        print('Creating New Table...')
        fs.create_table(
            name=table_name,
            primary_keys="timestamp",
            df=agg_prices_market_total_df_underlying,
            description=f"Aggregated total open interest {underlying}",
        )

    # Write new results to table
    fs.write_table(
      name=table_name,
      df=agg_prices_market_total_df_underlying,
      mode="merge",
    )

    fs.publish_table(
      name=table_name,
      online_store=online_store,
      mode='merge'
    )

# COMMAND ----------

# DBTITLE 1,Rewards: Maker/Taker/Referee/Referrer Agg Epoch User
tables = ["maker", "taker", "referee", "referrer"]
for item in tables:
    table_name = f"zetadex_feature_store.agg_{item}_rewards_epoch_user"
    spark_table_name = f"zetadex_mainnet.agg_{item}_rewards_epoch_user"
    row1 = (spark.table(spark_table_name).agg({"epoch": "max"}).collect())[0]
    print(row1["max(epoch)"])
    if item == "maker" or item == "taker":
        user = "user"
    else:
        user = item

    latest_rewards_df = \
    (spark.table(spark_table_name).filter(F.col("epoch") == row1["max(epoch)"]).withColumnRenamed(user, "pubkey"))



    latest_rewards_df.show()
    try:
        print('Creating New Table...')
        fs.create_table(
            name=table_name,
            primary_keys="pubkey",
            df=latest_rewards_df,
            description=f"Latest Agg {item} Rewards Epoch User"
        )
    except Exception:
        print(f'Table ({table_name}) Already Exists...')

    # Write new results to table
    fs.write_table(
        name=table_name,
        df=latest_rewards_df,
        mode="merge",
    )

    fs.publish_table(
        name=table_name,
        online_store=online_store,
        mode='merge'
    )



# COMMAND ----------

# DBTITLE 1,Rewards: Maker/Taker/Referee/Referrer Agg Epoch User Historical
tables = ["maker", "taker", "referee", "referrer"]
for item in tables:
    table_name = f"zetadex_feature_store.agg_{item}_rewards_epoch_user_historical"
    spark_table_name = f"zetadex_mainnet.agg_{item}_rewards_epoch_user"

    if item == "maker" or item == "taker":
        user = "user"
    else:
        user = item

    historical_rewards_df = \
    (spark.table(spark_table_name)
    .withColumn("pubkey_epoch", F.concat(F.col(user), F.lit("#"), F.unix_timestamp(F.col("epoch"))))
    .withColumnRenamed(user, "pubkey"))

    historical_rewards_df.show()
    
    try:
        print('Creating New Table...')
        fs.create_table(
            name=table_name,
            primary_keys="pubkey_epoch",
            df=historical_rewards_df,
            description=f"Rewards Epoch {item} User Historical Data"
        )
    except Exception:
        print(f'Table ({table_name}) Already Exists...')



    # Write new results to table
    fs.write_table(
        name=table_name,
        df=historical_rewards_df,
        mode="merge",
    )

    fs.publish_table(
        name=table_name,
        online_store=online_store,
        mode='merge'
    )


# COMMAND ----------

# DBTITLE 1,Flex TVL (zetadex_feature_store.zetaflex_cleaned_tvl)
table_name = "zetadex_feature_store.zetaflex_cleaned_tvl"

row1 = (spark.table("zetaflex_mainnet.cleaned_tvl").agg({"timestamp": "max"}).collect())[0]

print(row1["max(timestamp)"])

flex_tvl_df = \
(spark.table("zetaflex_mainnet.cleaned_tvl").filter(F.col("timestamp") == row1["max(timestamp)"]).withColumn("ddb_key", F.lit("flex_tvl")))

flex_tvl_df.show()

try:
    result = fs.get_table(table_name)
    print('Table Already Exists...')
except Exception:
    print('Creating New Table...')
    fs.create_table(
        name=table_name,
        primary_keys="ddb_key",
        df=flex_tvl_df,
        description="Zetaflex Current TVL",
    )

# Write new results to table
fs.write_table(
  name=table_name,
  df=flex_tvl_df,
  mode="merge",
)

fs.publish_table(
  name=table_name,
  online_store=online_store,
  mode='merge'
)

# COMMAND ----------


