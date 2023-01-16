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

AMPLITUDE_TABLE = "amplitude_events"

# COMMAND ----------

# MAGIC %md
# MAGIC # ETL

# COMMAND ----------

S3_BUCKET_LANDED = "amplitude-export-186689/410206/"
BASE_PATH_LANDED = join("/mnt", S3_BUCKET_LANDED)
S3_BUCKET_TRANSFORMED = f"zetadex-{NETWORK}"
BASE_PATH_TRANSFORMED = join("/mnt", S3_BUCKET_TRANSFORMED)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Amplitude Events

# COMMAND ----------

# DBTITLE 1,Bronze
amplitude_events_schema = """
amplitude_id string,
city string,
client_event_time timestamp,
client_upload_time timestamp,
country string,
data_type string,
device_family string,
device_id string,
device_model string,
device_type string,
dma string,
event_id string,
event_properties struct<
    asset string,
    destination_asset string,
    is_favourite boolean,
    kind string,
    leverage double,
    market string,
    orderType string,
    price double,
    side string,
    size double,
    source_asset string,
    transfer_amount_usdc string,
    wallet_address string,
    wallet_provider string,
    withdraw_amount_usdc string,
    withdrawable_amount_usdc long,
    deposit_amount_usdc string,
    wallet_amount_usdc long
>,
event_time timestamp,
event_type string,
ip_address string,
is_attribution_event boolean,
language string,
os_name string,
os_version string,
platform string,
processed_time timestamp,
region string,
server_received_time timestamp,
server_upload_time timestamp,
session_id string,
source_id string,
user_creation_time timestamp,
user_id string,
user_properties struct<
  initial_utm_medium string,
  initial_referring_domain string,
  initial_utm_content string,
  initial_utm_campaign string,
  initial_twclid string,
  referrer string,
  initial_gclid string,
  initial_utm_source string,
  initial_dclid string,
  initial_wbraid string,
  initial_fbclid string,
  initial_referrer string,
  initial_gbraid string,
  initial_utm_term string,
  initial_msclkid string,
  initial_ttclid string,
  initial_ko_click_id string,
  referring_domain string
>,
uuid string
"""


@dlt.table(
    comment="Raw data for amplitude events",
    table_properties={
        "quality": "bronze",
    },
    path=join(BASE_PATH_TRANSFORMED, AMPLITUDE_TABLE, "raw"),
    schema=amplitude_events_schema,
)
def raw_amplitude_events():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.region", "ap-southeast-1")
        .option("cloudFiles.includeExistingFiles", True)
        .option("cloudFiles.useNotifications", True)
        #.option("partitionColumns", "year,month,day,hour")
        .schema(amplitude_events_schema)
        .load(join(BASE_PATH_LANDED, "*.json.gz"))
    )

# COMMAND ----------

# DBTITLE 1,Silver
@dlt.table(
    comment="Cleaned data for amplitude wallet connects",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "event_time",
    },
    partition_cols=["date_","amplitude_id"]
)
def cleaned_amplitude_wallet_connects():
    
    w_amplitude_id = Window.partitionBy("amplitude_id").orderBy(F.asc('event_time'))
    
    return (
      dlt.read("raw_amplitude_events")
      .withWatermark("event_time", "1 hour")
      .filter(F.expr("lower(event_type) like '%wallet_connect%'"))
      .select(
        'amplitude_id',
        F.when(F.col('event_type') == 'Wallet_Connect',F.col('event_properties.wallet_address')).otherwise(F.split('event_type',' ').getItem(1)).alias('wallet_address'),
        F.when(F.col('event_type') == 'Wallet_Connect',F.col('event_properties.wallet_provider')).otherwise(F.split('event_type',' ').getItem(2)).alias('wallet_provider'),
        F.row_number().over(w_amplitude_id).alias('wallet_connect_sequence'),
        'event_time',
        F.col('event_time').alias('usage_start'),
        F.when(F.isnull(F.lead('event_time').over(w_amplitude_id)),F.to_timestamp(F.lit('9999-12-31 23:59:59'))).otherwise(F.lead('event_time').over(w_amplitude_id)).alias('usage_end'),
        F.to_date(F.col('event_time')).alias('date_')
      )
    )

@dlt.table(
    comment="Cleaned data for amplitude events",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "event_time",
    },
    partition_cols=["date_","amplitude_id"]
)
def cleaned_amplitude_events():
    
    cleaned_amplitude_wallet_connects = (
      dlt.read("cleaned_amplitude_wallet_connects")
      .withWatermark("event_time", "1 hour")
    )
    
    return (
      dlt.read("raw_amplitude_events").alias('a')
      .withWatermark("event_time", "1 hour")
      .join(
        cleaned_amplitude_wallet_connects.alias('c1'),
        F.expr(
          """
          c1.amplitude_id = a.amplitude_id
          and a.event_time >= c1.usage_start
          and a.event_time < c1.usage_end
          """
        ),
        how='left'
      )
      .join(
        cleaned_amplitude_wallet_connects.alias('c2'),
        F.expr(
          '''
          c2.amplitude_id = a.amplitude_id
          and c2.wallet_connect_sequence = 1
          '''
        ),
        how='left'
      )
      .select(
        'a.session_id',
        'a.amplitude_id',
        F.coalesce('c1.wallet_address','c2.wallet_address',F.lit('unknown/not connected')).alias('wallet_address'),
        F.coalesce('c1.wallet_provider','c2.wallet_provider',F.lit('unknown/not connected')).alias('wallet_provider'),
        "a.event_time",
        F.when(F.expr("a.event_type like '%Wallet_Connect%'"),'Wallet_Connect').otherwise(F.col('a.event_type')).alias('event_type'),
        F.col("a.event_properties.side").alias("side"),
        F.col("a.event_properties.asset").alias("asset"),
        F.col("a.event_properties.price").alias("price"),
        F.col("a.event_properties.orderType").alias("order_type"),
        F.col("a.event_properties.size").alias("size"),
        (F.col("a.event_properties.size")*F.col("a.event_properties.price")).alias("amount"),
        F.col("a.event_properties.leverage").alias("leverage"),
        F.col("a.event_properties.kind").alias("kind"),
        F.col("a.event_properties.deposit_amount_usdc").alias("deposit_amount_usdc"),
        F.col("a.event_properties.wallet_amount_usdc").alias("wallet_amount_usdc"),
        F.col("a.event_properties.withdraw_amount_usdc").alias("withdraw_amount_usdc"),
        F.col("a.event_properties.withdrawable_amount_usdc").alias("withdrawable_amount_usdc"),
        F.col("a.event_properties.source_asset").alias("source_asset"),
        F.col("a.event_properties.destination_asset").alias("destination_asset"),
        F.col("a.event_properties.transfer_amount_usdc").alias("transfer_amount_usdc"),
        F.col("a.event_properties.market").alias("market"),
        F.col("a.event_properties.is_favourite").alias("is_favourite"),
        "a.city",
        "a.country",
        "a.region",
        "a.device_family",
        "a.device_model",
        "a.language",
        "a.os_name",
        "a.platform",
        F.to_date(F.col("a.event_time")).alias('date_')
      )
    )
