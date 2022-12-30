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

MARKET_MAKER_TABLE = "market_maker_quotes"

# COMMAND ----------

S3_BUCKET_LANDED = f"zetadex-{NETWORK}-landing"
BASE_PATH_LANDED = join("/mnt", S3_BUCKET_LANDED)
S3_BUCKET_TRANSFORMED = f"zetadex-{NETWORK}"
BASE_PATH_TRANSFORMED = join("/mnt", S3_BUCKET_TRANSFORMED)

# COMMAND ----------

# MAGIC %md
# MAGIC # ETL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quotes

# COMMAND ----------

# DBTITLE 1,Silver
@dlt.table(
    comment="Cleaned data for market maker quotes",
    table_properties={
        "quality": "silver",
        #"pipelines.autoOptimize.zOrderCols": "timestamp",
    },
    partition_cols=["underlying","expiry","strike","kind"],
    path=join(BASE_PATH_TRANSFORMED, MARKET_MAKER_TABLE, "cleaned"),
)
def cleaned_market_maker_quotes():
    cleaned_ix_place_order_geyser = spark.table('zetadex_mainnet.cleaned_ix_place_order_geyser')
    cleaned_ix_order_complete_geyser = spark.table('zetadex_mainnet.cleaned_ix_order_complete_geyser')
    pubkey_label = spark.table('zetadex_mainnet.pubkey_label')

    orders = cleaned_ix_place_order_geyser.alias('a').filter("date_ >= '2022-11-01'").join(
        cleaned_ix_order_complete_geyser.alias('b'),
        ['date_', 'underlying', 'expiry', 'strike', 'kind', 'side', 'order_id'],
        how='inner'
    ).join(
        pubkey_label.alias('l'),
        F.expr(
          """
          a.accounts['authority'] = l.pub_key
          """
        ),
        how='inner'
    ).select(
        'l.pub_key',
        'l.label',
        'a.underlying',
        'a.expiry',
        'a.strike',
        'a.kind',
        'a.side',
        'a.price',
        'a.size',
        F.col('a.block_time').alias('start_time'),
        F.col('b.block_time').alias('end_time'),
        'a.date_'
    )

    quotes = orders.alias('b').join(
        orders.alias('a'),
        F.expr(
          '''
            b.pub_key = a.pub_key
            and b.date_ = a.date_
            and b.underlying = a.underlying
            and b.expiry = a.expiry
            and b.strike = a.strike
            and b.kind = a.kind
            and b.size = a.size
            and b.side = 'bid'
            and a.side = 'ask'
            and (a.start_time < b.end_time and a.end_time > b.start_time)
          '''
        ),
        how='inner'
    ).select(
        'b.pub_key',
        'b.label',
        'b.underlying',
        'b.expiry',
        'b.strike',
        'b.kind',
        F.col('b.size').alias('bid_size'),
        F.col('a.size').alias('ask_size'),
        (F.col('b.size') * F.col('b.price')).alias('bid_size_usd'),
        (F.col('a.size') * F.col('a.price')).alias('ask_size_usd'),
        F.col('b.price').alias('bid_price'),
        F.col('a.price').alias('ask_price'),
        (F.col('a.price') - F.col('b.price')).alias('spread'),
        ((F.col('a.price') - F.col('b.price')) / ((F.col('a.price') + F.col('b.price')) / 2) * 100).alias('spread_basis_points'),
        F.greatest('b.start_time', 'a.start_time').alias('start_quote'),
        F.least('b.end_time', 'a.end_time').alias('end_quote'),
        (F.least('b.end_time', 'a.end_time') - F.greatest('b.start_time', 'a.start_time')).alias('quote_duration'),
        'b.date_'
    )

    w_interval = Window.partitionBy('pub_key','underlying','expiry','strike','kind').orderBy("dt")
    intervals = quotes.select(
        'pub_key',
        'underlying',
        'expiry',
        'strike',
        'kind',
        F.col('start_quote').alias('dt'),
        'date_'
    ).union(
        quotes.select(
          'pub_key',
          'underlying',
          'expiry',
          'strike',
          'kind',
          F.col('end_quote').alias('dt'),
          'date_'
        )
    ).select(
        'pub_key',
        'underlying',
        'expiry',
        'strike',
        'kind',
        F.lag('dt').over(w_interval).alias('start_interval'),
        F.col('dt').alias('end_interval'),
        'date_'
    ).cache()

    merged_quotes = intervals.alias('t').join(
        quotes.alias('q'),
        F.expr(
              '''
                t.date_ = q.date_
                and t.pub_key = q.pub_key
                and t.underlying = q.underlying
                and t.expiry = q.expiry
                and t.strike = q.strike
                and t.kind = q.kind
                and t.start_interval < q.end_quote and t.end_interval > q.start_quote
              '''
        ),
        how='inner'
    ).groupBy(
        F.greatest('q.start_quote', 't.start_interval').alias('start_quote_interval'), 
        F.least('q.end_quote', 't.end_interval').alias('end_quote_interval'),
        (F.least('q.end_quote', 't.end_interval') - F.greatest('q.start_quote', 't.start_interval')).alias('quote_duration_interval'),
        'q.pub_key',
        'q.label',
        'q.underlying',
        'q.expiry',
        'q.strike',
        'q.kind'
    ).agg(
        F.sum('bid_size').alias('total_bid_size'),
        F.sum('ask_size').alias('total_ask_size'),
        F.sum('bid_size_usd').alias('total_bid_size_usd'),
        F.sum('ask_size_usd').alias('total_ask_size_usd'),
        F.when((F.sum('bid_size_usd') >= 12500) & (F.sum('ask_size_usd') >= 12500), True).otherwise(False).alias('quote_meets_volume_criteria'),
        (F.sum(F.col('spread') * (F.col('bid_size')+F.col('ask_size'))) / F.sum(F.col('bid_size')+F.col('ask_size'))).alias('size_weighted_mean_spread')
    )
    return (
      merged_quotes.alias('q').select(
        'q.*',
        (F.col('q.size_weighted_mean_spread') / (((F.col('q.total_ask_size_usd') / F.col('q.total_ask_size')) + (F.col('q.total_bid_size_usd') / F.col('q.total_bid_size'))) / 2) * 100).alias('size_weight_spread_basis_points'),
        F.when(((F.col('q.size_weighted_mean_spread') / (((F.col('q.total_ask_size_usd') / F.col('q.total_ask_size')) + (F.col('q.total_bid_size_usd') / F.col('q.total_bid_size'))) / 2) * 100)) <= 40, True).otherwise(False).alias('quote_meets_spread_criteria')
      )
    )
