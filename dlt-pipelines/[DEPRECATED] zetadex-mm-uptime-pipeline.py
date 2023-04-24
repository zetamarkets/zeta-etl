# Databricks notebook source
dbutils.widgets.dropdown("network", "devnet", ["devnet", "mainnet"], "Network")
# NETWORK = dbutils.widgets.get("network")
NETWORK = spark.conf.get("pipeline.network")
print(spark.conf)
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from os.path import join
import dlt

# COMMAND ----------

MARKET_MAKER_UPTIME_TABLE = "market_maker_uptime"

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
# MAGIC ## Orders

# COMMAND ----------

# DBTITLE 1,Silver
@dlt.table(
    comment="Cleaned orders combining placed and completed orders",
    table_properties={
        "quality" : "silver",
        "pipelines.autoOptimize.zOrderCols" : "date_"
    },
    partition_cols=["date_","underlying","kind"],
    path=join(BASE_PATH_TRANSFORMED, MARKET_MAKER_UPTIME_TABLE, "orders"),
    temporary="true"
)

def cleaned_mm_completed_orders():
    cleaned_ix_place_order = spark.table('zetadex_mainnet.cleaned_ix_place_order')
    cleaned_ix_complete_order = spark.table('zetadex_mainnet.cleaned_ix_order_complete')
    pubkey_label = spark.table('zetadex_mainnet.pubkey_label')
    
    orders = cleaned_ix_place_order.alias('a').filter("date_ = date_add(current_date(),-1)").join(
        cleaned_ix_complete_order.alias('b'),
        F.expr(
            """
            a.order_id = b.order_id
			and a.side = b.side
			and a.kind = b.kind
			and a.strike = b.strike
			and a.underlying = b.underlying
			and coalesce(a.expiry,timestamp('9999-12-31 23:59:59')) = coalesce(b.expiry,timestamp('9999-12-31 23:59:59'))
			and a.date_ = b.date_
            """
        ),
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
    
    return orders

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Quotes

# COMMAND ----------

# DBTITLE 1,Silver
@dlt.table(
    comment="Cleaned market maker quotes",
    table_properties={
        "quality" : "silver",
        "pipelines.autoOptimize.zOrderCols" : "date_",
    },
    partition_cols=["date_","underlying","kind"],
    path=join(BASE_PATH_TRANSFORMED, MARKET_MAKER_UPTIME_TABLE, "quotes"),
    temporary="true"
)

def cleaned_mm_quotes():
    orders = dlt.read("cleaned_mm_completed_orders")
    return (
        orders.alias('b').join(
            orders.alias('a'),
            F.expr(
              '''
                b.pub_key = a.pub_key
                and b.date_ = a.date_
                and b.underlying = a.underlying
                and coalesce(b.expiry,timestamp('9999-12-31 23:59:59')) = coalesce(a.expiry,timestamp('9999-12-31 23:59:59'))
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
            ((F.col('a.price') - F.col('b.price')) / ((F.col('a.price') + F.col('b.price')) / 2) * 10000).alias('spread_basis_points'),
            F.greatest('b.start_time', 'a.start_time').alias('start_quote'),
            F.least('b.end_time', 'a.end_time').alias('end_quote'),
            (F.least('b.end_time', 'a.end_time') - F.greatest('b.start_time', 'a.start_time')).alias('quote_duration'),
            'b.date_'
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Intervals

# COMMAND ----------

# DBTITLE 1,Silver
@dlt.table(
    comment="Cleaned quote intervals",
    table_properties={
        "quality" : "silver",
        "pipelines.autoOptimize.zOrderCols" : "date_",
    },
    partition_cols=["date_","underlying","kind"],
    path=join(BASE_PATH_TRANSFORMED, MARKET_MAKER_UPTIME_TABLE, "intervals"),
    temporary="true"
)

def cleaned_mm_quote_intervals():
    w_interval = Window.partitionBy('pub_key','underlying','expiry','strike','kind').orderBy("dt")
    quotes = dlt.read('cleaned_mm_quotes')
    return (
        quotes.select(
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
    )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Uptime

# COMMAND ----------

@dlt.table(
    comment="Cleaned merged quotes",
    table_properties={
        "quality" : "silver",
        "pipelines.autoOptimize.zOrderCols" : "date_",
    },
    partition_cols=["date_","underlying","kind"],
    path=join(BASE_PATH_TRANSFORMED, MARKET_MAKER_UPTIME_TABLE, "merged-quotes"),
)

def cleaned_mm_uptime():
    intervals = dlt.read('cleaned_mm_quote_intervals')
    quotes = dlt.read('cleaned_mm_quotes')
    w_merged_quotes = Window.partitionBy('q.pub_key','q.underlying','q.expiry','q.strike','q.kind',F.greatest('start_quote', 'start_interval'),F.least('end_quote', 'end_interval'))
    
    merged_quotes = intervals.alias('t').join(
            quotes.alias('q'),
            F.expr(
                  '''
                    t.date_ = q.date_
                    and t.pub_key = q.pub_key
                    and t.underlying = q.underlying
                    and coalesce(t.expiry,timestamp('9999-12-31 23:59:59')) = coalesce(q.expiry,timestamp('9999-12-31 23:59:59'))
                    and t.strike = q.strike
                    and t.kind = q.kind
                    and t.start_interval < q.end_quote and t.end_interval > q.start_quote
                  '''
            ),
            how='inner'
        ).select(
        F.greatest('start_quote', 'start_interval').alias('start_quote_interval'),
        F.least('end_quote', 'end_interval').alias('end_quote_interval'),
        (F.greatest('start_quote', 'start_interval') - F.least('end_quote', 'end_interval')).alias('quote_duration_interval'),
        'q.*',
        (((F.col('q.bid_size_usd')+F.col('q.ask_size_usd')) / F.sum(F.col('q.bid_size_usd')+F.col('q.ask_size_usd')).over(w_merged_quotes)) * F.col('q.spread_basis_points')).alias('size_weighted_spread_bps'),
        ((F.col('q.bid_size_usd') / F.sum(F.col('q.bid_size_usd')).over(w_merged_quotes)) * F.col('q.bid_price')).alias('size_weighted_bid_price'),
        ((F.col('q.ask_size_usd') / F.sum(F.col('q.ask_size_usd')).over(w_merged_quotes)) * F.col('q.ask_price')).alias('size_weighted_ask_price')
        )
    
    return (
        merged_quotes.alias('a').groupBy(
            'a.date_'
            , 'a.start_quote_interval'
            , 'a.end_quote_interval'
            , 'a.quote_duration_interval'
            , 'a.pub_key'
            , 'a.label'
            , 'a.underlying'
            , 'a.expiry'
            , 'a.strike'
            , 'a.kind'
        ).agg(
            F.sum('a.size_weighted_spread_bps').alias('size_weighted_spread_bps')
            , F.sum('a.size_weighted_ask_price').alias('size_weighted_ask_price')
            , F.sum('a.size_weighted_bid_price').alias('size_weighted_bid_price')
            , F.count('*').alias('quotes')
            , F.sum('a.bid_size').alias('total_bid_size')
            , F.sum('a.ask_size').alias('total_ask_size')
            , F.sum('a.bid_size_usd').alias('total_bid_size_usd')
            , F.sum('a.ask_size_usd').alias('total_ask_size_usd')
            , F.when((F.sum('bid_size_usd') >= 12500) & (F.sum('ask_size_usd') >= 12500), True).otherwise(False).alias('quote_meets_volume_criteria')
            , F.when(F.sum('size_weighted_spread_bps') <= 30, True).otherwise(False).alias('quote_meets_spread_criteria')
        )
    )
