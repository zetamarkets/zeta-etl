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

PRICE_FACTOR = 1e6
SIZE_FACTOR = 1e3

TRANSACTIONS_TABLE = "transactions-geyser"

# COMMAND ----------

# MAGIC %md
# MAGIC # ETL

# COMMAND ----------

S3_BUCKET_LANDED = f"zetadex-{NETWORK}-landing"
BASE_PATH_LANDED = join("/mnt", S3_BUCKET_LANDED)
S3_BUCKET_TRANSFORMED = f"zetadex-{NETWORK}"
BASE_PATH_TRANSFORMED = join("/mnt", S3_BUCKET_TRANSFORMED)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transactions

# COMMAND ----------

transactions_schema = """
signature string,
instructions array<
    struct<
        name string,
        args map<string,string>,
        accounts struct<
            named map<string, string>,
            remaining array<string>
        >,
        events array<
            struct<
                name string,
                event map<string,string>
            >
        >
    >
>,
is_successful boolean,
slot bigint,
block_time timestamp,
year string,
month string,
day string,
hour string
"""

df = spark.read.schema(transactions_schema).json("/mnt/zetadex-mainnet-landing/transactions-geyser/data")

# COMMAND ----------

df.select("block_time").select(F.min("block_time"), F.max("block_time")).show()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC - ~~deposit~~
# MAGIC - ~~withdraw~~
# MAGIC - ~~place_order (v1,2,3)~~
# MAGIC - ~~cancel_order (cancelOrder, cancelOrderHalted, cancelOrderByClientOrderId, cancelExpiredOrder, cancelOrderNoError, cancelOrderByClientOrderIdNoError, forceCancelOrders)~~
# MAGIC - ~~liquidate~~
# MAGIC - ~~positionMovement~~
# MAGIC - ~~settlePosition~~
# MAGIC - ~~crankEventQueue (for trade events)~~
# MAGIC 
# MAGIC I think java `long`s are 8 byte signed a.k.a 32 bit unsigned equivalent (so comparable to u32 in rust). For u64, u128 we should probably just use strings.
# MAGIC https://spark.apache.org/docs/latest/sql-ref-datatypes.html

# COMMAND ----------

# DBTITLE 1,Deposit
deposit_df = (df.filter("is_successful")
        .withColumn("instruction", F.explode("instructions"))
        .filter("instruction.name == 'deposit'")
        .select(
            "signature",
            "instruction.name",
            (F.col("instruction.args.amount").cast("decimal") / PRICE_FACTOR).alias("amount"),
            F.col("instruction.accounts.named").alias("accounts"),
            "slot",
            "block_time"
        )
)
display(deposit_df)

# COMMAND ----------

# DBTITLE 1,Withdraw
withdraw_df = (df.filter("is_successful")
        .withColumn("instruction", F.explode("instructions"))
        .filter("instruction.name == 'withdraw'")
        .select(
            "signature",
            "instruction.name",
            (F.col("instruction.args.amount").cast("decimal") / PRICE_FACTOR).alias("amount"),
            F.col("instruction.accounts.named").alias("accounts"),
            "slot",
            "block_time"
        )
)
display(withdraw_df)

# COMMAND ----------

# DBTITLE 1,Place Order
place_order_df = (df.filter("is_successful")
        .withColumn("instruction", F.explode("instructions"))
        .filter(F.col("instruction.name").startswith("place_order"))
        .withColumn("event", F.col("instruction.events")[0])
        .select(
            "signature",
            "instruction.name",
            (F.col("instruction.args.price").cast("decimal") / PRICE_FACTOR).alias("price"),
            (F.col("instruction.args.size").cast("decimal") / SIZE_FACTOR).alias("size"),
            "instruction.args.side",
            "instruction.args.order_type",
            "instruction.args.client_order_id",
            (F.col("event.event.fee").cast("decimal") / PRICE_FACTOR).alias("fee"),
            (F.col("event.event.oracle_price").cast("decimal") / PRICE_FACTOR).alias("oracle_price"),
            "event.event.order_id",
            F.col("instruction.accounts.named").alias("accounts"),
            "slot",
            "block_time"
        )
)
display(place_order_df)

# COMMAND ----------

# DBTITLE 1,Cancel Order
cancel_order_df = (df.filter("is_successful")
        .withColumn("instruction", F.explode("instructions"))
        .filter(F.col("instruction.name").contains("cancel_order"))
        .select(
            "signature",
            "instruction.name",
            "instruction.args.side",
            "instruction.args.order_id",
            "instruction.args.client_order_id",
            F.col("instruction.accounts.named").alias("accounts"),
            "slot",
            "block_time"
        )
)
display(cancel_order_df)

# COMMAND ----------

# DBTITLE 1,Liquidate
liquidate_df = (df.filter("is_successful")
        .withColumn("instruction", F.explode("instructions"))
        .filter("instruction.name == 'liquidate'")
        .withColumn("event", F.col("instruction.events")[0])
        .select(
            "signature",
            "instruction.name",
            (F.col("instruction.args.size").cast("decimal") / SIZE_FACTOR).alias("size"),
            (F.col("event.event.liquidator_reward").cast("decimal") / PRICE_FACTOR).alias("liquidator_reward"),
            (F.col("event.event.insurance_reward").cast("decimal") / PRICE_FACTOR).alias("insurance_reward"),
            (F.col("event.event.cost_of_trades").cast("decimal") / PRICE_FACTOR).alias("cost_of_trades"),
            (F.col("event.event.size").cast("decimal") / SIZE_FACTOR).alias("size"), # duplicate of the args?
            (F.col("event.event.remaining_liquidatee_balance").cast("decimal") / PRICE_FACTOR).alias("remaining_liquidatee_balance"),
            (F.col("event.event.remaining_liquidator_balance").cast("decimal") / PRICE_FACTOR).alias("remaining_liquidator_balance"),
            (F.col("event.event.mark_price").cast("decimal") / PRICE_FACTOR).alias("mark_price"),
            (F.col("event.event.underlying_price").cast("decimal") / PRICE_FACTOR).alias("underlying_price"),
            F.col("instruction.accounts.named").alias("accounts"),
            "slot",
            "block_time"
        )
)
display(liquidate_df)

# COMMAND ----------

# DBTITLE 1,Position Movement
position_movement_df = (df.filter("is_successful")
        .withColumn("instruction", F.explode("instructions"))
        .filter("instruction.name == 'position_movement'")
        .withColumn("event", F.col("instruction.events")[0])
        .select(
            "signature",
            "instruction.name",
            "instruction.args.movement_type",
            "instruction.args.movements", # need to parse this https://github.com/zetamarkets/zeta-options/blob/a273907e8d6e4fb44fc2c05c5e149d66e89b08cc/zeta/programs/zeta/src/context.rs#L1280-L1284
            (F.col("event.event.net_balance_transfer").cast("decimal") / PRICE_FACTOR).alias("net_balance_transfer"),
            (F.col("event.event.margin_account_balance").cast("decimal") / PRICE_FACTOR).alias("margin_account_balance"),
            (F.col("event.event.spread_account_balance").cast("decimal") / PRICE_FACTOR).alias("spread_account_balance"),
            (F.col("event.event.movement_fees").cast("decimal") / PRICE_FACTOR).alias("movement_fees"),
            F.col("instruction.accounts.named").alias("accounts"),
            "slot",
            "block_time"
        )
)
display(position_movement_df)

# COMMAND ----------

# DBTITLE 1,Settle Positions
settle_positions_df = (df.filter("is_successful")
        .withColumn("instruction", F.explode("instructions"))
        .filter("instruction.name == 'settle_positions'")
        .select(
            "signature",
            "instruction.name",
            F.col("instruction.args.expiry_ts").cast("timestamp").alias("expiry_ts"),
            F.col("instruction.accounts.named").alias("accounts"),
            "slot",
            "block_time"
        )
)
display(settle_positions_df)

# COMMAND ----------

# DBTITLE 1,Trade Event (Crank)
trade_event_df = (df.filter("is_successful")
        .withColumn("instruction", F.explode("instructions"))
        .filter("instruction.name == 'crank_event_queue'")
        .withColumn("event", F.explode("instruction.events")) # use `explode_outer` is you want ixs without events
        .select(
            "signature",
            "event.name",
            "event.event.margin_account",
            ((F.col("event.event.cost_of_trades").cast("decimal") / F.col("event.event.size").cast("decimal")) / (PRICE_FACTOR/SIZE_FACTOR)).alias("price"),
            (F.col("event.event.size").cast("decimal") / SIZE_FACTOR).alias("size"),
#             (F.col("event.event.cost_of_trades").cast("decimal") / PRICE_FACTOR).alias("cost_of_trades"),
            F.when(F.col("event.event.is_bid").cast("boolean"), "bid").otherwise("ask").alias("side"),
            F.col("event.event.index").cast("smallint").alias("market_index"),
            "event.event.client_order_id",
            "event.event.order_id",
            F.col("instruction.accounts.named").alias("accounts"),
            "slot",
            "block_time"
        )
)
display(trade_event_df)

# COMMAND ----------


