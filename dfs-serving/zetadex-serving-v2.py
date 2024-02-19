# Databricks notebook source
from pyspark.sql import functions as F, Window as W
import pyspark.sql.types as T
from datetime import datetime, timezone

# COMMAND ----------

current_date = str(datetime.now(timezone.utc).date())
current_hour = datetime.now(timezone.utc).hour
print(current_date)
print(current_hour)

# COMMAND ----------

import boto3
from pyspark.sql.dataframe import DataFrame

def to_camel_case(snake_str):
    components = snake_str.split('_')
    # We capitalize the first letter of each component except the first one
    # with the 'title' method and join them together.
    return components[0] + ''.join(x.title() for x in components[1:])


class ServingClient:
    def __init__(self, region="ap-southeast-1"):
        self.region = region
        self.dynamodb = boto3.resource("dynamodb", region_name=region)

    def create_dynamodb_table(
        self,
        table_name,
        primary_key,
        sort_key=None,
        local_secondary_indexes=[],
        global_secondary_indexes=[],
    ):
        """
        table_name = 'YourTableName'
        primary_key = {'name': 'id', 'type': 'N'}
        sort_key = {'name': 'timestamp', 'type': 'N'}

        local_secondary_indexes = [
            {
                'index_name': 'LSIName',
                'sort_key': {'name': 'lsi_range_key', 'type': 'N'},
                'projection_type': 'ALL'
            }
        ]

        global_secondary_indexes = [
            {
                'index_name': 'GSIName',
                'primary_key': {'name': 'gsi_hash_key', 'type': 'S'},
                'sort_key': {'name': 'gsi_range_key', 'type': 'N'},
                'projection_type': 'ALL',
                'read_capacity_units': 5,
                'write_capacity_units': 5
            }
        ]
        """

        attribute_definitions = [
            {"AttributeName": primary_key["name"], "AttributeType": primary_key["type"]}
        ]

        key_schema = [{"AttributeName": primary_key["name"], "KeyType": "HASH"}]

        if sort_key:
            attribute_definitions.append(
                {"AttributeName": sort_key["name"], "AttributeType": sort_key["type"]}
            )
            key_schema.append({"AttributeName": sort_key["name"], "KeyType": "RANGE"})

        create_table_params = {
            "TableName": table_name,
            "AttributeDefinitions": attribute_definitions,
            "KeySchema": key_schema,
            "BillingMode": "PAY_PER_REQUEST",
        }

        lsi = []
        for index in local_secondary_indexes:
            if index["sort_key"]["name"] not in [a["AttributeName"] for a in attribute_definitions]:
                attribute_definitions.append(
                    {
                        "AttributeName": index["sort_key"]["name"],
                        "AttributeType": index["sort_key"]["type"],
                    }
                )
            projection_type = index.get("projection_type", "ALL")
            projection = {
                "ProjectionType": projection_type,
            }
            if projection_type == "INCLUDE":
                if "non_key_attributes" in index:
                    projection["NonKeyAttributes"] = index.get("non_key_attributes", [])
                else:
                    raise KeyError(
                        "'NonKeyAttributes' must be specified for 'INCLUDE' projections"
                    )
            lsi.append(
                {
                    "IndexName": index["index_name"],
                    "KeySchema": [
                        {
                            "AttributeName": primary_key["name"],
                            "KeyType": "HASH",
                        },
                        {
                            "AttributeName": index["sort_key"]["name"],
                            "KeyType": "RANGE",
                        },
                    ],
                    "Projection": projection,
                }
            )
        if len(lsi) > 0:
            create_table_params["LocalSecondaryIndexes"] = lsi

        gsi = []
        for index in global_secondary_indexes:
            if index["primary_key"]["name"] not in [a["AttributeName"] for a in attribute_definitions]:
                attribute_definitions.extend(
                    [
                        {
                            "AttributeName": index["primary_key"]["name"],
                            "AttributeType": index["primary_key"]["type"],
                        },
                    ]
                )
            if index["sort_key"]["name"] not in [a["AttributeName"] for a in attribute_definitions]:
                attribute_definitions.extend(
                    [
                        {
                            "AttributeName": index["sort_key"]["name"],
                            "AttributeType": index["sort_key"]["type"],
                        },
                    ]
                )
            projection_type = index.get("projection_type", "ALL")
            projection = {
                "ProjectionType": projection_type,
            }
            if projection_type == "INCLUDE":
                if "non_key_attributes" in index:
                    projection["NonKeyAttributes"] = index.get("non_key_attributes", [])
                else:
                    raise KeyError(
                        "'NonKeyAttributes' must be specified for 'INCLUDE' projections"
                    )
            gsi.append(
                {
                    "IndexName": index["index_name"],
                    "KeySchema": [
                        {
                            "AttributeName": index["primary_key"]["name"],
                            "KeyType": "HASH",
                        },
                        {
                            "AttributeName": index["sort_key"]["name"],
                            "KeyType": "RANGE",
                        }
                    ],
                    "Projection": projection
                    # "ProvisionedThroughput": {
                    #     "ReadCapacityUnits": index.get("read_capacity_units"),
                    #     "WriteCapacityUnits": index.get("write_capacity_units"),
                    # },
                }
            )
        if len(gsi) > 0:
            create_table_params["GlobalSecondaryIndexes"] = gsi

        table = self.dynamodb.create_table(**create_table_params)

        print("Creating table, wait a moment...")
        table.meta.client.get_waiter("table_exists").wait(TableName=table_name)
        print(f"Table {table_name} created successfully")
        return table

    def check_table_exists(self, table_name):
        table = self.dynamodb.Table(table_name)
        try:
            table.load()
            return True
        except self.dynamodb.meta.client.exceptions.ResourceNotFoundException:
            return False

    def serve_table(
        self,
        table_name,
        df: DataFrame,
        primary_key,
        sort_key=None,
        local_secondary_indexes=[],
        global_secondary_indexes=[],
        mode="append",
        batch_size=25,
        throughput=100,
        update=False,
    ):
        if not self.check_table_exists(table_name):
            table = self.create_dynamodb_table(
                table_name,
                primary_key,
                sort_key,
                local_secondary_indexes,
                global_secondary_indexes,
            )

        # Convert date/timestamp columns from the native microsecond granularity ones to unix seconds
        timestamp_cols = [
            f.name for f in df.schema.fields if isinstance(f.dataType, T.TimestampType) or isinstance(f.dataType, T.DateType)
        ]
        output_df = df.select(
            [
                F.unix_timestamp(c).alias(c) if c in timestamp_cols else c
                for c in df.columns
            ]
        )

        print(f"Writing {output_df.count()} rows to {table_name}")
        output_df.write.option("tableName", table_name).option(
            "region", self.region
        ).option("writeBatchSize", batch_size).option("update", update).option(
            "throughput", throughput
        ).mode(
            mode
        ).format(
            "dynamodb"
        ).save()

        output_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Serving Routines
# MAGIC
# MAGIC ## User stats
# MAGIC * Funding [margin_account (or asset?), timestamp] (agg_funding_rate_1h)
# MAGIC   * asset|margin_account (P)
# MAGIC   * timestamp (S)
# MAGIC   * authority
# MAGIC   * margin_account|asset
# MAGIC   * balance_change
# MAGIC * PnL [authority, timestamp] (agg_pnl)
# MAGIC   * authority (P)
# MAGIC   * timestamp (S)
# MAGIC   * pnl
# MAGIC   * pnl_diff_24h
# MAGIC   * pnl_diff_7d
# MAGIC   * pnl_diff_30d
# MAGIC   * pnl_ratio_24h
# MAGIC   * pnl_ratio_7d
# MAGIC   * pnl_ratio_30d
# MAGIC   * pnl_ratio_24h_rank
# MAGIC   * pnl_ratio_7d_rank
# MAGIC   * pnl_ratio_30d_rank
# MAGIC   * pnl_diff_24h_rank
# MAGIC   * pnl_diff_7d_rank
# MAGIC   * pnl_diff_30d_rank
# MAGIC
# MAGIC ## Platform stats
# MAGIC * Rolling 24hr stats [asset|ALL_ASSETS, timestamp] (agg_ix_trade_1h)
# MAGIC   * asset|ALL_ASSETS (P)
# MAGIC   * timestamp (S)
# MAGIC   * trades_count
# MAGIC   * volume
# MAGIC * Hourly stats (agg_ix_trade_1h)
# MAGIC   * asset|ALL (P)
# MAGIC   * timestamp (S)
# MAGIC   * mark_price|null
# MAGIC   * open_interest
# MAGIC   * open_interest_notional
# MAGIC   * volume
# MAGIC   <!-- * tvl -->
# MAGIC * Flex TVL?
# MAGIC
# MAGIC ## Rewards
# MAGIC * Maker rewards (agg_maker_rewards_epoch_user)
# MAGIC   * authority (P)
# MAGIC   * epoch (S)
# MAGIC   * maker_tier
# MAGIC   * maker_volume
# MAGIC   * maker_rebate
# MAGIC   * maker_bonus
# MAGIC   * maker_volume_cumsum
# MAGIC   * maker_rebate_cumsum
# MAGIC   * maker_bonus_cumsum
# MAGIC   * maker_total_rewards_cumsum
# MAGIC * Taker rewards (TBD) (agg_taker_rewards_epoch_user)
# MAGIC   * authority (P)
# MAGIC   * epoch (S)
# MAGIC   * taker_volume
# MAGIC   * taker_fee
# MAGIC   * taker_bonus
# MAGIC   * taker_volume_cumsum
# MAGIC   * taker_fee_cumsum
# MAGIC   * taker_bonus_cumsum
# MAGIC   * taker_total_rewards_cumsum
# MAGIC * Referrer (agg_referrer_rewards_epoch_user)
# MAGIC   * authority (P)
# MAGIC   * epoch (S)
# MAGIC   * alias
# MAGIC   * referral_volume
# MAGIC   * referral_volume_30d
# MAGIC   * referral_fee
# MAGIC   * referral_count
# MAGIC   * referrer_tier
# MAGIC   * referrer_fee_rebate
# MAGIC   * referral_volume_cumsum
# MAGIC   * referral_fee_cumsum
# MAGIC   * referrer_fee_rebate_cumsum
# MAGIC * Referee (agg_referee_rewards_epoch_user)
# MAGIC   * authority (P)
# MAGIC   * epoch (S)
# MAGIC   * referrer
# MAGIC   * referrer_alias
# MAGIC   * volume
# MAGIC   * trading_fee
# MAGIC   * referee_fee_rebate
# MAGIC   * volume_cumsum
# MAGIC   * trading_fee_cumsum
# MAGIC   * referee_fee_rebate_cumsum

# COMMAND ----------

client = ServingClient()

# COMMAND ----------

# DBTITLE 1,Funding
table_name = "zetadex_mainnet_tx.agg_funding_rate_user_asset_1h"
primary_key = {"name": "marginAccount", "type": "S"}
sort_key = {"name": "timestamp#asset", "type": "S"}  # composite sort key

local_secondary_indexes = [
    {
        "index_name": "marginAccount-timestamp-index",
        "sort_key": {"name": "timestamp", "type": "N"},
        "projection_type": "ALL",
    }
]

df = (
    spark.table(table_name)
    .filter(
        "timestamp == date_trunc('hour', current_timestamp - interval 2 hour)"  # getting last hour funding because current hour funding is incomplete. Update: need to make it 2hr delay because of agg in gold tables windowing + watermarking?
    )
    .withColumn(
        "timestamp#asset", F.concat_ws("#", F.unix_timestamp("timestamp"), "asset")
    )
)  # composite sort key

# Rename columns to camelCase
for col in df.columns:
    df = df.withColumnRenamed(col, to_camel_case(col))

client.serve_table(table_name+"_v2", df, primary_key, sort_key, local_secondary_indexes=local_secondary_indexes, throughput=100000)

# COMMAND ----------

# DBTITLE 1,PnL
# table_name = "zetadex_mainnet_tx.agg_pnl"
# primary_key = {"name": "marginAccount", "type": "S"}

# df = (
#     spark.table(table_name)
#     .filter("date_ == current_date")
#     .filter("timestamp == date_trunc('hour', current_timestamp)")
#     .select(
#         "timestamp",
#         "authority",
#         "margin_account",
#         "balance",
#         "unrealized_pnl",
#         "equity",
#         "cumulative_pnl",
#         "pnl_24h",
#         "pnl_7d",
#         "pnl_30d",
#         "roi_24h",
#         "roi_7d",
#         "roi_30d",
#         "pnl_24h_rank",
#         "pnl_7d_rank",
#         "pnl_30d_rank",
#         "roi_24h_rank",
#         "roi_7d_rank",
#         "roi_30d_rank",
#         "pnl_24h_rank_change",
#         "pnl_7d_rank_change",
#         "pnl_30d_rank_change",
#         "roi_24h_rank_change",
#         "roi_7d_rank_change",
#         "roi_30d_rank_change",
#     )
# )

# # Rename columns to camelCase
# for col in df.columns:
#     df = df.withColumnRenamed(col, to_camel_case(col))

# client.serve_table(
#     table_name+"_v2",
#     df,
#     primary_key,
#     update=True,
#     throughput=10000,
# )

# COMMAND ----------

# DBTITLE 1,Leaderboard
table_name = "zetadex_mainnet_tx.agg_pnl"
primary_key = {"name": "metric#timePeriod", "type": "S"}
sort_key = {"name": "rank", "type": "N"}

local_secondary_indexes = [
    {
        "index_name": "metric-timePeriod-marginAccount-index",
        "sort_key": {"name": "marginAccount", "type": "S"},
        "projection_type": "ALL",
    }
]

df = (
    spark.table(table_name)
    .filter("date_ == current_date")
    .filter("timestamp == date_trunc('hour', current_timestamp)")
    # .withColumn("max_timestamp", F.max("timestamp").over(W.orderBy()))
    # .filter(F.col("timestamp") == F.col("max_timestamp"))
    # .drop("max_timestamp")
    .select(
        "timestamp",
        "authority",
        "margin_account",
        # "balance",
        # "unrealized_pnl",
        # "equity",
        # "cumulative_pnl",
        # metrics
        "pnl_24h",
        "pnl_7d",
        "pnl_30d",
        "pnl_alltime",
        "roi_24h",
        "roi_7d",
        "roi_30d",
        "pnl_24h_rank",
        "pnl_7d_rank",
        "pnl_30d_rank",
        "pnl_alltime_rank",
        "roi_24h_rank",
        "roi_7d_rank",
        "roi_30d_rank",
        "pnl_24h_rank_change",
        "pnl_7d_rank_change",
        "pnl_30d_rank_change",
        "pnl_alltime_rank_change",
        "roi_24h_rank_change",
        "roi_7d_rank_change",
        "roi_30d_rank_change",
        # z-scores
        "maker_volume_24h",
        "maker_volume_7d",
        "maker_volume_30d",
        "maker_volume_alltime",
        "taker_volume_24h",
        "taker_volume_7d",
        "taker_volume_30d",
        "taker_volume_alltime",
        "z_multiplier_24h",
        "z_multiplier_7d",
        "z_multiplier_30d",
        "z_multiplier_nft",
        "z_multiplier_alltime",
        "z_score_24h",
        "z_score_7d",
        "z_score_30d",
        "z_score_alltime",
        "z_score_24h_rank",
        "z_score_7d_rank",
        "z_score_30d_rank",
        "z_score_alltime_rank",
        "z_score_24h_rank_change",
        "z_score_7d_rank_change",
        "z_score_30d_rank_change",
        "z_score_alltime_rank_change",
    )
)

from functools import reduce

# Fixed columns you don't want to unpivot
id_vars = ["timestamp", "authority", "margin_account"]

# Metrics and periods you're working with
metrics = ["pnl", "roi", "z_score"]
periods = ["24h", "7d", "30d", "alltime"]
period_map = {
    "24h": "TWENTY_FOUR_HOURS",
    "7d": "SEVEN_DAYS",
    "30d": "THIRTY_DAYS",
    "alltime": "ALL_TIME",
}

# Unpivot the DataFrame
dfs = []  # List to collect DataFrames for union later
for metric in metrics:
    for period in periods:
        # no roi alltime rn
        if metric == "roi" and period == "alltime":
            continue
        # Build the metric, time_period, value, and rank columns
        df_temp = (
            df.withColumn(
                "metric#time_period", F.lit(f"{metric.upper()}#{period_map[period]}")
            )
            .withColumn("pnl", F.col(f"pnl_{period}"))
            .withColumn("z_score", F.col(f"z_score_{period}"))
            .withColumn("z_multiplier", F.col(f"z_multiplier_{period}"))
            .withColumn("maker_volume", F.col(f"maker_volume_{period}"))
            .withColumn("taker_volume", F.col(f"taker_volume_{period}"))
            .withColumn("rank", F.col(f"{metric}_{period}_rank"))
            .withColumn("rank_change", F.col(f"{metric}_{period}_rank_change"))
        )

        if period == "alltime":
            df_temp = df_temp.withColumn("roi", F.lit(None))
        else:
            df_temp = df_temp.withColumn("roi", F.col(f"roi_{period}"))

        df_temp = df_temp.select(
            id_vars
            + [
                "metric#time_period",
                "pnl",
                "roi",
                "z_score",
                "z_multiplier",
                "z_multiplier_nft",
                "maker_volume",
                "taker_volume",
                "rank",
                "rank_change",
            ]
        )  # select necessary columns
        dfs.append(df_temp)

# Combine all the temporary DataFrames
df = reduce(lambda df1, df2: df1.union(df2), dfs)

# Rename columns to camelCase
for col in df.columns:
    df = df.withColumnRenamed(col, to_camel_case(col))

client.serve_table(
    "zetadex_mainnet_tx.leaderboard_v2_s2",
    df,
    primary_key,
    sort_key,
    local_secondary_indexes=local_secondary_indexes,
    update=True,
    throughput=1000000,
)

# COMMAND ----------

# DBTITLE 1,PnL Historical
table_name = "zetadex_mainnet_tx.cleaned_pnl"
primary_key = {"name": "marginAccount", "type": "S"}
sort_key = {"name": "timestamp", "type": "N"}

df = (
    spark.table(table_name)
    .filter("timestamp == date_trunc('hour', current_timestamp)")
    .select(
        "timestamp",
        "authority",
        "margin_account",
        "balance",
        "unrealized_pnl",
        "equity",
        "cumulative_pnl",
    )
)

# Rename columns to camelCase
for col in df.columns:
    df = df.withColumnRenamed(col, to_camel_case(col))

# Hourly
client.serve_table(
    table_name + "_hourly"+"_v2",
    df.withColumn("ttl", F.col("timestamp") + F.expr('INTERVAL 2 WEEKS')), # expire datapoints after 2 weeks
    primary_key,
    sort_key,
    throughput=1000,
)

# Daily
client.serve_table(
    table_name + "_daily"+"_v2",
    df.filter(F.hour("timestamp") == 0),
    primary_key,
    sort_key,
    throughput=100000,
)

# COMMAND ----------

# DBTITLE 1,Exchange Stats
# 24hr Rolling Stats

df_24hr = (
    spark.table("zetadex_mainnet_tx.agg_ix_trade_asset_24h_rolling")
    .filter("timestamp == date_trunc('hour', current_timestamp - interval 2 hour)") # 2 because of the gold table agg being watermarked?
    .filter("asset != 'UNDEFINED'")
    # aggregate up the sums across all assets
    .rollup("asset")
    .agg(
        F.max("timestamp").alias("timestamp"),  # Use the max timestamp
        F.sum("trade_count_24h").alias("trade_count"),
        F.sum("volume_24h").alias("volume"),
        F.when(F.col("asset").isNull(), F.collect_list(F.struct("asset", F.col("trade_count_24h").alias("tradeCount"), F.col("volume_24h").alias("volume")))).alias("components")
    )
    .withColumn("asset#time_period", F.concat_ws("#", F.coalesce("asset", F.lit("ALL_ASSETS")), F.lit("TWENTY_FOUR_HOURS")) )
    .drop("asset")
)
# All Time Stats
# Account for volume and trades not in helius data
V1_VOLUME = 455316870.8536678
v1_TRADE_COUNT = 200322

df_alltime = (
    spark.table("zetadex_mainnet_tx.agg_ix_trade_1h")
    .agg(
        F.max("timestamp").alias("timestamp"),  # Use the max timestamp
        (F.sum("trade_count") + v1_TRADE_COUNT).alias("trade_count"),
        (F.sum("volume") + V1_VOLUME).alias("volume"),
        F.lit(None).alias("components")
    )
    .withColumn("asset#time_period", F.concat_ws("#", F.lit("ALL_ASSETS"), F.lit("ALL_TIME")) )
)

df = df_24hr.union(df_alltime)

# Rename columns to camelCase
for col in df.columns:
    df = df.withColumnRenamed(col, to_camel_case(col))

primary_key = {"name": "asset#timePeriod", "type": "S"}
sort_key = {"name": "timestamp", "type": "N"}

client.serve_table("zetadex_mainnet_tx.stats_v2", df, primary_key, sort_key)

# COMMAND ----------

# DBTITLE 1,Maker Rewards
table_name = "zetadex_mainnet_rewards.agg_maker_rewards_epoch_user"
primary_key = {"name": "authority", "type": "S"}
sort_key = {"name": "epoch", "type": "N"}

df = spark.table(table_name)

# Rename columns to camelCase
for col in df.columns:
    df = df.withColumnRenamed(col, to_camel_case(col))

client.serve_table(table_name+"_v2", df, primary_key, sort_key)

# COMMAND ----------

# DBTITLE 1,Taker Rewards
# table_name = "zetadex_mainnet_rewards.agg_taker_rewards_epoch_user"
# primary_key = {"name": "authority", "type": "S"}
# sort_key = {"name": "epoch", "type": "N"}

# df = spark.table(table_name)

# # Rename columns to camelCase
# for col in df.columns:
#     df = df.withColumnRenamed(col, to_camel_case(col))
 
# client.serve_table(table_name+"_v2", df, primary_key, sort_key)

# COMMAND ----------

# DBTITLE 1,Referrer Rewards
# table_name = "zetadex_mainnet_rewards.agg_referrer_rewards_epoch_user"
# primary_key = {"name": "referrer", "type": "S"}
# sort_key = {"name": "epoch", "type": "N"}

# df = spark.table(table_name)

# # Rename columns to camelCase
# for col in df.columns:
#     df = df.withColumnRenamed(col, to_camel_case(col))

# client.serve_table(table_name+"_v2", df, primary_key, sort_key)

# COMMAND ----------

# DBTITLE 1,Referee Rewards
# table_name = "zetadex_mainnet_rewards.agg_referee_rewards_epoch_user"
# primary_key = {"name": "referee", "type": "S"}
# sort_key = {"name": "epoch", "type": "N"}

# df = spark.table(table_name)

# # Rename columns to camelCase
# for col in df.columns:
#     df = df.withColumnRenamed(col, to_camel_case(col))

# client.serve_table(table_name+"_v2", df, primary_key, sort_key)

# COMMAND ----------

table_name = "zetadex_mainnet_tx.fee_tiers"
primary_key = {"name": "authority", "type": "S"}
sort_key = {"name": "timestamp", "type": "N"}

df = spark.table(table_name)

# Rename columns to camelCase
for col in df.columns:
    df = df.withColumnRenamed(col, to_camel_case(col))

client.serve_table(
    "zetadex_mainnet_tx.fee_tiers",
    df,
    primary_key,
    sort_key,
    throughput=100000,
)

# COMMAND ----------

# import concurrent.futures

# def write_to_dynamo(df, table_name, region, batch_size, update, throughput, mode):
#     df.write.option("tableName", table_name).option(
#         "region", region
#     ).option("writeBatchSize", batch_size).option("update", update).option(
#         "throughput", throughput
#     ).mode(
#         mode
#     ).format(
#         "dynamodb"
#     ).save()

# # Assuming dataframes is a list of your dataframes
# dataframes = [df1, df2, df3, ...]

# with concurrent.futures.ThreadPoolExecutor() as executor:
#     for df in dataframes:
#         executor.submit(write_to_dynamo, df, table_name, region, batch_size, update, throughput, mode)
