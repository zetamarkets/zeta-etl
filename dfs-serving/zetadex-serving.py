# Databricks notebook source
import pyspark.sql.functions as F
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
primary_key = {"name": "margin_account", "type": "S"}
sort_key = {"name": "timestamp#asset", "type": "S"}  # composite sort key

local_secondary_indexes = [
    {
        "index_name": "margin_account-timestamp-index",
        "sort_key": {"name": "timestamp", "type": "N"},
        "projection_type": "ALL",
    }
]

df = (
    spark.table(table_name)
    .filter(
        "timestamp == date_trunc('hour', current_timestamp - interval 1 hour)"  # getting last hour funding because current hour funding is incomplete
    )
    .withColumn(
        "timestamp#asset", F.concat_ws("#", F.unix_timestamp("timestamp"), "asset")
    )
)  # composite sort key

client.serve_table(table_name, df, primary_key, sort_key, local_secondary_indexes=local_secondary_indexes)

# COMMAND ----------

# DBTITLE 1,PnL
# table_name = "zetadex_mainnet_tx.agg_pnl"
# primary_key = {"name": "authority", "type": "S"}
# # sort_key = {"name": "timestamp", "type": "N"}

# global_secondary_indexes = []
# for time_period in ["24h", "7d", "30d"]:
#     global_secondary_indexes.extend(
#         [
#             {
#                 "index_name": f"pnl_{time_period}_rank-index",
#                 "hash_key": {"name": f"pnl_{time_period}_rank", "type": "N"},
#                 "projection_type": "INCLUDE",
#                 "non_key_attributes": [
#                     "timestamp",
#                     "cumulative_pnl",
#                     f"pnl_{time_period}",
#                 ],
#             },
#             {
#                 "index_name": f"roi_{time_period}_rank-index",
#                 "hash_key": {"name": f"roi_{time_period}_rank", "type": "N"},
#                 "projection_type": "INCLUDE",
#                 "non_key_attributes": [
#                     "timestamp",
#                     "cumulative_pnl",
#                     f"roi_{time_period}",
#                 ],
#             },
#         ]
#     )

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
#     )
# )

# client.serve_table(
#     table_name,
#     df,
#     primary_key,
#     global_secondary_indexes=global_secondary_indexes,
#     update=True,
#     throughput=1000,
# )

# COMMAND ----------

# DBTITLE 1,PnL Historical
table_name = "zetadex_mainnet_tx.cleaned_pnl"
primary_key = {"name": "authority", "type": "S"}
sort_key = {"name": "timestamp", "type": "N"}

df = (
    spark.table(table_name)
    .filter("timestamp == date_trunc('hour', current_timestamp)")
    # .filter("timestamp >= current_timestamp - interval 1 week")
    # Temporary hack to get it to work with legacy pnl index
    .groupBy("timestamp","authority")
    .agg(F.sum("balance").alias("balance"), F.sum("unrealized_pnl").alias("unrealized_pnl"), F.sum("equity").alias("equity"), F.sum("cumulative_pnl").alias("cumulative_pnl"))
    .select(
        "timestamp",
        "authority",
        # "margin_account",
        "balance",
        "unrealized_pnl",
        "equity",
        "cumulative_pnl",
    )
)

# Hourly
client.serve_table(
    table_name + "_hourly",
    df.withColumn("ttl", F.col("timestamp") + F.expr('INTERVAL 2 WEEKS')), # expire datapoints after 2 weeks
    primary_key,
    sort_key,
    throughput=1000,
)

# Daily
client.serve_table(
    table_name + "_daily",
    df.filter(F.hour("timestamp") == 0),
    primary_key,
    sort_key,
    throughput=1000,
)

# COMMAND ----------

# DBTITLE 1,Rolling 24H Exchange Stats
table_name = "zetadex_mainnet_tx.agg_ix_trade_asset_24h_rolling"
primary_key = {"name": "asset", "type": "S"}
sort_key = {"name": "timestamp", "type": "N"}

df = (
    spark.table(table_name)
    .filter("timestamp == date_trunc('hour', current_timestamp - interval 1 hour)")
    # aggregate up the sums across all assets
    .rollup("asset")
    .agg(
        F.max("timestamp").alias("timestamp"),  # Use the max timestamp
        F.sum("trade_count_24h").alias("trade_count_24h"),
        F.sum("volume_24h").alias("volume_24h"),
    )
    .withColumn("asset", F.coalesce("asset", F.lit("ALL_ASSETS")))
)

client.serve_table(table_name, df, primary_key, sort_key)

# COMMAND ----------

# DBTITLE 1,1H Exchange Stats
# table_name = "zetadex_mainnet_tx.agg_ix_trade_1h"
# primary_key = {"name": "asset", "type": "S"}
# sort_key = {"name": "timestamp", "type": "N"}

# df = spark.table(table_name) \
#     .withColumn("asset", F.lit("ALL_ASSETS")) \
#     .filter("timestamp == date_trunc('hour', current_timestamp - interval 1 hour)")

# client.serve_table(table_name, df, primary_key, sort_key)

# COMMAND ----------

# DBTITLE 1,Maker Rewards
table_name = "zetadex_mainnet_rewards.agg_maker_rewards_epoch_user"
primary_key = {"name": "authority", "type": "S"}
sort_key = {"name": "epoch", "type": "N"}

df = spark.table(table_name)
client.serve_table(table_name, df, primary_key, sort_key)

# COMMAND ----------

# DBTITLE 1,Taker Rewards
table_name = "zetadex_mainnet_rewards.agg_taker_rewards_epoch_user"
primary_key = {"name": "authority", "type": "S"}
sort_key = {"name": "epoch", "type": "N"}

df = spark.table(table_name)
client.serve_table(table_name, df, primary_key, sort_key)

# COMMAND ----------

# DBTITLE 1,Referrer Rewards
table_name = "zetadex_mainnet_rewards.agg_referrer_rewards_epoch_user"
primary_key = {"name": "referrer", "type": "S"}
sort_key = {"name": "epoch", "type": "N"}

df = spark.table(table_name)

client.serve_table(table_name, df, primary_key, sort_key)

# COMMAND ----------

# DBTITLE 1,Referee Rewards
table_name = "zetadex_mainnet_rewards.agg_referee_rewards_epoch_user"
primary_key = {"name": "referee", "type": "S"}
sort_key = {"name": "epoch", "type": "N"}

df = spark.table(table_name)

client.serve_table(table_name, df, primary_key, sort_key)

# COMMAND ----------

# DBTITLE 1,Mad Wars
table_name = "madwars.pnl_individual"
primary_key = {"name": "authority", "type": "S"}

df = (
    spark.table(table_name)
    .select(
        "timestamp",
        "authority",
        # "balance",
        # "unrealized_pnl",
        # "equity",
        # "cumulative_pnl",
        "pnl",
        "roi",
        "volume",
        "pnl_rank_global",
        "roi_rank_global",
        "volume_rank_global",
        "pnl_rank_team",
        "roi_rank_team",
        "volume_rank_team",
        "team",
        "backpack_username",
        "multiplier",
    )
    .withColumn("_global_gsi", F.lit("GLOBAL"))
)

client.serve_table(table_name, df, primary_key, update=True, throughput=1000)

table_name = "madwars.pnl_team"
primary_key = {"name": "team", "type": "S"}

df = (
    spark.table(table_name)
    .select(
        "timestamp",
        "team",
        # "equity",
        # "cumulative_pnl",
        "pnl",
        "roi",
        "volume",
        "pnl_rank",
        "roi_rank",
        "volume_rank",
    )
    .withColumn("_global_gsi", F.lit("GLOBAL"))
)

client.serve_table(table_name, df, primary_key, update=True)

# COMMAND ----------


