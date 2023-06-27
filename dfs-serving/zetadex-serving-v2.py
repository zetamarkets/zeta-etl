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

class ServingClient:
    def __init__(self, region='ap-southeast-1'):
        self.region = region
        self.dynamodb = boto3.resource('dynamodb', region_name=region)

    def create_dynamodb_table(self, table_name, primary_key, sort_key=None, local_secondary_indexes=None, global_secondary_indexes=None):
        '''
        table_name = 'YourTableName'
        primary_key = {'name': 'id', 'type': 'N'}
        sort_key = {'name': 'timestamp', 'type': 'N'}

        local_secondary_indexes = [
            {
                'index_name': 'LSIName',
                'name': 'lsi_key',
                'type': 'S',
                'projection_type': 'ALL'
            }
        ]

        global_secondary_indexes = [
            {
                'index_name': 'GSIName',
                'hash_key': {'name': 'gsi_hash_key', 'type': 'S'},
                'range_key': {'name': 'gsi_range_key', 'type': 'N'},
                'projection_type': 'ALL',
                'read_capacity_units': 5,
                'write_capacity_units': 5
            }
        ]
        '''

        attribute_definitions = [
            {
                'AttributeName': primary_key['name'],
                'AttributeType': primary_key['type']
            }
        ]

        key_schema = [
            {
                'AttributeName': primary_key['name'],
                'KeyType': 'HASH'
            }
        ]

        if sort_key:
            attribute_definitions.append({
                'AttributeName': sort_key['name'],
                'AttributeType': sort_key['type']
            })
            key_schema.append({
                'AttributeName': sort_key['name'],
                'KeyType': 'RANGE'
            })

        if local_secondary_indexes:
            for index in local_secondary_indexes:
                attribute_definitions.append({
                    'AttributeName': index['name'],
                    'AttributeType': index['type']
                })

        if global_secondary_indexes:
            for index in global_secondary_indexes:
                attribute_definitions.append({
                    'AttributeName': index['name'],
                    'AttributeType': index['type']
                })

        table = self.dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=attribute_definitions,
            KeySchema=key_schema,
            BillingMode='PAY_PER_REQUEST'
        )

        if local_secondary_indexes:
            lsi = []
            for index in local_secondary_indexes:
                lsi.append({
                    'IndexName': index['index_name'],
                    'KeySchema': [
                        {
                            'AttributeName': primary_key['name'],
                            'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': index['name'],
                            'KeyType': 'RANGE'
                        }
                    ],
                    'Projection': {
                        'ProjectionType': index['projection_type']
                    }
                })
            table.update(AttributeDefinitions=attribute_definitions, LocalSecondaryIndexes=lsi)

        if global_secondary_indexes:
            gsi = []
            for index in global_secondary_indexes:
                gsi.append({
                    'IndexName': index['index_name'],
                    'KeySchema': [
                        {
                            'AttributeName': index['hash_key']['name'],
                            'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': index['range_key']['name'],
                            'KeyType': 'RANGE'
                        }
                    ],
                    'Projection': {
                        'ProjectionType': index['projection_type']
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': index['read_capacity_units'],
                        'WriteCapacityUnits': index['write_capacity_units']
                    }
                })
            table.update(AttributeDefinitions=attribute_definitions, GlobalSecondaryIndexes=gsi)

        print("Creating table, wait a moment...")
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        print(f"Table {table_name} created successfully")
        return table

    def check_table_exists(self, table_name):
        table = self.dynamodb.Table(table_name)
        try:
            table.load()
            return True
        except self.dynamodb.meta.client.exceptions.ResourceNotFoundException:
            return False

    def serve_table(self, table_name, df: DataFrame, primary_key, sort_key=None, mode='append', batch_size=25, throughput=100, update=False):
        if not self.check_table_exists(table_name):
            if sort_key:
                table = self.create_dynamodb_table(table_name, primary_key, sort_key)
            else:
                table = self.create_dynamodb_table(table_name, primary_key)
        
        print(f"Writing {df.count()} rows to {table_name}")
        df.write.option("tableName", table_name) \
              .option('region', self.region) \
              .option('writeBatchSize', batch_size) \
              .option('update', update) \
              .option('throughput', throughput) \
              .mode(mode) \
              .format("dynamodb") \
              .save()

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
# MAGIC * Rolling 24hr stats [asset|ALL, timestamp] (agg_ix_trade_1h)
# MAGIC   * asset|ALL (P)
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
sort_key = {"name": "timestamp", "type": "N"}

df = spark.table(table_name) \
    .filter("timestamp == date_trunc('hour', current_timestamp - interval 1 hour)") # getting last hour funding because current hour funding is incomplete

client.serve_table(table_name, df, primary_key, sort_key)

# COMMAND ----------

# DBTITLE 1,PnL
table_name = "zetadex_mainnet_tx.agg_pnl"
primary_key = {"name": "authority", "type": "S"}
# sort_key = {"name": "timestamp", "type": "N"}
df = spark.table(table_name) \
    .filter("timestamp == date_trunc('hour', current_timestamp)")

client.serve_table(table_name, df, primary_key, update=True, throughput=1000)

# COMMAND ----------

# DBTITLE 1,Rolling 24H Exchange Stats
table_name = "zetadex_mainnet_tx.agg_ix_trade_24h_rolling"
primary_key = {"name": "asset", "type": "S"}
sort_key = {"name": "timestamp", "type": "N"}

df = spark.table(table_name) \
    .withColumn("asset", F.lit("ALL")) \
    .filter("timestamp == date_trunc('hour', current_timestamp - interval 1 hour)")

client.serve_table(table_name, df, primary_key, sort_key)

# COMMAND ----------

# DBTITLE 1,1H Exchange Stats
table_name = "zetadex_mainnet_tx.agg_ix_trade_1h"
primary_key = {"name": "asset", "type": "S"}
sort_key = {"name": "timestamp", "type": "N"}

df = spark.table(table_name) \
    .withColumn("asset", F.lit("ALL")) \
    .filter("timestamp == date_trunc('hour', current_timestamp - interval 1 hour)")

client.serve_table(table_name, df, primary_key, sort_key)

# COMMAND ----------

# DBTITLE 1,Maker Rewards
table_name = "zetadex_mainnet_rewards.agg_maker_rewards_epoch_user"
primary_key = {"name": "authority", "type": "S"}
sort_key = {"name": "epoch", "type": "N"}

df = spark.table(table_name) \

client.serve_table(table_name, df, primary_key, sort_key)

# COMMAND ----------

# DBTITLE 1,Taker Rewards
table_name = "zetadex_mainnet_rewards.agg_taker_rewards_epoch_user"
primary_key = {"name": "authority", "type": "S"}
sort_key = {"name": "epoch", "type": "N"}

df = spark.table(table_name) \

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


