-- Databricks notebook source
CREATE
OR REPLACE TEMPORARY VIEW orders as
with x as (
select
  a.order_id,
  a.asset,
  a.side,
  a.price,
  a.size,
  price*size as notional,
  timestamp(a.block_time) as start_time,
  timestamp(coalesce(b.block_time, from_unixtime(unix_timestamp(a.block_time) - (unix_timestamp(a.block_time) % 65535) + a.tif_offset, "dd-MM-yyyy HH:mm:ss"))) as end_time,
  a.authority,
  label
from zetadex_mainnet_tx.cleaned_ix_place_order a
  left join zetadex_mainnet_tx.cleaned_ix_order_complete b 
    on a.asset = b.asset
    and a.order_id = b.order_id
    and b.block_time > a.block_time
  inner join zetadex_mainnet.pubkey_label l on a.authority = l.pub_key
where
  date(a.block_time) = dateadd(current_date(),-2)
  --date_trunc('hour',a.block_time) = '2023-06-01 08:00:00.000'
  --date(a.block_time) between date('2023-06-01') and dateadd(current_date(),-2)
--   and l.pub_key in (
--      '9xLcZBGBcVQEiEb2cDLCKY5pke5RkqXTLguvFztioCT',
--      'mmkCqiSpavevetXeRTmwv3pK4Dt6kAdTqYh6E5XxrgR',
--      'mmkyprqAN3ukTQF78ck8F9K5UfN8t9qQLet8RRVTcaC',
--      'FoNc7VbxaVb2KFCxBrYdYtwjX5VAPoa5YrjZLmA9ZADA',
--      'dieg32oMKahzEfSy1ct6QYQxvrjuiTPcHLH5mv5od9B',
--      '1ucky6S59JdQTFMgGERbxxHxfpXgmo4Uz6DpXjGcDmJ'
--    )
)
, x2 as (
select
  *,
  min(case when side='ask' then price END) over (partition by start_time, end_time, authority, asset) as best_ask,
  max(case when side='bid' then price END) over (partition by start_time, end_time, authority, asset) as best_bid
from x
)
, x3 as ( 
select
  *,
  (best_bid+best_ask)/2 as quote_mid_price
from x2
)
select
  *,
  abs(price - quote_mid_price) / quote_mid_price * 10000 as bps_from_mid
from x3

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW agg_orders as
select
  o.authority
  , o.label
  , o.start_time
  , o.end_time
  , o.asset
  , o.side
  , sum(o.size) as size
  , sum(o.notional) as notional
from orders o
where
  o.bps_from_mid <= 40
group by 1,2,3,4,5,6

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW quotes as
select
  a.authority,
  a.label,
  a.asset,
  a.start_time as start_quote,
  a.end_time as end_quote,
  b.size as bid_size,
  a.size as ask_size,
  b.notional as bid_size_usd,
  a.notional as ask_size_usd,
  case when b.notional >= 5000 and a.notional >= 5000 then true else false end as meets_size_criteria,
  cast(a.end_time - a.start_time as bigint) as quote_duration_seconds
from agg_orders b
  inner join agg_orders a on true
    and b.side = 'bid'
    and a.side = 'ask'
    and b.authority = a.authority
    and b.asset = a.asset
    and b.start_time = a.start_time
    and b.end_time = a.end_time -- might in future want to join based on spread, but right now keeping it simple assuming that places and cancels are atomic across all levels

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW uptime as

with
hours as (select explode(sequence(timestamp(dateadd(current_date(),-2)),timestamp(dateadd(current_date(),-1)),interval 1 hour)) as hour)

, base as (
	select
		h.hour as timestamp
		, q.label
		, q.asset
	from hours h
		cross join (select distinct q.label, q.asset from quotes q) q
  where
    date(h.hour) = dateadd(current_date(),-2)
)

, uptime as (
		select
			q.label
			, date_trunc('hour', q.start_quote) as timestamp
			, q.asset
			, count(*) as quote_count
			, median(q.quote_duration_seconds) as median_quote_length
			, median(q.ask_size_usd) as median_ask_size_usd
			, median(q.bid_size_usd) as median_bid_size_usd
			, sum(q.quote_duration_seconds) as seconds_meeting_spread
			, sum(q.quote_duration_seconds) / 3600 as meet_spread_rate
			, sum(case when q.meets_size_criteria then q.quote_duration_seconds end) as seconds_meeting_uptime
			, sum(case when q.meets_size_criteria then q.quote_duration_seconds end) / 3600 as uptime_rate
		from quotes q
		group by 1,2,3
)

select
	b.label
  , b.timestamp
	, b.asset
	, coalesce(u.quote_count, 0) as quote_count
	, coalesce(u.median_quote_length, 0) as median_quote_length
	, coalesce(u.median_ask_size_usd, 0) as median_ask_size_usd
	, coalesce(u.median_bid_size_usd, 0) as median_bid_size_usd
	, coalesce(u.seconds_meeting_spread, 0) as seconds_meeting_spread
	, coalesce(u.meet_spread_rate, 0) as meet_spread_rate
	, coalesce(u.seconds_meeting_uptime, 0) as seconds_meeting_uptime
	, coalesce(u.uptime_rate, 0) as uptime_rate
from base b
	left join uptime u on u.timestamp = b.timestamp
		and u.label = b.label
		and u.asset = b.asset

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("uptime")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("delta").mode("append").saveAsTable("zetadex_mainnet.cleaned_mm_uptime")

-- COMMAND ----------


