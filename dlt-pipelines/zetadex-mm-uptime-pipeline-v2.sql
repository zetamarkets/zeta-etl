-- Databricks notebook source
CREATE
OR REPLACE TEMPORARY VIEW orders as
select
  a.order_id,
  a.asset,
  a.side,
  a.price,
  a.size,
  a.block_time as start_time,
  b.block_time as end_time,
  a.authority,
  label
from
  zetadex_mainnet_tx.cleaned_ix_place_order a
  inner join zetadex_mainnet_tx.cleaned_ix_order_complete b on date(a.block_time) = date(b.block_time)
  and a.asset = b.asset
  and a.side = b.side
  and a.order_id = b.order_id
  inner join zetadex_mainnet.pubkey_label l on a.authority = l.pub_key
where
  --date(a.block_time) = current_date() - interval 2 days
  date(a.block_time) between date('2023-04-03') and (current_date() - interval 2 days)
  and b.block_time > a.block_time
  and l.pub_key in (
     '9xLcZBGBcVQEiEb2cDLCKY5pke5RkqXTLguvFztioCT',
     'FoNc7VbxaVb2KFCxBrYdYtwjX5VAPoa5YrjZLmA9ZADA',
     'BJhhx876qULyKxFsVgtt6dbeeu2byCAWMAn758CjYMgR',
     'A6TBy4GsAyoU9uXhAZoWWEwjBBER5L1iDgjPU6PGdW2i',
     'GubTBrbgk9JwkwX1FkXvsrF1UC2AP7iTgg8SGtgH14QE'
   )

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW quotes as
select
  /*+ RANGE_JOIN(a, 60) */
  b.order_id as bid_order_id,
  a.order_id as ask_order_id,
  b.authority,
  b.label,
  b.asset,
  b.size as bid_size,
  a.size as ask_size,
  b.size * b.price as bid_size_usd,
  a.size * a.price as ask_size_usd,
  b.price as bid_price,
  a.price as ask_price,
  a.price - b.price as spread_dollars,
  (a.price - b.price) / ((a.price + b.price) / 2) * 10000 as spread_basis_points,
  greatest(b.start_time, a.start_time) as start_quote,
  least(b.end_time, a.end_time) as end_quote,
  cast(
    least(b.end_time, a.end_time) - greatest(b.start_time, a.start_time) as bigint
  ) as quote_duration_seconds
from
  orders b
  inner join orders a on true -- and date(b.start_time) = date(a.start_time)
  and b.authority = a.authority
  and b.asset = a.asset
  and b.size = a.size
  and b.side = 'bid'
  and a.side = 'ask'
  and (
    a.start_time < b.end_time
    and a.end_time > b.start_time
  ) -- overlap not including 0 length overlap https://stackoverflow.com/questions/325933/determine-whether-two-date-ranges-overlap
  and a.price - b.price > 0 -- annoying edge case where old orders can be in cross with new ones probably because of serum not being cranked
where
  (
    (a.price - b.price) / ((a.price + b.price) / 2) * 10000
  ) <= 40

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW intervals as
select
    label,
    asset,
    lag(dt) over (
      partition by label,
      asset
      order by
        dt
    ) start_interval,
    dt as end_interval
  from
    (
      select
        label,
        asset,
        start_quote as dt
      from
        quotes
      union
      select
        label,
        asset,
        end_quote as dt
      from
        quotes
    )

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW intervals_2 as
select
  /*+ RANGE_JOIN(quotes, 60) */
  greatest(start_quote, start_interval) as start_quote_interval,
  least(end_quote, end_interval) as end_quote_interval,
  cast(least(end_quote, end_interval) - greatest(start_quote, start_interval) as bigint) as quote_duration_interval,
  q.*,
  (
    (q.bid_size_usd + q.ask_size_usd) / sum(q.bid_size_usd + q.ask_size_usd) over (
      partition by q.label,
      q.asset,
      greatest(start_quote, start_interval),
      least(end_quote, end_interval)
    )
  ) * q.spread_basis_points as size_weighted_spread_bps,
  (
    q.bid_size_usd / sum(q.bid_size_usd) over (
      partition by q.label,
      q.asset,
      greatest(start_quote, start_interval),
      least(end_quote, end_interval)
    )
  ) * q.bid_price as size_weighted_bid_price,
  (
    q.ask_size_usd / sum(q.ask_size_usd) over (
      partition by q.label,
      q.asset,
      greatest(start_quote, start_interval),
      least(end_quote, end_interval)
    )
  ) * q.ask_price as size_weighted_ask_price
from
  intervals t
  join quotes q on date(t.start_interval) = date(q.start_quote)
  and t.label = q.label
  and t.asset = q.asset
  and t.start_interval < q.end_quote
  and t.end_interval > q.start_quote -- range join

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW uptime as

select
  a.label,
  a.start_quote_interval,
  a.end_quote_interval,
  a.quote_duration_interval,
  a.asset,
  sum(a.size_weighted_spread_bps) as size_weighted_spread_bps,
  sum(a.size_weighted_ask_price) as size_weighted_ask_price,
  sum(a.size_weighted_bid_price) as size_weighted_bid_price,
  count(*) as quotes,
  sum(bid_size) as total_bid_size,
  sum(ask_size) as total_ask_size,
  sum(bid_size_usd) as total_bid_size_usd,
  sum(ask_size_usd) as total_ask_size_usd,
  case
    when sum(bid_size_usd) >= 5000
    and sum(ask_size_usd) >= 5000 then true
    else false
  end as quote_meets_size_criteria,
  case
    when sum(a.size_weighted_spread_bps) <= 40 then true
    else false
  end as quote_meets_spread_criteria
from
  intervals_2 a
group by
  1,
  2,
  3,
  4,
  5

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW uptime_agg as

with
hours as (select explode(sequence(timestamp('2023-04-03'),timestamp(dateadd(current_date(),-2)),interval 1 hour)) as hour)

, base as (
	select
		h.hour as timestamp
		, m.label
		, m.asset
	from hours h
		cross join (select distinct m.label, m.asset from zetadex_mainnet.cleaned_mm_uptime m) m
)

, uptime as (
	select
		m.label
		, date_trunc('hour', m.start_quote_interval) as timestamp
		, m.asset
		, count(*) as quote_count
		, median(m.quote_duration_interval) as median_quote_length
		, median(m.total_ask_size_usd) as median_total_ask_size_usd
		, median(m.total_bid_size_usd) as median_total_bid_size_usd
		, median(case when m.quote_meets_size_criteria then m.total_ask_size_usd end) as median_total_ask_size_usd_meets_size_criteria
		, median(case when m.quote_meets_size_criteria then m.total_bid_size_usd end) as median_total_bid_size_usd_meets_size_criteria
		, sum(m.quote_duration_interval) as seconds_meeting_spread
		, sum(m.quote_duration_interval) / 3600 as meet_spread_rate
		, sum(case when m.quote_meets_size_criteria then m.quote_duration_interval end) as seconds_meeting_size
		, sum(case when m.quote_meets_size_criteria then m.quote_duration_interval end) / sum(m.quote_duration_interval) as meet_size_rate
		, sum(case when m.quote_meets_size_criteria and m.quote_meets_spread_criteria then m.quote_duration_interval end) as seconds_meeting_uptime
		, sum(case when m.quote_meets_size_criteria and m.quote_meets_spread_criteria then m.quote_duration_interval end) / 3600 as uptime_rate
	from zetadex_mainnet.cleaned_mm_uptime m
	group by 1,2,3
)

select
	b.label
  , b.timestamp
	, b.asset
	, coalesce(m.quote_count, 0) as quote_count
	, coalesce(m.median_quote_length, 0) as median_quote_length
	, coalesce(m.median_total_ask_size_usd, 0) as median_total_ask_size_usd
	, coalesce(m.median_total_bid_size_usd, 0) as median_total_bid_size_usd
	, coalesce(m.median_total_ask_size_usd_meets_size_criteria, 0) as median_total_ask_size_usd_meets_size_criteria
	, coalesce(m.median_total_bid_size_usd_meets_size_criteria, 0) as median_total_bid_size_usd_meets_size_criteria
	, coalesce(m.seconds_meeting_spread, 0) as seconds_meeting_spread
	, coalesce(m.meet_spread_rate, 0) as meet_spread_rate
	, coalesce(m.seconds_meeting_size, 0) as seconds_meeting_size
	, coalesce(m.meet_size_rate, 0) as meet_size_rate
	, coalesce(m.uptime_rate, 0) as uptime_rate
	, 0.90 as uptime_benchmark
from base b
	left join uptime m on m.timestamp = b.timestamp
		and m.label = b.label
		and m.asset = b.asset

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("uptime")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("delta").mode("append").saveAsTable("zetadex_mainnet.cleaned_mm_uptime")

-- COMMAND ----------

--%sql select * from zetadex_mainnet.cleaned_mm_uptime
DROP TABLE IF EXISTS zetadex_mainnet.cleaned_mm_uptime

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select dateadd(current_date(),-2)

-- COMMAND ----------


