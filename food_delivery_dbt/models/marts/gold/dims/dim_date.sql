-- Dimension: Date (UTC).
-- Goal:
--   Create a canonical date spine used for time-based joins and aggregations across the warehouse.
--   This avoids relying on each fact table to carry its own date attributes and ensures consistent
--   definitions of year/month/week/day-of-week across reporting.
--
-- How dates are generated:
--   - Pull distinct UTC dates that actually appear in the warehouse from multiple upstream sources:
--       1) orders.order_date_utc
--       2) order_status_events.event_date_utc
--       3) refunds.refund_date_utc
--       4) fx_rates.rate_date_utc
--
-- Notes on UNION:
--   - UNION removes duplicates by default (UNION ALL would keep duplicates).
--   - This is desired here because dim_date should have one row per calendar date.
--
-- Grain:
--   One row per date_utc.

with dates as (

    -- Dates on which orders were placed (derived in the bronze export as UTC day and staged as order_date_utc).
    select
        order_date_utc as date_utc
    from {{ ref('stg_raw__orders') }}
    where order_date_utc is not null

    union

    -- Dates on which order status events occurred (UTC day derived from event timestamp).
    select
        event_date_utc as date_utc
    from {{ ref('stg_raw__order_status_events') }}
    where event_date_utc is not null

    union

    -- Dates on which refunds occurred (UTC day derived from refund timestamp).
    select
        refund_date_utc as date_utc
    from {{ ref('stg_raw__refunds') }}
    where refund_date_utc is not null

    union

    -- Dates on which FX rates were recorded (UTC day derived from rate timestamp).
    select
        rate_date_utc as date_utc
    from {{ ref('stg_raw__fx_rates') }}
    where rate_date_utc is not null
)

select
    date_utc,                              -- Primary key of this dimension (one row per UTC calendar date).
    extract(year from date_utc) as year_utc,
    extract(month from date_utc) as month_utc,
    extract(day from date_utc) as day_utc,

    -- Day of week (DuckDB returns 0-6 depending on engine; keep consistent with your reporting layer).
    extract(dow from date_utc) as day_of_week,

    -- Week of year for weekly rollups.
    extract(week from date_utc) as week_of_year
from dates
