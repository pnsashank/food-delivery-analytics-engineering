-- Source CTE.
-- Purpose: Read raw status event history and standardize types/strings for reliable rollups and tests.
with src as (
    select * from {{ source('raw', 'order_status_events')}}
)

select
    -- Unique event identifier.
    -- try_cast prevents hard failures if any unexpected non-numeric values appear in the raw layer.
    try_cast(event_id as bigint) as event_id,

    -- Parent order reference used to roll up status timelines per order.
    try_cast(order_id as bigint) as order_id,

    -- Event timestamp (UTC basis expected from upstream load/export).
    -- Stored as timestamptz to preserve timezone-aware semantics.
    try_cast(event_ts as timestamptz) as event_ts,

    -- Normalize status values:
    -- 1) trim whitespace
    -- 2) convert empty strings to NULL
    -- 3) uppercase for consistent comparisons and accepted_values tests
    upper(nullif(trim(status), '')) as status,

    -- Normalize actor values for consistent comparisons and accepted_values tests.
    upper(nullif(trim(actor), '')) as actor,

    -- Optional free-text notes; normalize empty strings to NULL.
    nullif(trim(notes), '') as notes,

    -- Partition-derived UTC day coming from the Parquet export.
    -- Renamed to event_date_utc to align with dim_date and time-series aggregations.
    cast(event_day as date) as event_date_utc

from src
