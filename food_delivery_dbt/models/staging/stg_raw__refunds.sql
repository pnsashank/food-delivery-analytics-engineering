-- Source CTE.
-- Purpose: Read raw refunds and standardize types/strings for consistent downstream joins and tests.
with src as (
    select * from {{ source('raw', 'refunds')}}
)

select
    -- Surrogate primary key from OLTP.
    -- try_cast avoids hard failures if the raw file has unexpected types.
    try_cast(refund_id as bigint) as refund_id,

    -- Foreign key to the order being refunded.
    try_cast(order_id as bigint) as order_id,

    -- Refund timestamp (UTC in the pipeline; stored as timestamptz for safety).
    try_cast(refund_ts as timestamptz) as refund_ts,

    -- Refund reason normalization:
    -- 1) trim whitespace
    -- 2) convert empty strings to NULL
    -- 3) upper-case so accepted_values tests match reliably.
    upper(nullif(trim(refund_reason), '')) as refund_reason,

    -- Monetary amount of the refund.
    -- Decimal precision matches OLTP intent and supports numeric comparisons/tests.
    try_cast(refund_amount as decimal(12,2)) as refund_amount,

    -- Currency used for the refund amount (AUD/INR in your dataset).
    try_cast(currency_id as bigint) as currency_id,

    -- Partition-derived date for easy date-based filtering and joins to dim_date.
    -- Comes from the Parquet partition column (refund_day) generated during extraction.
    cast(refund_day as date) as refund_date_utc

from src
