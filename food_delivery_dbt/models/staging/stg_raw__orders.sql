-- Source CTE.
-- Purpose: Read raw orders and apply type + string normalization so downstream joins/tests are stable.
with src as (
    select * from {{ source('raw', 'orders') }}
)

select
    -- Natural primary key from OLTP.
    cast(order_id as bigint) as order_id,

    -- Customer placing the order.
    cast(customer_id as bigint) as customer_id,

    -- Address used for delivery (denormalized reference to customer_addresses).
    cast(delivery_address_id as bigint) as delivery_address_id,

    -- Restaurant/outlet fulfilling the order.
    cast(restaurant_id as bigint) as restaurant_id,

    -- Order placement timestamp.
    -- Stored as timestamptz to preserve timezone-aware semantics (UTC expected upstream).
    cast(order_placed_at as timestamptz) as order_placed_at,

    -- Scheduled delivery timestamp (nullable).
    cast(scheduled_delivery as timestamptz) as scheduled_delivery_at,

    -- Monetary measures: standardized precision/scale for consistent aggregations and comparisons.
    cast(subtotal as decimal(12,2)) as subtotal,
    cast(tax as decimal(12,2)) as tax,
    cast(delivery_fee as decimal(12,2)) as delivery_fee,
    cast(discount as decimal(12,2)) as discount,
    cast(total_amount as decimal(12,2)) as total_amount,

    -- Normalize categorical fields:
    -- 1) cast to varchar to avoid type mismatches
    -- 2) trim whitespace
    -- 3) map empty strings to NULL
    -- 4) uppercase for accepted_values tests and consistent filtering/grouping
    upper(nullif(trim(cast(payment_method as varchar)), '')) as payment_method,
    upper(nullif(trim(cast(payment_status as varchar)), '')) as payment_status,

    -- Currency used for the order (nullable if legacy rows existed before currency support).
    cast(currency_id as bigint) as currency_id,

    -- Partition-derived UTC date coming from the Parquet export.
    -- Used for date dimension joins and day-grain reporting without repeated date casts.
    cast(order_day as date) as order_date_utc

from src
