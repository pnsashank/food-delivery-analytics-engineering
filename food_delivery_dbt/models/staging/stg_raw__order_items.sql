-- Source CTE.
-- Purpose: Read raw order-item line records and normalize types for stable joins and rollups.
with src as (
    select * from {{ source('raw', 'order_items')}}
)

select
    -- Surrogate key for the order line.
    -- Cast to bigint for consistent key typing across the warehouse.
    cast(order_item_id as bigint) as order_item_id,

    -- Parent order reference used for joins to orders/facts.
    cast(order_id as bigint) as order_id,

    -- Menu item reference used for joins to menu item dimension/SCD.
    cast(menu_item_id as bigint) as menu_item_id,

    -- Quantity is an integer by definition; required for item counts and subtotal computations.
    cast(quantity as integer) as quantity,

    -- Unit price stored as fixed-point numeric to avoid floating point drift in calculations.
    cast(unit_price as decimal(10,2)) as unit_price,

    -- Line total stored as fixed-point numeric; used as the additive measure for item subtotals.
    cast(line_total as decimal(12,2)) as line_total,

    -- Partition-derived UTC order day coming from the Parquet export.
    -- Renamed to order_date_utc to make the timezone basis explicit and consistent with dim_date.
    cast(order_day as date) as order_date_utc

from src
