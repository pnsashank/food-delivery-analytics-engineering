-- Intermediate model: Order item rollup (order-level aggregates).
-- Purpose:
--   Aggregate order_items to the order grain to support:
--     - Order-level item subtotal comparison vs order header subtotal
--     - KPI reporting (total quantity, number of lines, distinct items)
--
-- Grain:
--   One row per order_id.

with order_items as (
    -- Select only the columns needed for aggregation to keep the model focused and efficient.
    select
        order_id,
        menu_item_id,
        quantity,
        line_total
    from {{ ref('stg_raw__order_items') }}
)

select
    -- Natural key at the order grain.
    order_id,

    -- Sum of item line totals (rounded to 2 decimals to match currency precision).
    round(sum(line_total), 2) as items_subtotal,

    -- Total quantity across all line items for the order.
    sum(quantity) as items_total_qty,

    -- Number of line items (each row in order_items is one line).
    count(*) as item_lines,

    -- Count of unique menu items purchased within the order.
    count(distinct menu_item_id) as distinct_menu_items

from order_items
group by order_id
