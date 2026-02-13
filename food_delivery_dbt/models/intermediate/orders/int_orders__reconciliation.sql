-- Intermediate model: Order financial reconciliation.
-- Purpose:
--   Reconcile the order header financials (orders.subtotal/total_amount) with
--   item-derived values (sum of order_items.line_total) to surface data quality issues.
--
-- What this produces:
--   - items_subtotal: sum of item line totals per order (defaults to 0 if no items)
--   - subtotal_minus_items: header subtotal minus items subtotal
--   - expected_total_from_items: computed total from items + tax + delivery_fee - discount
--   - expected_total_minus_total_amount: expected_total_from_items minus stored total_amount
--
-- Grain:
--   One row per order_id.

with orders as (
    -- Order header amounts at the order grain.
    -- Only select columns needed for reconciliation to keep this model lightweight.
    select
        order_id,
        subtotal,
        tax,
        delivery_fee,
        discount,
        total_amount
    from {{ ref('stg_raw__orders') }}
),

item_details as (
    -- Item rollup amounts at the same order grain.
    select
        order_id,
        items_subtotal
    from {{ ref('int_orders__items_rollup') }}
)

select
    -- Natural key.
    orders.order_id,

    -- Explicit naming to differentiate header subtotal vs items subtotal.
    orders.subtotal as orders_subtotal,

    -- Default to 0 to allow consistent arithmetic when an order has no item rows.
    coalesce(item_details.items_subtotal, 0) as items_subtotal,

    -- Difference between header subtotal and item subtotal.
    -- Rounding to 2 decimals aligns with currency precision.
    round(orders.subtotal - coalesce(item_details.items_subtotal, 0), 2) as subtotal_minus_items,

    -- Carry through order header adjustments.
    orders.tax,
    orders.delivery_fee,
    orders.discount,
    orders.total_amount,

    -- Recomputed "expected" total using the item subtotal.
    -- This should match total_amount if all upstream calculations are consistent.
    round(
        coalesce(item_details.items_subtotal, 0) + orders.tax + orders.delivery_fee - orders.discount,
        2
    ) as expected_total_from_items,

    -- Signed difference: (expected total) - (stored total).
    -- Values near 0 indicate consistency; larger absolute values indicate mismatches.
    round(
        (
            coalesce(item_details.items_subtotal, 0) + orders.tax + orders.delivery_fee - orders.discount
        ) - orders.total_amount,
        2
    ) as expected_total_minus_total_amount

from orders
left join item_details
  on orders.order_id = item_details.order_id
