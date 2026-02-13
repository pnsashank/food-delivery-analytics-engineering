-- Refunds fact model.
-- Goal:
--   Produce one row per refund_id with enough context to analyze refunds by:
--     - customer / restaurant (natural keys)
--     - restaurant_sk (SCD surrogate key for correct historical attribution)
--     - payment method/status at the time of the order
--     - item rollups for the associated order (subtotal/qty/lines/distinct items)
--     - basic data quality flag (refund does not exceed order total)
--
-- Why this exists:
--   - int_refunds__enriched already joins refunds to the enriched orders layer to bring
--     operational context (status, placed time, totals, reconciliation fields, etc.).
--   - This model keeps the refund grain (refund_id) but adds two important lookups:
--       1) fct_orders for restaurant_sk + payment attributes (canonical facts layer)
--       2) int_orders__items_rollup for item-level aggregates (fast reporting)

with refunds as (
    -- One row per refund_id, enriched with order context in the intermediate layer.
    -- Includes customer_id / restaurant_id / courier_id, order timestamps, totals, and QA flags.
    select *
    from {{ ref('int_refunds__enriched') }}
),

orders as (
    -- Pull only the columns needed from the canonical orders fact:
    --   - restaurant_sk: SCD surrogate key resolved via time-range join in fct_orders
    --   - payment_method/payment_status: used for refund analysis by payment flow
    select
        order_id,
        restaurant_sk,
        payment_method,
        payment_status
    from {{ ref('fct_orders') }}
),

items as (
    -- Item rollups by order_id (computed from order_items):
    --   - items_subtotal: sum(line_total)
    --   - items_total_qty: sum(quantity)
    --   - item_lines: number of item rows
    --   - distinct_menu_items: count(distinct menu_item_id)
    select *
    from {{ ref('int_orders__items_rollup') }}
)

select
    -- Refund identifiers (refund grain).
    refunds.refund_id,
    refunds.order_id,

    -- Natural keys for common slicing without needing dimension joins.
    refunds.customer_id,
    refunds.restaurant_id,
    orders.restaurant_sk,

    -- Refund event details.
    refunds.currency_id,
    refunds.refund_ts,
    refunds.refund_reason,
    refunds.refund_amount,

    -- Payment context from the orders fact (canonicalized definitions + tests).
    orders.payment_method,
    orders.payment_status,

    -- Order item aggregates for refund analysis (e.g., refund vs items_subtotal).
    items.items_subtotal,
    items.items_total_qty,
    items.item_lines,
    items.distinct_menu_items,

    -- Simple QA flag from int_refunds__enriched:
    -- True if refund_amount <= total_amount for the linked order.
    refunds.is_refund_not_exceed_total

from refunds
-- Join to the canonical orders fact to get restaurant_sk + payment attributes.
left join orders
  on refunds.order_id = orders.order_id
-- Join to item rollups to attach item-level aggregates to the refund record.
left join items
  on refunds.order_id = items.order_id
