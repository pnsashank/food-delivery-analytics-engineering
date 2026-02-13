-- Intermediate model: Refunds enriched with order context.
-- Purpose:
--   Bring order-level attributes onto each refund so refunds can be analyzed by
--   customer/restaurant/courier, order status, and order financials.
--
-- What this produces (grain = one row per refund_id):
--   - Refund details (timestamp, reason, amount, currency)
--   - Order context (customer/restaurant/courier, placed date, lifecycle status)
--   - Order financial context (total_amount, items_subtotal, reconciliation diff)
--   - A simple validation flag: refund amount does not exceed order total
--
-- Notes:
--   - LEFT JOIN is used so refunds remain visible even if the corresponding order
--     record is missing or fails upstream assumptions.
--   - The flag (refund_amount <= total_amount) becomes NULL if total_amount is NULL,
--     which is useful for identifying missing order context.

with refunds as (
  -- Refund fact stream at refund grain from the staging layer.
  -- Staging is expected to standardize types (timestamps, enums, numerics).
  select
    refund_id,
    order_id,
    refund_ts,
    refund_reason,
    refund_amount,
    currency_id
  from {{ ref('stg_raw__refunds') }}
),

orders as (
  -- Order enrichment stream at order grain.
  -- Includes lifecycle milestones, latest status, and reconciliation metrics.
  select
    order_id,
    customer_id,
    restaurant_id,
    courier_id,
    order_placed_at,
    order_date_utc,
    latest_status,
    delivered_at,
    canceled_at,
    total_amount,
    items_subtotal,
    expected_total_minus_total_amount
  from {{ ref('int_orders__enriched') }}
)

select
  -- Refund identifiers.
  refunds.refund_id,
  refunds.order_id,

  -- Customer/restaurant/courier context from the associated order.
  orders.customer_id,
  orders.restaurant_id,
  orders.courier_id,

  -- Refund attributes.
  refunds.refund_ts,
  refunds.refund_reason,
  refunds.refund_amount,
  refunds.currency_id,

  -- Order lifecycle context useful for refund analysis.
  orders.order_placed_at,
  orders.order_date_utc,
  orders.latest_status,
  orders.delivered_at,
  orders.canceled_at,

  -- Order financial context and reconciliation signal.
  orders.total_amount,
  orders.items_subtotal,
  orders.expected_total_minus_total_amount,

  -- Basic sanity flag: refund should not exceed the order total.
  (refunds.refund_amount <= orders.total_amount) as is_refund_not_exceed_total

from refunds
left join orders on refunds.order_id = orders.order_id
