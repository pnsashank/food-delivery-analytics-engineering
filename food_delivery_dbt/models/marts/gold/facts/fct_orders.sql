-- Orders fact model enriched with the correct SCD restaurant surrogate key.
-- Goal:
--   Attach restaurant_sk (surrogate key from the restaurants SCD dimension) to each order.
--
-- Why this exists:
--   - int_orders__enriched contains the “best possible” order record by combining:
--       * the raw order header
--       * item rollups (items_subtotal, quantities, distinct items)
--       * status rollups (timestamps + latest status + durations)
--       * delivery assignments (courier + ETAs)
--       * reconciliation checks (subtotal vs items, expected total vs total_amount)
--   - dim_restaurants_backfill is an SCD Type 2 dimension for restaurants.
--     Restaurants can change attributes over time (brand name, active flag, zone, address fields, etc.).
--   - This model time-travels and binds each order to the restaurant version that was valid at order time.

with orders as (
    -- One row per order_id, already enriched with operational rollups and QA columns.
    select *
    from {{ ref('int_orders__enriched') }}
),

restaurants as (
    -- SCD restaurant rows with validity windows.
    -- valid_from/valid_to define when a particular restaurant version is considered active.
    select 
        restaurant_sk,
        restaurant_id,
        valid_from,
        valid_to,
        is_current as is_current_restaurant_details
    from {{ ref('dim_restaurants_backfill') }}
)

select 
    -- Natural keys and core foreign keys from the transactional system.
    orders.order_id,
    orders.customer_id,
    orders.delivery_address_id,
    orders.restaurant_id,
    orders.currency_id,

    -- SCD surrogate key + a convenience flag indicating whether the matched dimension row is current.
    restaurants.restaurant_sk,
    restaurants.is_current_restaurant_details,

    -- Primary order timestamps (order_time and derived date for partitioning/analysis).
    orders.order_placed_at,
    orders.scheduled_delivery_at,
    orders.order_date_utc,

    -- Monetary measures at the order grain.
    orders.subtotal,
    orders.tax,
    orders.delivery_fee,
    orders.discount,
    orders.total_amount,

    -- Payment attributes (kept at the fact for filtering and reporting).
    orders.payment_method,
    orders.payment_status,

    -- Item-level rollups (computed from order_items).
    orders.items_subtotal,
    orders.items_total_qty,
    orders.item_lines,
    orders.distinct_menu_items,

    -- Status timeline rollups (computed from order_status_events).
    orders.placed_at,
    orders.accepted_at,
    orders.prep_start_at,
    orders.ready_for_pickup_at,
    orders.picked_up_at,
    orders.delivered_at,
    orders.canceled_at,
    orders.latest_status,
    orders.latest_status_at,
    orders.is_delivered,
    orders.is_canceled,
    orders.minutes_to_accept,
    orders.minutes_to_pickup,
    orders.minutes_to_deliver,
    orders.minutes_pickup_to_deliver,

    -- Delivery assignment fields (if present).
    orders.courier_id,
    orders.assigned_at,
    orders.pickup_eta,
    orders.dropoff_eta,

    -- QA / reconciliation fields useful for anomaly detection and audits.
    orders.subtotal_minus_items,
    orders.expected_total_from_items,
    orders.expected_total_minus_total_amount 

from orders 
left join restaurants 
  on orders.restaurant_id = restaurants.restaurant_id
 -- Time-range join to pick the restaurant version valid when the order was placed.
 -- Notes:
 --   - order_placed_at is normalized to UTC for consistent comparison across sources.
 --   - The interval is [valid_from, valid_to) to avoid double-matching at boundary timestamps.
 --   - valid_to is open-ended (NULL) for current rows, so it is treated as far-future.
and cast(timezone('UTC', orders.order_placed_at) as timestamp) >= restaurants.valid_from
and cast(timezone('UTC', orders.order_placed_at) as timestamp) < coalesce(restaurants.valid_to, timestamp '9999-12-31 00:00:00')
