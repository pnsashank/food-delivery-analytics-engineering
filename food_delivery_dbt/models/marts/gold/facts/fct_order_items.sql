-- Order items fact model enriched with the correct SCD menu item surrogate key.
-- Goal:
--   Attach menu_item_sk (surrogate key from the menu items SCD dimension) to each order item line.
--
-- Why this exists:
--   - int_order_items__enriched provides the transactional grain (one row per order item line) enriched with order context.
--   - dim_menu_items_backfill is an SCD Type 2 dimension (with a backfilled "start of time" valid_from for the first version),
--     so we can time-travel and find which menu item attributes were valid at the time the order was placed.
--   - Joining at the fact layer ensures downstream marts can filter/aggregate by menu item version via menu_item_sk.

with order_items as (
    -- Transaction grain: one row per order_item_id, already enriched with order + menu item attributes.
    select *
    from {{ ref('int_order_items__enriched') }}
),

menu_items as (
    -- SCD dimension rows for menu items, including validity windows.
    -- valid_from/valid_to define the time period when a particular version of a menu item is considered active.
    select
        menu_item_sk,
        menu_item_id,
        valid_from,
        valid_to,
        is_current as is_current_menu_item_details
    from {{ ref('dim_menu_items_backfill') }}
)

select
    -- Natural keys for the order item line.
    order_items.order_item_id,
    order_items.order_id,
    order_items.menu_item_id,

    -- SCD surrogate key and whether the matched dimension row is the current version.
    menu_items.menu_item_sk,
    menu_items.is_current_menu_item_details,

    -- Denormalized order context (useful for slicing and filtering without additional joins).
    order_items.customer_id,
    order_items.restaurant_id,
    order_items.currency_id,
    order_items.order_placed_at,
    order_items.order_date_utc,

    -- Measures at the order-item grain.
    order_items.quantity,
    order_items.unit_price,
    order_items.line_total

from order_items
left join menu_items
  on order_items.menu_item_id = menu_items.menu_item_id
 -- Time-range join to pick the menu item version valid at the moment the order was placed.
 -- Notes:
 --   - order_placed_at is normalized to UTC for consistent comparisons.
 --   - The interval is [valid_from, valid_to) (inclusive start, exclusive end) to avoid overlaps at boundary timestamps.
 --   - valid_to is open-ended (NULL) for the current row, so it is treated as far-future.
and cast(timezone('UTC', order_items.order_placed_at) as timestamp) >= menu_items.valid_from
and cast(timezone('UTC', order_items.order_placed_at) as timestamp) < coalesce(menu_items.valid_to, timestamp '9999-12-31 00:00:00')
