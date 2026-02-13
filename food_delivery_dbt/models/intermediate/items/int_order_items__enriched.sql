-- Intermediate model: Enrich order item lines with order context and menu item attributes.
-- Purpose:
--   1) Keep the grain at "one row per order_item_id".
--   2) Attach customer/restaurant/currency/time fields from the parent order.
--   3) Attach descriptive attributes from the menu item dimension-like staging table.
--   4) Add a sanity flag to validate that the menu item belongs to the same restaurant as the order.

with order_items as (
    -- Base line items for each order.
    -- Grain: one row per order_item_id.
    select 
        order_item_id,
        order_id,
        menu_item_id,
        quantity,
        unit_price,
        line_total
    from {{ ref('stg_raw__order_items') }}
),

orders as (
    -- Parent order context used to enrich each line.
    -- Contains keys and timestamps required for downstream facts and auditing.
    select 
        order_id,
        customer_id,
        restaurant_id,
        currency_id,
        order_placed_at,
        order_date_utc
    from {{ ref('stg_raw__orders') }}
),

menu_items as (
    -- Menu item attributes to make order lines analyzable without additional joins.
    -- menu_item_restaurant_id is retained to validate cross-restaurant mismatches.
    select
        menu_item_id,
        restaurant_id as menu_item_restaurant_id,
        item_name,
        category,
        price_amount as menu_price,
        is_available
    from {{ ref('stg_raw__menu_items') }}
)

select 
    -- Line item identifiers and measures (core grain).
    order_items.order_item_id,
    order_items.order_id,
    order_items.menu_item_id,
    order_items.quantity,
    order_items.unit_price,
    order_items.line_total,

    -- Order-level enrichment (customer, restaurant, currency, timestamps).
    orders.customer_id,
    orders.restaurant_id,
    orders.currency_id,
    orders.order_placed_at,
    orders.order_date_utc,

    -- Menu item enrichment (descriptive fields + reference price/availability).
    menu_items.item_name,
    menu_items.category,
    menu_items.menu_price,
    menu_items.is_available,

    -- Data-quality flag:
    -- True if the menu item belongs to the same restaurant as the order.
    -- Useful to detect bad foreign keys or incorrect menu_item assignments.
    (menu_items.menu_item_restaurant_id = orders.restaurant_id) as is_menu_item_from_order_restaurant

from order_items 
-- Attach parent order context; left join preserves order_items even if order is missing (DQ visibility).
left join orders
  on order_items.order_id = orders.order_id

-- Attach menu item attributes; left join preserves lines even if menu item is missing (DQ visibility).
left join menu_items
  on order_items.menu_item_id = menu_items.menu_item_id
