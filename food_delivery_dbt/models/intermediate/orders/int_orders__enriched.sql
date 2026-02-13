with orders as (
    select *
    from {{ ref('stg_raw__orders') }}
),

items as (
    select *
    from {{ ref('int_orders__items_rollup') }}
),

status_orders as (
    select *
    from {{ ref('int_orders__status_rollup') }}
),

delivery as (
    select *
    from {{ ref('int_orders__delivery_enriched') }}
),

recon as (
    select *
    from {{ ref('int_orders__reconciliation') }}
)

select
    orders.order_id,
    orders.customer_id,
    orders.delivery_address_id,
    orders.restaurant_id,
    orders.currency_id,

    orders.order_placed_at,
    orders.scheduled_delivery_at,
    orders.order_date_utc,

    orders.subtotal,
    orders.tax,
    orders.delivery_fee,
    orders.discount,
    orders.total_amount,

    orders.payment_method,
    orders.payment_status,

    items.items_subtotal,
    items.items_total_qty,
    items.item_lines,
    items.distinct_menu_items,

    status_orders.placed_at,
    status_orders.accepted_at,
    status_orders.prep_start_at,
    status_orders.ready_for_pickup_at,
    status_orders.picked_up_at,
    status_orders.delivered_at,
    status_orders.canceled_at,
    status_orders.latest_status,
    status_orders.latest_status_at,
    status_orders.is_delivered,
    status_orders.is_canceled,
    status_orders.minutes_to_accept,
    status_orders.minutes_to_pickup,
    status_orders.minutes_to_deliver,
    status_orders.minutes_pickup_to_deliver,

    delivery.courier_id,
    delivery.assigned_at,
    delivery.pickup_eta,
    delivery.dropoff_eta,

    recon.subtotal_minus_items,
    recon.expected_total_from_items,
    recon.expected_total_minus_total_amount

from orders
left join items on orders.order_id = items.order_id
left join status_orders on orders.order_id = status_orders.order_id
left join delivery on orders.order_id = delivery.order_id
left join recon on orders.order_id = recon.order_id
