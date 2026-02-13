select
  order_id,
  subtotal,
  items_subtotal,
  round(subtotal - items_subtotal, 2) as diff
from {{ ref('fct_orders') }}
where items_subtotal is not null
  and abs(round(subtotal - items_subtotal, 2)) > 0.05
limit 50
