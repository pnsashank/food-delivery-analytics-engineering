select
  order_id,
  subtotal,
  tax,
  delivery_fee,
  discount,
  total_amount,
  round(subtotal + tax + delivery_fee - discount, 2) as expected_total,
  round(total_amount - (subtotal + tax + delivery_fee - discount), 2) as diff
from {{ ref('fct_orders') }}
where abs(round(total_amount - (subtotal + tax + delivery_fee - discount), 2)) > 0.05
limit 50
