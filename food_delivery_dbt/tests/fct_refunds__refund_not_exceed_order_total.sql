select
  r.refund_id,
  r.order_id,
  r.refund_amount,
  o.total_amount
from {{ ref('fct_refunds') }} r
join {{ ref('fct_orders') }} o
  on r.order_id = o.order_id
where r.refund_amount > o.total_amount + 0.01
limit 50
