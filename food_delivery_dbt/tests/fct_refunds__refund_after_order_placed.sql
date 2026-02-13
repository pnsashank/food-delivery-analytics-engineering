select
  r.refund_id,
  r.order_id,
  r.refund_ts,
  o.order_placed_at
from {{ ref('fct_refunds') }} r
join {{ ref('fct_orders') }} o
  on r.order_id = o.order_id
where cast(r.refund_ts as timestamp) < cast(o.order_placed_at as timestamp)
limit 50
