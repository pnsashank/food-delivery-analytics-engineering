select order_id
from {{ ref('fct_orders') }}
where is_delivered = true
  and delivered_at is null
limit 50