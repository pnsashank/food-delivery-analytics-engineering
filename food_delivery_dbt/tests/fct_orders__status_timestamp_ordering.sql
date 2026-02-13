select
  order_id,
  accepted_at,
  picked_up_at,
  delivered_at
from {{ ref('fct_orders') }}
where
  (accepted_at is not null and picked_up_at is not null and cast(accepted_at as timestamp) > cast(picked_up_at as timestamp))
  or
  (picked_up_at is not null and delivered_at is not null and cast(picked_up_at as timestamp) > cast(delivered_at as timestamp))
limit 50
