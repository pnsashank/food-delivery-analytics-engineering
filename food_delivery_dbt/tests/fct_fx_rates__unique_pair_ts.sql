select
  base_currency_id,
  quote_currency_id,
  rate_ts,
  count(*) as n
from {{ ref('fct_fx_rates') }}
group by 1,2,3
having count(*) > 1
limit 50
