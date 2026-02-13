select fx_rate_id, rate
from {{ ref('fct_fx_rates') }}
where rate <= 0
   or rate > 100000
limit 50