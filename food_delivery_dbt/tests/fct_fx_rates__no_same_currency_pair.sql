select fx_rate_id, base_currency_id, quote_currency_id
from {{ ref('fct_fx_rates') }}
where base_currency_id = quote_currency_id
limit 50