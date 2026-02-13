-- FX rates fact model enriched with currency codes.
-- Goal:
--   Provide a clean, query-friendly FX rates table that includes both:
--     - currency IDs (join keys)
--     - currency codes (human-readable labels)
--
-- Why this exists:
--   - stg_raw__fx_rates contains the raw FX rate events (base/quote IDs, timestamp, rate, source).
--   - dim_currencies contains the stable reference list of currencies (id, code, name, active flag).
--   - Joining currency codes here avoids repeating the same join logic in every downstream report.

with fx as (
    -- Staged FX rates (typed/standardized in staging).
    select *
    from {{ ref('stg_raw__fx_rates') }}
),

currencies as (
    -- Currency dimension (reference data).
    select *
    from {{ ref('dim_currencies') }}
)

select
    -- Primary key for the FX rate record.
    fx.fx_rate_id,

    -- Base currency: ID + readable code (e.g., USD).
    fx.base_currency_id,
    base.currency_code as base_currency_code,

    -- Quote currency: ID + readable code (e.g., INR).
    fx.quote_currency_id,
    quote.currency_code as quote_currency_code,

    -- Timestamp when this rate is applicable (as captured by the source system).
    fx.rate_ts,

    -- Conversion rate from base -> quote at rate_ts.
    fx.rate,

    -- Optional provenance field (API/provider name, internal batch tag, etc.).
    fx.source

from fx
-- Join to currency dimension twice:
--   1) once for the base currency attributes
--   2) once for the quote currency attributes
left join currencies as base
  on fx.base_currency_id = base.currency_id
left join currencies as quote
  on fx.quote_currency_id = quote.currency_id
