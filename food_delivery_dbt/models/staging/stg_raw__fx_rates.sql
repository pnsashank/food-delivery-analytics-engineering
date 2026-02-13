-- Source CTE.
-- Purpose: Read raw FX rates once, then standardize types and derive a stable UTC date grain for joins.
with src as (
    select * from {{ source('raw', 'fx_rates')}}
)

select
    -- Surrogate key for the FX rate row (enforced as bigint for consistency across the warehouse).
    try_cast(fx_rate_id as bigint) as fx_rate_id,

    -- Currency pair identifiers used for dimension joins and uniqueness checks.
    -- try_cast protects the model from any unexpected non-numeric values in raw files.
    try_cast(base_currency_id as bigint) as base_currency_id,
    try_cast(quote_currency_id as bigint) as quote_currency_id,

    -- Timestamp for the rate observation; stored as timestamptz to preserve timezone awareness.
    try_cast(rate_ts as timestamptz) as rate_ts,

    -- Rate value with fixed precision/scale to avoid floating-point drift in analytics.
    try_cast(rate as decimal(18,8)) as rate,

    -- Data provenance (e.g., SIMULATED or external provider identifier).
    try_cast(source as text) as source,

    -- Partition-derived date (from the Parquet folder structure) used for date-dimension joins and partition pruning.
    -- Assumption: rate_day was generated in UTC during extraction (as per your Postgres→DuckDB export logic).
    cast(rate_day as date) as rate_date_utc

from src
