-- Source CTE.
-- Purpose: Centralize the raw input reference so transformations are applied consistently.
with src as (
  select * from {{ source('raw', 'currencies') }}
)

select
  -- Enforce a stable integer key type for joins and warehouse consistency.
  cast(currency_id as bigint) as currency_id,

  -- Normalize currency codes:
  -- 1) cast -> varchar to ensure string operations behave consistently
  -- 2) trim -> remove leading/trailing whitespace
  -- 3) nullif(..., '') -> convert empty strings to NULL
  -- 4) upper -> standardize to ISO-style uppercase (e.g., AUD, INR)
  upper(nullif(trim(cast(currency_code as varchar)), '')) as currency_code,

  -- Clean free-text name:
  -- trim whitespace and convert empty strings to NULL for consistency.
  nullif(trim(cast(currency_name as varchar)), '') as currency_name,

  -- Ensure boolean type is consistent across the warehouse.
  cast(is_active as boolean) as is_active,

  -- Parse timestamps safely:
  -- try_cast returns NULL instead of failing the model if a bad value appears.
  try_cast(created_at as timestamptz) as created_at

from src

