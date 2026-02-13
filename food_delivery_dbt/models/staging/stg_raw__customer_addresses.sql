-- Source CTE.
-- Purpose: Isolate the raw input so all cleaning/casting happens in one place.
with src as (
  select * from {{ source('raw', 'customer_addresses') }}
)

select
  -- Enforce a stable integer surrogate key type for warehouse joins.
  cast(address_id as bigint) as address_id,

  -- Enforce a stable integer foreign key type for joins to customers.
  cast(customer_id as bigint) as customer_id,

  -- Normalize label values:
  -- 1) cast -> varchar for string ops
  -- 2) trim -> remove whitespace
  -- 3) nullif(..., '') -> treat empty strings as NULL
  -- 4) upper -> standardize to match accepted_values tests (HOME/WORK/OTHER)
  upper(nullif(trim(cast(label as varchar)), '')) as label,

  -- Address line 1 is required logically; trim and nullify empties for consistent quality checks.
  nullif(trim(cast(line_1 as varchar)), '') as line_1,

  -- Optional address line 2; trim and nullify empties to avoid meaningless empty strings.
  nullif(trim(cast(line_2 as varchar)), '') as line_2,

  -- City normalization: trim + nullify empties (keeps downstream dims clean).
  nullif(trim(cast(city as varchar)), '') as city,

  -- State is optional; clean in the same way for consistency.
  nullif(trim(cast(state as varchar)), '') as state,

  -- Country normalization: trim + nullify empties.
  nullif(trim(cast(country as varchar)), '') as country,

  -- Postal codes are often stored as text; trim + nullify empties (preserves leading zeros if present).
  nullif(trim(cast(postal_code as varchar)), '') as postal_code,

  -- Normalize geospatial numeric precision (stable rounding/joins/aggregations).
  cast(latitude as decimal(9,6)) as latitude,
  cast(longitude as decimal(9,6)) as longitude,

  -- Ensure boolean type is consistent for downstream default-address logic.
  cast(is_default as boolean) as is_default,

  -- Parse timestamps safely:
  -- try_cast prevents the model from failing if a malformed value appears.
  try_cast(created_at as timestamptz) as created_at

from src
