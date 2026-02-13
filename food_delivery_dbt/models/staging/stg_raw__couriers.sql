-- Source CTE.
-- Purpose: Keep a single, consistent reference point to the raw source so downstream edits only change in one place.
with src as (
  select * from {{ source('raw', 'couriers') }}
)

select
  -- Enforce a stable integer key type for joins and warehouse consistency.
  cast(courier_id as bigint) as courier_id,

  -- Standardize text fields:
  -- 1) cast -> varchar for consistent string handling in DuckDB
  -- 2) trim -> remove accidental whitespace
  -- 3) nullif(..., '') -> convert empty strings to NULL so tests/joins behave predictably
  nullif(trim(cast(city as varchar)), '') as city,

  -- Normalize categorical values to a consistent case for reliable grouping and accepted_values tests.
  upper(nullif(trim(cast(vehicle as varchar)), '')) as vehicle,

  -- Ensure boolean type is consistent across the warehouse.
  cast(is_active as boolean) as is_active,

  -- Parse timestamps safely:
  -- try_cast returns NULL instead of failing the model if a bad value appears.
  try_cast(created_at as timestamptz) as created_at

from src
