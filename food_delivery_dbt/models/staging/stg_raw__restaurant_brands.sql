-- Source CTE.
-- Purpose: Read raw restaurant brand records and standardize types/strings for consistent joins and tests.
with src as (
  select * from {{ source('raw', 'restaurant_brands') }}
)

select
  -- Natural key for the brand coming from OLTP.
  cast(brand_id as bigint) as brand_id,

  -- Clean brand name:
  -- 1) cast to string type used in DuckDB/dbt
  -- 2) trim leading/trailing whitespace
  -- 3) convert empty strings to NULL so not_null tests behave correctly.
  nullif(trim(cast(brand_name as varchar)), '') as brand_name,

  -- Active flag, standardized to boolean.
  cast(is_active as boolean) as is_active,

  -- Record creation timestamp (kept as timestamptz for timezone-safe analytics).
  -- try_cast prevents model failure if raw files contain unexpected timestamp formats.
  try_cast(created_at as timestamptz) as created_at

from src
