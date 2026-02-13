-- Source CTE.
-- Purpose: Pull raw menu item records and apply standard typing + light text hygiene before downstream joins.
with src as (
  select * from {{ source('raw', 'menu_items') }}
)

select
  -- Natural key for the menu item; enforced as bigint for consistency across models.
  cast(menu_item_id as bigint) as menu_item_id,

  -- Parent restaurant/outlet reference; enforced as bigint to align with restaurant dimensions/facts.
  cast(restaurant_id as bigint) as restaurant_id,

  -- Standardize text fields:
  -- - cast to varchar to avoid mixed types from Parquet inference
  -- - trim to remove accidental whitespace
  -- - nullif(...,'') to convert empty strings into NULLs (cleaner analytics + stronger tests).
  nullif(trim(cast(item_name as varchar)), '') as item_name,
  nullif(trim(cast(category as varchar)), '') as category,

  -- Price stored as fixed-point numeric to avoid floating precision issues in sums/aggregations.
  -- Renamed to price_amount to make units explicit and consistent across marts.
  cast(price as decimal(10,2)) as price_amount,

  -- Availability flag; enforced as boolean for consistent filtering.
  cast(is_available as boolean) as is_available,

  -- Record creation timestamp; try_cast avoids hard failures if a malformed timestamp appears in raw.
  try_cast(created_at as timestamptz) as created_at

from src
