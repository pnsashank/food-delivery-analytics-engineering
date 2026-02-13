-- Source CTE.
-- Purpose: Read raw restaurant outlet rows and standardize key fields, text fields, and timestamps for downstream joins.
with src as (
  select * from {{ source('raw', 'restaurant_outlets') }}
)

select
  -- Natural key for the outlet (called restaurant_id in OLTP for simplicity).
  cast(restaurant_id as bigint) as restaurant_id,

  -- Parent brand key used to join to restaurant brands.
  cast(brand_id as bigint) as brand_id,

  -- Outlet name cleaned for consistency (trim + empty-to-NULL).
  nullif(trim(cast(outlet_name as varchar)), '') as outlet_name,

  -- City cleaned for grouping/filters (trim + empty-to-NULL).
  nullif(trim(cast(city as varchar)), '') as city,

  -- Delivery zone used for operational segmentation (trim + empty-to-NULL).
  nullif(trim(cast(delivery_zone as varchar)), '') as delivery_zone,

  -- Address line used for display and possible geocoding later (trim + empty-to-NULL).
  nullif(trim(cast(address_line1 as varchar)), '') as address_line1,

  -- Postal code as text to preserve leading zeros (trim + empty-to-NULL).
  nullif(trim(cast(postal_code as varchar)), '') as postal_code,

  -- Active flag standardized to boolean.
  cast(is_active as boolean) as is_active,

  -- Outlet creation timestamp (timezone-aware).
  -- try_cast avoids failing the model if the raw value is malformed in any row.
  try_cast(created_at as timestamptz) as created_at

from src
