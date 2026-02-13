-- Snapshot source: Restaurant outlets enriched with brand details.
-- Goal:
--   Produce a single, denormalized row per restaurant outlet (restaurant_id) that contains
--   both outlet attributes and the related brand attributes, so snapshots can capture
--   historical changes to either side.
--
-- Grain:
--   One row per restaurant_id from stg_raw__restaurant_outlets.
--
-- Notes:
--   - LEFT JOIN keeps outlets even if the brand lookup is missing (useful for data-quality visibility).
--   - brand_is_active is renamed to avoid confusion with outlet is_active.

with outlets as (
    -- Outlet (restaurant) master data: operational fields about each physical/virtual outlet.
    select
        restaurant_id,
        brand_id,
        outlet_name,
        city,
        delivery_zone,
        address_line1,
        postal_code,
        is_active
    from {{ ref('stg_raw__restaurant_outlets') }}
),

brands as (
    -- Brand master data: descriptive fields about the parent brand.
    select
        brand_id,
        brand_name,
        is_active as brand_is_active
    from {{ ref('stg_raw__restaurant_brands') }}
)

select
    -- Natural key for outlet-level tracking.
    outlets.restaurant_id,

    -- Brand foreign key (association of outlet to brand).
    outlets.brand_id,

    -- Brand attributes (changes here should be tracked as part of the restaurant snapshot context).
    brands.brand_name,
    brands.brand_is_active,

    -- Outlet attributes (changes here should be tracked directly).
    outlets.outlet_name,
    outlets.city,
    outlets.delivery_zone,
    outlets.address_line1,
    outlets.postal_code,
    outlets.is_active

from outlets
left join brands
  on outlets.brand_id = brands.brand_id
