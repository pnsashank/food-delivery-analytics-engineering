-- SCD2 Restaurant dimension built from the dbt snapshot (snap_restaurants).
-- Goal:
--   Expose a versioned (type-2) dimension for restaurants/outlets so fact tables can join to the
--   correct historical attributes as-of an order timestamp.
--
-- Snapshot mechanics (high level):
--   - snap_restaurants stores multiple versions per restaurant_id whenever any "check_cols" change.
--   - dbt automatically provides:
--       * dbt_scd_id      : stable surrogate key per version
--       * dbt_valid_from  : timestamp when this version became active
--       * dbt_valid_to    : timestamp when this version ended (null => current)
--
-- Output grain:
--   One row per restaurant_id per SCD2 version.

select
    -- Surrogate key for this specific SCD2 version (used for fact joins).
    dbt_scd_id as restaurant_sk,

    -- Natural/business key for the restaurant/outlet.
    restaurant_id,

    -- Brand attributes captured at the time of the snapshot.
    brand_id,
    brand_name,
    brand_is_active,

    -- Outlet attributes captured at the time of the snapshot.
    outlet_name,
    city,
    delivery_zone,
    address_line1,
    postal_code,
    is_active,

    -- SCD2 validity window for this version.
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,

    -- Convenience flag for selecting only the current version.
    (dbt_valid_to is null) as is_current

from {{ ref('snap_restaurants') }}
