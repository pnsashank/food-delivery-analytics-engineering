-- Backfill helper for the Restaurant SCD2 dimension.
-- Goal:
--   Ensure the earliest SCD2 row per restaurant_id has a "beginning of time" valid_from so
--   time-range joins from fact tables never miss historical rows that occurred before the first
--   captured snapshot timestamp.
--
-- Context:
--   dim_restaurants is an SCD2 dimension built from a dbt snapshot. For each restaurant_id,
--   multiple versions can exist over time. The first version’s valid_from reflects when the snapshot
--   first observed the row (not necessarily when the restaurant logically existed).
--
-- Approach:
--   1) Rank SCD versions per restaurant_id by valid_from ascending.
--   2) For the first version (row_number = 1), replace valid_from with a fixed early timestamp.
--   3) Keep all other versions unchanged.
--
-- Output grain:
--   One row per restaurant_id per SCD2 version, identical to dim_restaurants, but with the first
--   version’s valid_from backfilled.

with restaurant_details as (
    select
        restaurant_sk,                                          -- SCD2 surrogate key for this version.
        restaurant_id,                                          -- Natural/business key.
        brand_id,                                               -- Brand identifier for the restaurant/outlet.
        brand_name,                                             -- Denormalized brand name captured in the snapshot.
        outlet_name,                                            -- Restaurant/outlet name.
        city,                                                   -- Outlet city.
        delivery_zone,                                          -- Delivery zone/area.
        is_active,                                              -- Outlet status flag.

        valid_from,                                             -- SCD2 version start timestamp.
        valid_to,                                               -- SCD2 version end timestamp (null => current).
        is_current,                                             -- Convenience flag for current version.

        row_number() over (
            partition by restaurant_id
            order by valid_from
        ) as row_number                                         -- Rank versions (earliest = 1) per restaurant_id.
    from {{ ref('dim_restaurants') }}
)

select
    restaurant_sk,
    restaurant_id,
    brand_id,
    brand_name,
    outlet_name,
    city,
    delivery_zone,
    is_active,

    -- Backfill the first observed version to a "beginning of time" timestamp.
    -- This prevents fact-to-dimension time joins from failing for very early fact timestamps.
    case
        when row_number = 1 then timestamp '1900-01-01 00:00:00'
        else valid_from
    end as valid_from,

    valid_to,
    is_current
from restaurant_details
