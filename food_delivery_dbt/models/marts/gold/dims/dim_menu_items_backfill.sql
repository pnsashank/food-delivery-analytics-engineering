-- Backfill helper for SCD2 dimension: Menu Items.
-- Goal:
--   Ensure the first historical version for each natural key (menu_item_id) has a deterministic,
--   open-ended "start of time" valid_from value. This makes time-range joins predictable when
--   older fact records exist before the first captured snapshot change.
--
-- Why this is needed:
--   dbt snapshots produce dbt_valid_from/dbt_valid_to based on when a change is first observed.
--   If the very first snapshot row for a menu_item_id starts at a "later" timestamp, facts with
--   order_placed_at earlier than that may fail the SCD join (no matching version).
--
-- Approach:
--   1) Take all rows from dim_menu_items (already derived from the snapshot).
--   2) Rank rows per menu_item_id by valid_from ascending.
--   3) For the first row only, override valid_from to a fixed sentinel timestamp:
--        '1900-01-01 00:00:00'
--   4) Keep valid_to and is_current unchanged.
--
-- Grain:
--   One row per (menu_item_sk) which represents a specific SCD version for a menu_item_id.

with menu_item_details as (

    select
        menu_item_sk,                       -- Surrogate key for the SCD version (dbt_scd_id).
        menu_item_id,                       -- Natural/business key.
        item_name,                          -- Tracked attributes (change-detected in snapshot).
        category,
        restaurant_id,                      -- Context/ownership of the menu item.
        valid_from,                         -- Start of validity window as produced by the snapshot.
        valid_to,                           -- End of validity window (null means current row).
        is_current,                         -- Convenience flag derived from valid_to is null.

        -- Rank SCD versions from earliest to latest per natural key.
        row_number() over (
            partition by menu_item_id
            order by valid_from
        ) as row_number

    from {{ ref('dim_menu_items') }}

)

select
    menu_item_sk,
    menu_item_id,
    item_name,
    category,
    restaurant_id,

    -- Force the first observed version to start from a sentinel "start of time" value so that
    -- point-in-time joins always find a matching row for earlier fact timestamps.
    case
        when row_number = 1 then timestamp '1900-01-01 00:00:00'
        else valid_from
    end as valid_from,

    valid_to,
    is_current
from menu_item_details
