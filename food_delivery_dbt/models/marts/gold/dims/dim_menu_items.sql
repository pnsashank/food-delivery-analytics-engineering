-- SCD2 Menu Item Dimension built from a dbt snapshot.
-- Goal:
--   Provide a slowly-changing dimension (Type 2) for menu items so fact tables can join to the
--   correct historical version of a menu item at the time an order occurred.
--
-- Source:
--   snap_menu_items is a dbt snapshot using the "check" strategy on selected columns
--   (e.g., item_name, category). dbt automatically adds snapshot metadata columns:
--     - dbt_scd_id      : stable surrogate identifier per versioned row
--     - dbt_valid_from  : start timestamp for the version validity window
--     - dbt_valid_to    : end timestamp for the version window (null for current)
--
-- Grain:
--   One row per menu_item_id per version (i.e., one row for each period where attributes stayed
--   unchanged). Multiple rows can exist per menu_item_id over time.
--
-- Notes:
--   This model is intentionally thin: it mainly renames dbt snapshot metadata into warehouse-
--   friendly column names and exposes an is_current flag for convenience.

select
    dbt_scd_id as menu_item_sk,                 -- Surrogate key for this SCD version.
    menu_item_id,                              -- Natural/business key for the menu item.
    restaurant_id,                             -- Restaurant/outlet that owns the menu item.
    item_name,                                 -- Snapshotted attribute (changes tracked over time).
    category,                                  -- Snapshotted attribute (changes tracked over time).
    dbt_valid_from as valid_from,               -- Version start timestamp.
    dbt_valid_to as valid_to,                   -- Version end timestamp (null => current row).

    -- Convenience flag for filtering the "current" version in analytics.
    (dbt_valid_to is null) as is_current

from {{ ref('snap_menu_items') }}
