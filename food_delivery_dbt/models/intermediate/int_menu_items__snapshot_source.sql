-- Snapshot source: Menu items (natural key = menu_item_id).
-- Purpose:
--   Provide a clean, stable input for a dbt snapshot so historical changes to
--   menu item descriptive attributes can be tracked over time (SCD Type 2).
--
-- Why this exists:
--   - Snapshots should be built from a minimal set of columns that define the
--     business meaning of the entity and are expected to change (slowly).
--   - Keeping the snapshot source narrow reduces noise and avoids triggering new
--     snapshot versions due to irrelevant operational fields.
--
-- Grain:
--   One row per menu_item_id (expected).
--
-- Column intent:
--   - menu_item_id: Natural/business key used as the snapshot unique_key.
--   - restaurant_id: Relationship to the owning restaurant outlet.
--   - item_name, category: Descriptive attributes monitored for changes (check_cols).

select 
    menu_item_id,
    restaurant_id,
    item_name,
    category
from {{ ref('stg_raw__menu_items') }}
