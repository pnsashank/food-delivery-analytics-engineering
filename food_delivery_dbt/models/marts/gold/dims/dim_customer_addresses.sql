-- Dimension: Customer addresses.
-- Goal:
--   Provide a stable, reusable address lookup that facts (orders) can join to for delivery location context.
--
-- Grain:
--   One row per address_id.
--
-- Notes:
--   - This is a direct pass-through from staging, where string fields are trimmed/null-normalized and types are cast.
--   - label indicates the address purpose (HOME/WORK/OTHER) and is validated via tests.
--   - is_default supports the business rule “at most one default address per customer” (enforced upstream in Postgres).
--   - Latitude/longitude are stored to enable geo analysis (distance, zones, heatmaps) later.

select
    address_id,     -- Surrogate key for an address record.
    customer_id,    -- Owning customer (FK to dim_customers/customer_id).
    label,          -- Address label: HOME/WORK/OTHER.
    line_1,         -- Primary address line.
    line_2,         -- Optional secondary address line.
    city,           -- City for aggregation and delivery operations.
    state,          -- Optional state/region.
    country,        -- Country for geography and compliance reporting.
    postal_code,    -- Postal/ZIP code (optional, depends on source quality).
    latitude,       -- Decimal latitude (if available).
    longitude,      -- Decimal longitude (if available).
    is_default,     -- Whether this is the customer's default delivery address.
    created_at      -- Source-system creation timestamp.
from {{ ref('stg_raw__customer_addresses') }}
