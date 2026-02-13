-- Dimension: Currencies.
-- Goal:
--   Provide a canonical lookup for currency attributes so facts can be reported with human-readable codes/names.
--
-- Grain:
--   One row per currency_id.
--
-- Notes:
--   - This is a direct pass-through from the staging model, which standardizes casing and data types.
--   - currency_code (ISO-like 3-letter code) is typically used in dashboards; currency_name is for readability.
--   - is_active allows filtering out deprecated currencies while preserving historical rows if they exist.

select
    currency_id,    -- Surrogate/natural key from the source system.
    currency_code,  -- 3-letter currency code (uppercased in staging).
    currency_name,  -- Currency display name.
    is_active,      -- Whether the currency is currently active/valid in the system.
    created_at      -- When the currency record was created in the source system.
from {{ ref('stg_raw__currencies') }}
