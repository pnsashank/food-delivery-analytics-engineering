-- Dimension: Couriers.
-- Goal:
--   Provide a stable, query-friendly lookup table for courier attributes used in reporting.
--
-- Grain:
--   One row per courier_id.
--
-- Notes:
--   - This is a direct pass-through from the staging model, which already standardizes types and formatting.
--   - is_active supports filtering to currently active couriers in operational and KPI reporting.
--   - created_at can be used for cohorting (e.g., new couriers over time) if needed.

select
    courier_id,   -- Natural key for the courier.
    city,         -- Primary operating city (used for geo slicing/filters).
    vehicle,      -- Vehicle type (standardized in staging, e.g., BIKE/SCOOTER/CAR).
    is_active,    -- Current active flag for the courier.
    created_at    -- When the courier record was created in the source system.
from {{ ref('stg_raw__couriers') }}
