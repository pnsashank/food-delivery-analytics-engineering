-- Intermediate model: Delivery assignment enrichment.
-- Purpose:
--   1) Standardize a delivery-focused dataset at one row per order_id.
--   2) Provide courier assignment and ETA timestamps for downstream order facts.
-- Grain:
--   One row per order_id (delivery_assignments uses order_id as the primary key).

select 
    -- Natural key for the delivery assignment (and the parent order).
    order_id,

    -- Assigned courier responsible for the delivery.
    courier_id,

    -- Timestamp when the courier was assigned to the order.
    assigned_at,

    -- Operational ETA fields (may be null depending on data availability).
    pickup_eta,
    dropoff_eta
from {{ ref('stg_raw__delivery_assignments') }}
