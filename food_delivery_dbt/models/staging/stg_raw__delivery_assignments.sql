-- Source CTE.
-- Purpose: Read raw delivery_assignments once, then apply consistent typing for joins and timestamps.
with src as (
  select * from {{ source('raw', 'delivery_assignments') }}
)

select
  -- Enforce integer key types for fact/dim joins.
  cast(order_id as bigint) as order_id,
  cast(courier_id as bigint) as courier_id,

  -- Safely parse timestamps coming from raw Parquet.
  -- try_cast keeps the model resilient if any row contains malformed timestamp values.
  try_cast(assigned_at as timestamptz) as assigned_at,
  try_cast(pickup_eta as timestamptz) as pickup_eta,
  try_cast(dropoff_eta as timestamptz) as dropoff_eta

from src
