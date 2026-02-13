-- Source CTE.
-- Purpose: Read raw ratings and normalize types/strings so joins and tests are consistent.
with src as (
  select * from {{ source('raw', 'ratings') }}
)

select
  -- Surrogate primary key from OLTP.
  cast(rating_id as bigint) as rating_id,

  -- Order being rated (1:1 rating per order in OLTP).
  cast(order_id as bigint) as order_id,

  -- Customer who submitted the rating.
  cast(customer_id as bigint) as customer_id,

  -- Restaurant rating score (expected 1–5).
  cast(restaurant_rating as integer) as restaurant_rating,

  -- Courier rating score (nullable; expected 1–5 when present).
  cast(courier_rating as integer) as courier_rating,

  -- Free-text comment:
  -- 1) cast to varchar
  -- 2) trim whitespace
  -- 3) convert empty strings to NULL to avoid meaningless blanks in analytics.
  nullif(trim(cast(comment as varchar)), '') as comment,

  -- Rating creation timestamp.
  -- try_cast protects the pipeline if upstream data contains invalid timestamp strings.
  try_cast(created_at as timestamptz) as created_at

from src
