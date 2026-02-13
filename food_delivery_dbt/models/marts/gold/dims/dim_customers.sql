-- Dimension: Customers.
-- Goal:
--   Provide a clean, de-duplicated customer lookup for joining into facts (orders, refunds, ratings) and for customer-level analysis.
--
-- Grain:
--   One row per customer_id.
--
-- Notes:
--   - This is sourced directly from staging, where strings are trimmed and empty strings are converted to NULL.
--   - email is expected to be unique and not null (enforced via dbt tests and upstream constraints).
--   - phone is optional and may be NULL depending on source completeness.
--   - created_at is kept as the operational creation timestamp (useful for cohorting by signup date).

select
    customer_id,  -- Primary customer identifier (natural key in this model).
    full_name,    -- Customer full name (cleaned in staging).
    email,        -- Customer email (unique identifier for contact/identity use cases).
    phone,        -- Optional phone number.
    created_at    -- Source-system timestamp when the customer record was created.
from {{ ref('stg_raw__customers') }}
