-- Source CTE.
-- Purpose: Read raw customers once, then apply all type enforcement + cleaning in a single select.
with src as (
    select * from {{ source('raw', 'customers')}}
)

select 
    -- Enforce a stable integer surrogate key type for joins across the warehouse.
    cast(customer_id as bigint) as customer_id,

    -- Normalize full_name:
    -- 1) cast -> varchar for string ops
    -- 2) trim -> remove leading/trailing whitespace
    -- 3) nullif(..., '') -> treat empty strings as NULL for cleaner downstream constraints/tests
    nullif(trim(cast(full_name as varchar)), '') as full_name,

    -- Normalize email:
    -- trim + nullify empties to avoid invalid empty-string "emails".
    -- This supports dbt tests like not_null/unique and expression checks (e.g., email like '%@%').
    nullif(trim(cast(email as varchar)), '') as email,

    -- Normalize phone:
    -- Optional field; trim + nullify empties so downstream models don't treat '' as a real value.
    nullif(trim(cast(phone as varchar)), '') as phone,

    -- Parse created_at safely:
    -- try_cast prevents model failure if a malformed timestamp appears in raw.
    try_cast(created_at as timestamptz) as created_at 

from src
