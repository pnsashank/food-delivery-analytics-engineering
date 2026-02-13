-- Intermediate model: Order status rollup.
-- Purpose:
--   Convert the event-level status history (order_status_events) into a single
--   order-grain record with milestone timestamps, latest status, and duration metrics.
--
-- What this produces:
--   - placed_at/accepted_at/...: first timestamp when each milestone status occurred
--   - latest_status/latest_status_at: most recent status and its timestamp
--   - is_delivered/is_canceled: boolean flags derived from milestone presence
--   - minutes_to_*: derived elapsed times between key milestones
--
-- Notes:
--   - Uses MIN(...) to capture the earliest time each status was reached.
--   - Uses arg_max(status, event_ts) to capture the status at the maximum event_ts.
--   - date_diff(...) will return NULL when either input timestamp is NULL.

with events as (
    -- Base event stream at the event grain.
    -- Keep only columns needed for milestone rollups and latest-status logic.
    select
        order_id,
        event_ts,
        status
    from {{ ref('stg_raw__order_status_events') }}
),

aggregation as (
    -- Aggregate to one row per order_id.
    -- Extract milestone timestamps and the latest observed status.
    select
        order_id,

        -- Earliest occurrence of each status (milestone timestamps).
        min(case when status = 'PLACED' then event_ts end) as placed_at,
        min(case when status = 'ACCEPTED' then event_ts end) as accepted_at,
        min(case when status = 'PREP_START' then event_ts end) as prep_start_at,
        min(case when status = 'READY_FOR_PICKUP' then event_ts end) as ready_for_pickup_at,
        min(case when status = 'PICKED_UP' then event_ts end) as picked_up_at,
        min(case when status = 'DELIVERED' then event_ts end) as delivered_at,
        min(case when status = 'CANCELED' then event_ts end) as canceled_at,

        -- Latest status according to the greatest event timestamp.
        arg_max(status, event_ts) as latest_status,
        max(event_ts) as latest_status_at
    from events
    group by order_id
)

select
    -- Natural key.
    order_id,

    -- Milestone timestamps.
    placed_at,
    accepted_at,
    prep_start_at,
    ready_for_pickup_at,
    picked_up_at,
    delivered_at,
    canceled_at,

    -- Latest status snapshot.
    latest_status,
    latest_status_at,

    -- Convenience flags for downstream models/tests.
    (delivered_at is not null) as is_delivered,
    (canceled_at is not null) as is_canceled,

    -- Duration metrics (minutes). Will be NULL if required timestamps are missing.
    date_diff('minute', placed_at, accepted_at) as minutes_to_accept,
    date_diff('minute', placed_at, picked_up_at) as minutes_to_pickup,
    date_diff('minute', placed_at, delivered_at) as minutes_to_deliver,
    date_diff('minute', picked_up_at, delivered_at) as minutes_pickup_to_deliver

from aggregation
