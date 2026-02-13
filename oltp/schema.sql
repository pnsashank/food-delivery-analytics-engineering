create schema if not exists oltp; -- Logical namespace for OLTP (source-of-truth) tables and enums.

do $$
begin
  -- Creates the order lifecycle enum exactly once (idempotent migrations).
  if not exists (
    select 1
    from pg_type t
    join pg_namespace n on n.oid = t.typnamespace
    where t.typname = 'order_status' and n.nspname = 'oltp'
  ) then
    create type oltp.order_status as enum (
      'PLACED',
      'ACCEPTED',
      'PREP_START',
      'READY_FOR_PICKUP',
      'PICKED_UP',
      'DELIVERED',
      'CANCELED'
    );
  end if;

  -- Actor that produced a status event (useful for auditability and attribution).
  if not exists (
    select 1
    from pg_type t
    join pg_namespace n on n.oid = t.typnamespace
    where t.typname = 'actor_type' and n.nspname = 'oltp'
  ) then
    create type oltp.actor_type as enum ('SYSTEM','RESTAURANT','COURIER','CUSTOMER');
  end if;

  -- Normalized payment method values; uppercase + underscores keeps values stable across systems.
  if not exists (
    select 1
    from pg_type t
    join pg_namespace n on n.oid = t.typnamespace
    where t.typname = 'payment_method' and n.nspname = 'oltp'
  ) then
    create type oltp.payment_method as enum (
      'CARD',
      'DIGITAL_WALLET',
      'CONTACTLESS_NFC',
      'CASH',
      'PAYPAL',
      'BANK_TRANSFER'
    );
  end if;

  -- Payment lifecycle state; used for finance/ops reporting and reconciliation.
  if not exists (
    select 1
    from pg_type t
    join pg_namespace n on n.oid = t.typnamespace
    where t.typname = 'payment_status' and n.nspname = 'oltp'
  ) then
    create type oltp.payment_status as enum ('PENDING','AUTHORIZED','PAID','FAILED','REFUNDED');
  end if;

  -- Courier vehicle enum keeps analytics clean and prevents free-text drift.
  if not exists (
    select 1
    from pg_type t
    join pg_namespace n on n.oid = t.typnamespace
    where t.typname = 'vehicle_type' and n.nspname = 'oltp'
  ) then
    create type oltp.vehicle_type as enum ('BIKE','SCOOTER','CAR');
  end if;

  -- Refund reasons are bounded for reliable aggregation.
  if not exists (
    select 1
    from pg_type t
    join pg_namespace n on n.oid = t.typnamespace
    where t.typname = 'refund_reason' and n.nspname = 'oltp'
  ) then
    create type oltp.refund_reason as enum (
      'LATE_DELIVERY',
      'MISSING_ITEM',
      'WRONG_ITEM',
      'QUALITY_ISSUE',
      'OTHER'
    );
  end if;

  -- Address labels (referenced by customer_addresses.label); created explicitly to avoid runtime failures.
  if not exists (
    select 1
    from pg_type t
    join pg_namespace n on n.oid = t.typnamespace
    where t.typname = 'address_label' and n.nspname = 'oltp'
  ) then
    create type oltp.address_label as enum ('HOME','WORK','OTHER');
  end if;
end $$;

-- CUSTOMERS
create table if not exists oltp.customers (
  customer_id bigserial primary key,                 -- Surrogate key for joins and stability.
  full_name text not null,                           -- Customer display name.
  email text not null unique,                        -- Unique identifier used by many systems.
  phone text,                                        -- Optional; not all customers provide.
  created_at  timestamptz not null default now()      -- Insert time in UTC-aware timestamp.
);

create table if not exists oltp.customer_addresses (
  address_id bigserial primary key,                                  -- Surrogate key per address.
  customer_id bigint not null references oltp.customers(customer_id)  -- Address belongs to a customer.
    on delete cascade,                                               -- Delete addresses when customer is deleted.
  label oltp.address_label not null default 'OTHER',                 -- HOME/WORK/OTHER.
  line_1 text not null,                                              -- Required address line.
  line_2 text,                                                       -- Optional apartment/unit.
  city text not null,
  state text,
  country text not null,
  postal_code text,
  latitude numeric(9,6),                                             -- Optional geocoding fields for distance/zone logic.
  longitude numeric(9,6),
  is_default boolean not null default false,                         -- Preferred address for the customer.
  created_at timestamptz not null default now()
);

create index if not exists idx_addresses_customer
  on oltp.customer_addresses(customer_id); -- Supports customer -> addresses lookups and FK joins.

create unique index if not exists uq_default_address_per_customer
  on oltp.customer_addresses(customer_id)
  where is_default; -- Enforces at most one default address per customer (partial unique index).

-- RESTAURANTS: BRANDS/OUTLETS
create table if not exists oltp.restaurant_brands (
  brand_id bigserial primary key,                 -- Brand surrogate key.
  brand_name text not null unique,                -- Natural identifier for the brand.
  is_active boolean not null default true,        -- Soft-active flag for filtering and history.
  created_at timestamptz not null default now()
);

create table if not exists oltp.restaurant_outlets (
  restaurant_id  bigserial primary key,  -- Outlet surrogate key (kept as restaurant_id for simplicity).
  brand_id bigint not null references oltp.restaurant_brands(brand_id), -- Outlet belongs to a brand.
  outlet_name text not null,
  city text not null,
  delivery_zone text not null,           -- Partitioning/ops concept used for dispatching.
  address_line1 text,
  postal_code text,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  constraint uq_brand_outlet unique (brand_id, outlet_name, city, delivery_zone) -- Prevents duplicates for same outlet identity.
);

create index if not exists idx_outlets_brand
  on oltp.restaurant_outlets(brand_id); -- Speeds joins/filtering by brand.

create index if not exists idx_outlets_city_zone
  on oltp.restaurant_outlets(city, delivery_zone); -- Supports geo/zone queries (dispatch + analytics).

-- MENU ITEMS
create table if not exists oltp.menu_items (
  menu_item_id   bigserial primary key,  -- Menu item surrogate key.
  restaurant_id  bigint not null references oltp.restaurant_outlets(restaurant_id), -- Item belongs to an outlet.
  item_name text not null,
  category text not null,
  price numeric(10,2) not null check (price >= 0), -- Currency-specific price; constrained to non-negative.
  is_available boolean not null default true,       -- Availability flag for ordering / menus.
  created_at timestamptz not null default now()
);

create index if not exists idx_menu_items_restaurant
  on oltp.menu_items(restaurant_id); -- Frequent lookup: items by restaurant.

-- COURIERS
create table if not exists oltp.couriers (
  courier_id bigserial primary key,                 -- Courier surrogate key.
  city text not null,                               -- Home/operating city.
  vehicle oltp.vehicle_type not null,               -- BIKE/SCOOTER/CAR.
  is_active  boolean not null default true,         -- Soft-active for workforce filtering.
  created_at timestamptz not null default now()
);

-- ORDERS
create table if not exists oltp.orders (
  order_id bigserial primary key, -- Order surrogate key.
  customer_id bigint not null references oltp.customers(customer_id), -- Customer who placed the order.
  delivery_address_id bigint not null references oltp.customer_addresses(address_id), -- Delivery destination.
  restaurant_id bigint not null references oltp.restaurant_outlets(restaurant_id), -- Fulfillment outlet.
  order_placed_at timestamptz not null, -- Business timestamp for when order was placed.
  scheduled_delivery  timestamptz null, -- Optional scheduled delivery (future delivery use case).
  subtotal numeric(12,2) not null check (subtotal >= 0),       -- Items subtotal (pre-tax, pre-fees).
  tax numeric(12,2) not null check (tax >= 0),
  delivery_fee numeric(12,2) not null check (delivery_fee >= 0),
  discount numeric(12,2) not null check (discount >= 0),
  total_amount numeric(12,2) not null check (total_amount >= 0), -- Customer-charged total.
  payment_method oltp.payment_method not null,
  payment_status oltp.payment_status not null default 'PENDING',

  constraint orders_total_consistency
    check (round(subtotal + tax + delivery_fee - discount, 2) = round(total_amount, 2)) -- Enforces arithmetic integrity.
);

create index if not exists idx_orders_customer_time
  on oltp.orders(customer_id, order_placed_at desc); -- Common access pattern: recent orders per customer.

-- ORDER ITEMS
create table if not exists oltp.order_items (
  order_item_id bigserial primary key, -- Line item surrogate key.
  order_id bigint not null references oltp.orders(order_id) on delete cascade, -- Child of order.
  menu_item_id bigint not null references oltp.menu_items(menu_item_id), -- Item ordered.
  quantity integer not null check (quantity > 0),
  unit_price numeric(10,2) not null check (unit_price >= 0), -- Captured at order time (price can change later).
  line_total  numeric(12,2) not null check (line_total >= 0), -- quantity * unit_price rounded.

  constraint order_items_line_total_consistency
    check (round(quantity * unit_price, 2) = round(line_total, 2)) -- Prevents inconsistent line totals.
);

-- STATUS EVENT HISTORY
create table if not exists oltp.order_status_events (
  event_id  bigserial primary key, -- Event surrogate key.
  order_id  bigint not null references oltp.orders(order_id) on delete cascade, -- Child of order.
  event_ts  timestamptz not null, -- When the status transition was recorded.
  status oltp.order_status not null, -- Lifecycle step.
  actor  oltp.actor_type not null, -- Who produced the event.
  notes text -- Optional free-form debugging / operational notes.
);

create index if not exists idx_events_order_time
  on oltp.order_status_events(order_id, event_ts); -- Supports rollups: first/last status by order.

-- DELIVERY ASSIGNMENT
create table if not exists oltp.delivery_assignments (
  order_id bigint primary key references oltp.orders(order_id) on delete cascade, -- 1:1 with order (at most one active assignment record).
  courier_id bigint not null references oltp.couriers(courier_id), -- Assigned courier.
  assigned_at timestamptz not null, -- When assignment happened.
  pickup_eta timestamptz,           -- Optional ETA fields used for SLA/latency metrics.
  dropoff_eta timestamptz
);

create index if not exists idx_assignments_courier
  on oltp.delivery_assignments(courier_id); -- Enables courier workload queries.

-- REFUNDS
create table if not exists oltp.refunds (
  refund_id bigserial primary key, -- Refund surrogate key.
  order_id bigint not null references oltp.orders(order_id) on delete cascade, -- Refund belongs to an order.
  refund_ts timestamptz not null, -- Refund timestamp (business/ops time).
  refund_reason oltp.refund_reason not null, -- Controlled vocabulary for aggregation.
  refund_amount numeric(12,2) not null check (refund_amount >= 0) -- Non-negative amounts only.
);

-- RATINGS
create table if not exists oltp.ratings (
  rating_id bigserial primary key, -- Rating surrogate key.
  order_id bigint not null unique references oltp.orders(order_id) on delete cascade, -- 1 rating per order max.
  customer_id  bigint not null references oltp.customers(customer_id), -- Rated by this customer.
  restaurant_rating integer not null check (restaurant_rating between 1 and 5),
  courier_rating integer check (courier_rating between 1 and 5), -- Optional (can be NULL).
  comment text, -- Optional free-text.
  created_at timestamptz not null default now() -- Insert time; differs from "event time" if needed.
);
