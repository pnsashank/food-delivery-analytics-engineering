# Food Delivery Analytics Engineering (Postgres → Parquet → DuckDB → dbt)

End-to-end analytics engineering project that:
1) builds an OLTP schema in PostgreSQL  
2) exports OLTP tables to Parquet (bronze)  
3) creates DuckDB `raw.*` views over the Parquet files  
4) runs dbt to build staging → intermediate → gold (dims/facts) models  
5) uses dbt snapshots for slowly-changing dimensions (restaurants, menu items)  
6) validates data quality with dbt tests (schema tests + custom SQL tests)

---

## About this project

This repository is an end-to-end analytics engineering project that simulates a food delivery platform and builds a production-style analytics warehouse locally.

### Goal
Start from an OLTP-style source database (PostgreSQL) and produce clean, well-tested analytics tables (DuckDB + dbt) that support reporting and analysis such as order performance, delivery timelines, menu item trends, restaurant changes over time, and refunds.

### What it builds
- A full pipeline: **Postgres (OLTP) → Parquet (bronze) → DuckDB raw views → dbt staging → dbt intermediate → dbt gold star schema**
- **Staging models** standardize data types, clean strings, and parse timestamps.
- **Intermediate models** compute order rollups, status timelines (placed/accepted/picked up/delivered), delivery enrichment, and reconciliation checks.
- **Gold models** create a star schema:
  - Dimensions (customers, restaurants, menu items, couriers, currencies, date)
  - Facts (orders, order_items, refunds, fx_rates)
- **SCD2 snapshots** track changes to restaurants and menu items over time and join the correct historical version into facts.

### Why this matters (analytics engineering focus)
- Demonstrates typical warehouse practices: layered modeling, SCD handling, referential integrity tests, business-rule tests, and reproducible local setup.
- The end result is a set of query-ready fact/dimension tables suitable for BI dashboards or ad-hoc analytics.

---

## Tech stack

- PostgreSQL (OLTP source)
- Python (export + automation scripts)
- DuckDB (local analytics warehouse)
- dbt (models + snapshots + tests)
- dbt-utils (test macros)

---

## Prerequisites

### Install Postgres + DuckDB

```bash
brew install postgresql duckdb
brew services start postgresql
```
## Setup

### 0) Python environment + dependencies (requirements.txt)

From repo root:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 1) Create PostgreSQL database + OLTP schema

From repo root:

Create database:

```bash
createdb food_delivery
```

Apply schema files:

```bash
psql -d food_delivery -f oltp/schema.sql
psql -d food_delivery -f oltp/new_update_schema.sql
```

(Optional) Verify tables:

```bash
psql -d food_delivery -c "\dt oltp.*"
```

### 2) Populate OLTP (optional sample data)

```bash
source venv/bin/activate
python oltp/populate_oltp.py
```

### 3) Export Postgres → Parquet (bronze layer)

Output directory:

`food_delivery_dbt/data/bronze/`

Run:

```bash
python postgres_to_duckdb.py
```

Expected outputs:

```text
food_delivery_dbt/data/bronze/
├─ customers.parquet
├─ customer_addresses.parquet
├─ restaurant_brands.parquet
├─ restaurant_outlets.parquet
├─ menu_items.parquet
├─ couriers.parquet
├─ currencies.parquet
├─ delivery_assignments.parquet
├─ ratings.parquet
├─ orders/...
├─ order_items/...
├─ order_status_events/...
├─ refunds/...
└─ fx_rates/...
```

### 4) Create DuckDB raw views over Parquet

```bash
duckdb food_delivery_dbt/warehouse/food_delivery.duckdb < food_delivery_dbt/warehouse/sql/create_raw_views.sql
```

Verify:

```bash
duckdb food_delivery_dbt/warehouse/food_delivery.duckdb -c "show schemas;"
duckdb food_delivery_dbt/warehouse/food_delivery.duckdb -c "show tables in raw;"
duckdb food_delivery_dbt/warehouse/food_delivery.duckdb -c "select count(*) from raw.orders;"
```

### 5) dbt setup

```bash
cd food_delivery_dbt
dbt init
dbt deps
```

### 6) Configure dbt profile (~/.dbt/profiles.yml)

Create/edit:

`~/.dbt/profiles.yml`

Recommended portable version (uses env var for the DuckDB file path):

```yaml
food_delivery_dbt:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('DUCKDB_PATH') }}"
      threads: 4
```

Set the env var from inside the dbt project directory:

```bash
export DUCKDB_PATH="$(pwd)/warehouse/food_delivery.duckdb"
```

Confirm dbt can connect:

```bash
dbt debug
```

### 7) Run snapshots + build models

This project snapshots:

- `snap_restaurants` (restaurant attributes SCD2)
- `snap_menu_items` (menu items attributes SCD2)

Run snapshots:

```bash
dbt snapshot
```

Build everything (models + tests):

```bash
dbt build
```

Other useful commands:

```bash
dbt run
dbt test
```

---

## Data model overview

### Bronze (files)
Parquet files in `food_delivery_dbt/data/bronze/`

### Raw (DuckDB views)
`raw.*` views created by `warehouse/sql/create_raw_views.sql`

Purpose: expose Parquet as queryable sources for dbt.

### Staging models (`stg_raw__*`)
- Type casting (IDs, decimals, timestamps)
- String cleanup (`trim`, `nullif`, `upper`)
- Parsed timestamps into `timestamptz`
- Derived date fields (`*_date_utc`)

### Intermediate models

**Orders**
- `int_orders__items_rollup`: item totals and counts at order grain
- `int_orders__status_rollup`: per-status timestamps + SLA metrics
- `int_orders__delivery_enriched`: delivery assignment enrichment
- `int_orders__reconciliation`: expected totals and differences
- `int_orders__enriched`: unified order-grain dataset

**Items**
- `int_order_items__enriched`: item grain enriched with order + menu item context

**Refunds**
- `int_refunds__enriched`: refund grain enriched with order context + checks

### Gold star schema

**Dimensions**
- `dim_customers`
- `dim_customer_addresses`
- `dim_couriers`
- `dim_currencies`
- `dim_date`
- `dim_restaurants` (SCD2 snapshot)
- `dim_menu_items` (SCD2 snapshot)

**Facts**
- `fct_orders` (order grain, joins restaurant SCD2 at order time)
- `fct_order_items` (order-item grain, joins menu-item SCD2 at order time)
- `fct_refunds` (refund grain with order + rollups)
- `fct_fx_rates` (fx rates enriched with currency codes)

---

## Data quality checks

### Schema tests (YAML)
- `not_null` / `unique` on keys
- `relationships` between facts/dims
- `accepted_values` for enum-like fields
- SCD window sanity:
  - `valid_to > valid_from`
  - one current row per natural key

### Custom SQL tests (`food_delivery_dbt/tests/`)

Examples:
- FX:
  - no same-currency pair
  - unique pair + timestamp
  - positive/reasonable rates
- Orders:
  - delivered implies delivered_at exists
  - status timestamps are ordered correctly
  - total consistency (`subtotal + tax + fee - discount ≈ total`)
- Refunds:
  - refund timestamp after order placed
  - refund does not exceed order total (tolerance)

Run:

```bash
dbt test
```

---

## End-to-end quickstart

From repo root:

```bash
brew install postgresql duckdb
brew services start postgresql

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

createdb food_delivery
psql -d food_delivery -f oltp/schema.sql
psql -d food_delivery -f oltp/new_update_schema.sql

python oltp/populate_oltp.py
python postgres_to_duckdb.py

duckdb food_delivery_dbt/warehouse/food_delivery.duckdb < food_delivery_dbt/warehouse/sql/create_raw_views.sql

cd food_delivery_dbt
export DUCKDB_PATH="$(pwd)/warehouse/food_delivery.duckdb"
dbt deps
dbt debug
dbt snapshot
dbt build
```

---

## OLTP ER diagram

```mermaid
erDiagram
  CUSTOMERS ||--o{ CUSTOMER_ADDRESSES : has
  CUSTOMERS ||--o{ ORDERS : places
  CUSTOMER_ADDRESSES ||--o{ ORDERS : used_for_delivery

  RESTAURANT_BRANDS ||--o{ RESTAURANT_OUTLETS : owns
  RESTAURANT_OUTLETS ||--o{ MENU_ITEMS : offers

  COURIERS ||--o{ DELIVERY_ASSIGNMENTS : assigned_to
  ORDERS ||--|| DELIVERY_ASSIGNMENTS : has_assignment

  CURRENCIES ||--o{ ORDERS : priced_in
  CURRENCIES ||--o{ REFUNDS : refunded_in
  CURRENCIES ||--o{ FX_RATES : base_or_quote

  ORDERS ||--o{ ORDER_ITEMS : contains
  MENU_ITEMS ||--o{ ORDER_ITEMS : referenced_by

  ORDERS ||--o{ ORDER_STATUS_EVENTS : has_status_history
  ORDERS ||--o{ REFUNDS : has_refunds
  ORDERS ||--|| RATINGS : has_rating

  CUSTOMERS {
    BIGINT customer_id PK
    TEXT full_name
    TEXT email
    TEXT phone
    TIMESTAMPTZ created_at
  }

  CUSTOMER_ADDRESSES {
    BIGINT address_id PK
    BIGINT customer_id FK
    TEXT label
    TEXT line_1
    TEXT line_2
    TEXT city
    TEXT state
    TEXT country
    TEXT postal_code
    DECIMAL latitude
    DECIMAL longitude
    BOOLEAN is_default
    TIMESTAMPTZ created_at
  }

  RESTAURANT_BRANDS {
    BIGINT brand_id PK
    TEXT brand_name
    BOOLEAN is_active
    TIMESTAMPTZ created_at
  }

  RESTAURANT_OUTLETS {
    BIGINT restaurant_id PK
    BIGINT brand_id FK
    TEXT outlet_name
    TEXT city
    TEXT delivery_zone
    TEXT address_line1
    TEXT postal_code
    BOOLEAN is_active
    TIMESTAMPTZ created_at
  }

  MENU_ITEMS {
    BIGINT menu_item_id PK
    BIGINT restaurant_id FK
    TEXT item_name
    TEXT category
    DECIMAL price
    BOOLEAN is_available
    TIMESTAMPTZ created_at
  }

  COURIERS {
    BIGINT courier_id PK
    TEXT city
    TEXT vehicle
    BOOLEAN is_active
    TIMESTAMPTZ created_at
  }

  CURRENCIES {
    BIGINT currency_id PK
    CHAR currency_code
    TEXT currency_name
    BOOLEAN is_active
    TIMESTAMPTZ created_at
  }

  FX_RATES {
    BIGINT fx_rate_id PK
    BIGINT base_currency_id FK
    BIGINT quote_currency_id FK
    TIMESTAMPTZ rate_ts
    DECIMAL rate
    TEXT source
  }

  ORDERS {
    BIGINT order_id PK
    BIGINT customer_id FK
    BIGINT delivery_address_id FK
    BIGINT restaurant_id FK
    BIGINT currency_id FK
    TIMESTAMPTZ order_placed_at
    TIMESTAMPTZ scheduled_delivery
    DECIMAL subtotal
    DECIMAL tax
    DECIMAL delivery_fee
    DECIMAL discount
    DECIMAL total_amount
    TEXT payment_method
    TEXT payment_status
  }

  ORDER_ITEMS {
    BIGINT order_item_id PK
    BIGINT order_id FK
    BIGINT menu_item_id FK
    INT quantity
    DECIMAL unit_price
    DECIMAL line_total
  }

  ORDER_STATUS_EVENTS {
    BIGINT event_id PK
    BIGINT order_id FK
    TIMESTAMPTZ event_ts
    TEXT status
    TEXT actor
    TEXT notes
  }

  DELIVERY_ASSIGNMENTS {
    BIGINT order_id PK,FK
    BIGINT courier_id FK
    TIMESTAMPTZ assigned_at
    TIMESTAMPTZ pickup_eta
    TIMESTAMPTZ dropoff_eta
  }

  REFUNDS {
    BIGINT refund_id PK
    BIGINT order_id FK
    BIGINT currency_id FK
    TIMESTAMPTZ refund_ts
    TEXT refund_reason
    DECIMAL refund_amount
  }

  RATINGS {
    BIGINT rating_id PK
    BIGINT order_id FK
    BIGINT customer_id FK
    INT restaurant_rating
    INT courier_rating
    TEXT comment
    TIMESTAMPTZ created_at
  }
```

---

## Pipeline flow

```mermaid
flowchart LR
  PG[(PostgreSQL OLTP)] --> PY[postgres_to_duckdb.py]
  PY --> PQ[(Parquet bronze: food_delivery_dbt/data/bronze)]
  PQ --> SQLV[create_raw_views.sql]
  SQLV --> RAW[(DuckDB schema raw: views over Parquet)]

  RAW --> STG[Staging models: stg_raw__*]

  STG --> INT_ITEMS[int_order_items__enriched]
  STG --> INT_ITEMS_ROLL[int_orders__items_rollup]
  STG --> INT_STATUS[int_orders__status_rollup]
  STG --> INT_DELIVERY[int_orders__delivery_enriched]
  STG --> INT_RECON[int_orders__reconciliation]

  INT_ITEMS_ROLL --> INT_ORDERS[int_orders__enriched]
  INT_STATUS --> INT_ORDERS
  INT_DELIVERY --> INT_ORDERS
  INT_RECON --> INT_ORDERS
  STG --> INT_ORDERS

  STG --> INT_REFUNDS[int_refunds__enriched]
  INT_ORDERS --> INT_REFUNDS

  STG --> SNAP_SRC_MI[int_menu_items__snapshot_source]
  STG --> SNAP_SRC_R[int_restaurants__snapshot_source]

  SNAP_SRC_MI --> SNAP_MI[snap_menu_items SCD2 snapshot]
  SNAP_SRC_R --> SNAP_R[snap_restaurants SCD2 snapshot]

  SNAP_MI --> DIM_MI[dim_menu_items (menu_item_sk SCD2)]
  SNAP_R --> DIM_R[dim_restaurants (restaurant_sk SCD2)]

  DIM_MI --> DIM_MI_BF[dim_menu_items_backfill]
  DIM_R --> DIM_R_BF[dim_restaurants_backfill]

  STG --> DIM_CUST[dim_customers]
  STG --> DIM_ADDR[dim_customer_addresses]
  STG --> DIM_CURR[dim_currencies]
  STG --> DIM_COUR[dim_couriers]
  STG --> DIM_DATE[dim_date]

  STG --> FCT_FX[fct_fx_rates]
  DIM_CURR --> FCT_FX

  INT_ITEMS --> FCT_OI[fct_order_items]
  DIM_MI_BF --> FCT_OI

  INT_ORDERS --> FCT_O[fct_orders]
  DIM_CUST --> FCT_O
  DIM_ADDR --> FCT_O
  DIM_CURR --> FCT_O
  DIM_R_BF --> FCT_O

  INT_REFUNDS --> FCT_REF[fct_refunds]
  FCT_O --> FCT_REF
  INT_ITEMS_ROLL --> FCT_REF
```

---

## Gold star schema (only)

```mermaid
erDiagram
  DIM_CUSTOMERS ||--o{ FCT_ORDERS : "customer_id"
  DIM_CUSTOMER_ADDRESSES ||--o{ FCT_ORDERS : "address_id = delivery_address_id"
  DIM_COURIERS ||--o{ FCT_ORDERS : "courier_id"
  DIM_CURRENCIES ||--o{ FCT_ORDERS : "currency_id"
  DIM_DATE ||--o{ FCT_ORDERS : "date_utc = order_date_utc"

  DIM_RESTAURANTS ||--o{ FCT_ORDERS : "restaurant_sk"
  DIM_RESTAURANTS ||--o{ FCT_REFUNDS : "restaurant_sk"

  DIM_CUSTOMERS ||--o{ FCT_REFUNDS : "customer_id"
  DIM_CURRENCIES ||--o{ FCT_REFUNDS : "currency_id"
  DIM_DATE ||--o{ FCT_REFUNDS : "date_utc = refund_date_utc"

  FCT_ORDERS ||--o{ FCT_ORDER_ITEMS : "order_id"
  DIM_MENU_ITEMS ||--o{ FCT_ORDER_ITEMS : "menu_item_sk"
  DIM_DATE ||--o{ FCT_ORDER_ITEMS : "date_utc = order_date_utc"
  DIM_CURRENCIES ||--o{ FCT_ORDER_ITEMS : "currency_id"
  DIM_CUSTOMERS ||--o{ FCT_ORDER_ITEMS : "customer_id"

  DIM_CURRENCIES ||--o{ FCT_FX_RATES : "base_currency_id"
  DIM_CURRENCIES ||--o{ FCT_FX_RATES : "quote_currency_id"
  DIM_DATE ||--o{ FCT_FX_RATES : "date_utc = rate_date_utc"
```
