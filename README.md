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

**Goal**
- Start from an OLTP-style source database (PostgreSQL) and produce clean, well-tested analytics tables (DuckDB + dbt) that support reporting and analysis such as order performance, delivery timelines, menu item trends, restaurant changes over time, and refunds.

**What it builds**
- A full pipeline: **Postgres (OLTP) → Parquet (bronze) → DuckDB raw views → dbt staging → dbt intermediate → dbt gold star schema**
- **Staging models** standardize data types, clean strings, and parse timestamps.
- **Intermediate models** compute order rollups, status timelines (placed/accepted/picked up/delivered), delivery enrichment, and reconciliation checks.
- **Gold models** create a star schema:
  - Dimensions (customers, restaurants, menu items, couriers, currencies, date)
  - Facts (orders, order_items, refunds, fx_rates)
- **SCD2 snapshots** track changes to restaurants and menu items over time and join the correct historical version into facts.

**Why this matters (analytics engineering focus)**
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

0) Python environment + dependencies (requirements.txt)
From repo root:

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt


1) Create PostgreSQL database + OLTP schema
From repo root:
Create database:
createdb food_delivery

Apply schema files:
psql -d food_delivery -f oltp/schema.sql
psql -d food_delivery -f oltp/new_update_schema.sql

(Optional) Verify tables:
psql -d food_delivery -c "\dt oltp.*"

2) Populate OLTP (optional sample data)
source venv/bin/activate
python oltp/populate_oltp.py

3) Export Postgres → Parquet (bronze layer)

food_delivery_dbt/data/bronze/

python postgres_to_duckdb.py

Expected Outputs:

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

4) Create DuckDB raw views over Parquet

duckdb food_delivery_dbt/warehouse/food_delivery.duckdb < food_delivery_dbt/warehouse/sql/create_raw_views.sql

Verify:

duckdb food_delivery_dbt/warehouse/food_delivery.duckdb -c "show schemas;"
duckdb food_delivery_dbt/warehouse/food_delivery.duckdb -c "show tables in raw;"
duckdb food_delivery_dbt/warehouse/food_delivery.duckdb -c "select count(*) from raw.orders;"

5) dbt setup

cd food_delivery_dbt

dbt init
dbt deps

6) Configure dbt profile (~/.dbt/profiles.yml)

create/edit:

~/.dbt/profiles.yml

Recommended portable version (uses env var for the DuckDB file path):
food_delivery_dbt:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('DUCKDB_PATH') }}"
      threads: 4

Set the env var from inside the dbt project directory:
export DUCKDB_PATH="$(pwd)/warehouse/food_delivery.duckdb"


Confirm dbt can connect:
dbt debug


7) Run snapshots + build models

This project snapshots:

snap_restaurants (restaurant attributes SCD2)
snap_menu_items (menu items attributes SCD2)

Run snapshots:

dbt snapshot

Build everything (models + tests):
dbt build
Other useful commands:

dbt run
dbt test
Data model overview
Bronze (files)
Parquet files in food_delivery_dbt/data/bronze/


Raw (DuckDB views)
raw.* views created by warehouse/sql/create_raw_views.sql


Purpose: expose Parquet as queryable sources for dbt


Staging models (stg_raw__*)
Type casting (IDs, decimals, timestamps)


String cleanup (trim, nullif, upper)


Parsed timestamps into timestamptz


Derived date fields (*_date_utc)


Intermediate models
Orders:
int_orders__items_rollup: item totals and counts at order grain


int_orders__status_rollup: per-status timestamps + SLA metrics


int_orders__delivery_enriched: delivery assignment enrichment


int_orders__reconciliation: expected totals and differences


int_orders__enriched: unified order-grain dataset


Items:
int_order_items__enriched: item grain enriched with order + menu item context


Refunds:
int_refunds__enriched: refund grain enriched with order context + checks


Gold star schema
Dimensions:
dim_customers


dim_customer_addresses


dim_couriers


dim_currencies


dim_date


dim_restaurants (SCD2 snapshot)


dim_menu_items (SCD2 snapshot)


Facts:
fct_orders (order grain, joins restaurant SCD2 at order time)


fct_order_items (order-item grain, joins menu-item SCD2 at order time)


fct_refunds (refund grain with order + rollups)


fct_fx_rates (fx rates enriched with currency codes)


Data quality checks
Schema tests (YAML)
not_null / unique on keys


relationships between facts/dims


accepted_values for enum-like fields


SCD window sanity (valid_to > valid_from, one current row per natural key)


Custom SQL tests (food_delivery_dbt/tests/)
Examples:
FX: no same-currency pair, unique pair+timestamp, positive/reasonable rates


Orders: delivered implies delivered_at exists


Orders: status timestamps are ordered correctly


Orders: total consistency (subtotal + tax + fee - discount ≈ total)


Refunds: refund timestamp after order placed


Refunds: refund does not exceed order total (tolerance)


Run:
dbt test
End-to-end quickstart
From repo root:
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

```mermaid
flowchart TB

%% =========================
%% OLTP (Postgres)
%% =========================
subgraph OLTP["OLTP (PostgreSQL)"]
  C["oltp.customers"]
  CA["oltp.customer_addresses"]
  RB["oltp.restaurant_brands"]
  RO["oltp.restaurant_outlets"]
  MI["oltp.menu_items"]
  CO["oltp.couriers"]
  O["oltp.orders"]
  OI["oltp.order_items"]
  OSE["oltp.order_status_events"]
  DA["oltp.delivery_assignments"]
  R["oltp.refunds"]
  RT["oltp.ratings"]
  CUR["oltp.currencies"]
  FX["oltp.fx_rates"]
end

RB --> RO
RO --> MI
C --> CA
C --> O
CA --> O
RO --> O
O --> OI
MI --> OI
O --> OSE
O --> DA
CO --> DA
O --> R
O --> RT
C --> RT
CUR --> O
CUR --> R
CUR --> FX
CUR --> FX

%% =========================
%% Bronze (Parquet)
%% =========================
subgraph BRONZE["Bronze (Parquet files)"]
  B_C["bronze/customers.parquet"]
  B_CA["bronze/customer_addresses.parquet"]
  B_RB["bronze/restaurant_brands.parquet"]
  B_RO["bronze/restaurant_outlets.parquet"]
  B_MI["bronze/menu_items.parquet"]
  B_CO["bronze/couriers.parquet"]
  B_CUR["bronze/currencies.parquet"]
  B_DA["bronze/delivery_assignments.parquet"]
  B_RT["bronze/ratings.parquet"]
  B_O["bronze/orders/**/*.parquet"]
  B_OI["bronze/order_items/**/*.parquet"]
  B_OSE["bronze/order_status_events/**/*.parquet"]
  B_R["bronze/refunds/**/*.parquet"]
  B_FX["bronze/fx_rates/**/*.parquet"]
end

%% (Your python export: Postgres -> DuckDB -> Parquet)
C --> B_C
CA --> B_CA
RB --> B_RB
RO --> B_RO
MI --> B_MI
CO --> B_CO
CUR --> B_CUR
DA --> B_DA
RT --> B_RT
O --> B_O
OI --> B_OI
OSE --> B_OSE
R --> B_R
FX --> B_FX

%% =========================
%% DuckDB Raw Views (sources)
%% =========================
subgraph RAW["DuckDB raw schema (views from Parquet)"]
  S_C["source: raw.customers"]
  S_CA["source: raw.customer_addresses"]
  S_RB["source: raw.restaurant_brands"]
  S_RO["source: raw.restaurant_outlets"]
  S_MI["source: raw.menu_items"]
  S_CO["source: raw.couriers"]
  S_CUR["source: raw.currencies"]
  S_DA["source: raw.delivery_assignments"]
  S_RT["source: raw.ratings"]
  S_O["source: raw.orders"]
  S_OI["source: raw.order_items"]
  S_OSE["source: raw.order_status_events"]
  S_R["source: raw.refunds"]
  S_FX["source: raw.fx_rates"]
end

B_C --> S_C
B_CA --> S_CA
B_RB --> S_RB
B_RO --> S_RO
B_MI --> S_MI
B_CO --> S_CO
B_CUR --> S_CUR
B_DA --> S_DA
B_RT --> S_RT
B_O --> S_O
B_OI --> S_OI
B_OSE --> S_OSE
B_R --> S_R
B_FX --> S_FX

%% =========================
%% Staging
%% =========================
subgraph STG["dbt Staging (clean types + standardization)"]
  STG_C["stg_raw__customers"]
  STG_CA["stg_raw__customer_addresses"]
  STG_RB["stg_raw__restaurant_brands"]
  STG_RO["stg_raw__restaurant_outlets"]
  STG_MI["stg_raw__menu_items"]
  STG_CO["stg_raw__couriers"]
  STG_CUR["stg_raw__currencies"]
  STG_DA["stg_raw__delivery_assignments"]
  STG_RT["stg_raw__ratings"]
  STG_O["stg_raw__orders"]
  STG_OI["stg_raw__order_items"]
  STG_OSE["stg_raw__order_status_events"]
  STG_R["stg_raw__refunds"]
  STG_FX["stg_raw__fx_rates"]
end

S_C --> STG_C
S_CA --> STG_CA
S_RB --> STG_RB
S_RO --> STG_RO
S_MI --> STG_MI
S_CO --> STG_CO
S_CUR --> STG_CUR
S_DA --> STG_DA
S_RT --> STG_RT
S_O --> STG_O
S_OI --> STG_OI
S_OSE --> STG_OSE
S_R --> STG_R
S_FX --> STG_FX

%% =========================
%% Intermediate
%% =========================
subgraph INT["dbt Intermediate (rollups + enrichments)"]
  INT_ITEMS["int_orders__items_rollup"]
  INT_STATUS["int_orders__status_rollup"]
  INT_DELIV["int_orders__delivery_enriched"]
  INT_RECON["int_orders__reconciliation"]
  INT_ORD["int_orders__enriched"]

  INT_OI_ENR["int_order_items__enriched"]
  INT_REF_ENR["int_refunds__enriched"]

  INT_MI_SNAP_SRC["int_menu_items__snapshot_source"]
  INT_R_SNAP_SRC["int_restaurants__snapshot_source"]
end

STG_OI --> INT_ITEMS
STG_OSE --> INT_STATUS
STG_DA --> INT_DELIV
STG_O --> INT_RECON

STG_O --> INT_ORD
INT_ITEMS --> INT_ORD
INT_STATUS --> INT_ORD
INT_DELIV --> INT_ORD
INT_RECON --> INT_ORD

STG_OI --> INT_OI_ENR
STG_O --> INT_OI_ENR
STG_MI --> INT_OI_ENR

STG_R --> INT_REF_ENR
INT_ORD --> INT_REF_ENR

STG_MI --> INT_MI_SNAP_SRC
STG_RO --> INT_R_SNAP_SRC
STG_RB --> INT_R_SNAP_SRC

%% =========================
%% Snapshots (SCD2)
%% =========================
subgraph SNAP["dbt Snapshots (SCD2)"]
  SNAP_MI["snap_menu_items<br/>(SCD2 snapshot)"]
  SNAP_R["snap_restaurants<br/>(SCD2 snapshot)"]
end

INT_MI_SNAP_SRC --> SNAP_MI
INT_R_SNAP_SRC --> SNAP_R

%% =========================
%% Gold Dimensions
%% =========================
subgraph DIMS["Gold Dims"]
  DIM_C["dim_customers"]
  DIM_CA["dim_customer_addresses"]
  DIM_CO["dim_couriers"]
  DIM_CUR["dim_currencies"]
  DIM_DATE["dim_date"]

  DIM_MI["dim_menu_items<br/>(from snapshot)"]
  DIM_MI_BF["dim_menu_items_backfill<br/>(valid_from backfill)"]

  DIM_R["dim_restaurants<br/>(from snapshot)"]
  DIM_R_BF["dim_restaurants_backfill<br/>(valid_from backfill)"]
end

STG_C --> DIM_C
STG_CA --> DIM_CA
STG_CO --> DIM_CO
STG_CUR --> DIM_CUR
STG_O --> DIM_DATE
STG_OSE --> DIM_DATE
STG_R --> DIM_DATE
STG_FX --> DIM_DATE

SNAP_MI --> DIM_MI
DIM_MI --> DIM_MI_BF

SNAP_R --> DIM_R
DIM_R --> DIM_R_BF

%% =========================
%% Gold Facts
%% =========================
subgraph FACTS["Gold Facts"]
  F_FX["fct_fx_rates"]
  F_O["fct_orders"]
  F_OI["fct_order_items"]
  F_R["fct_refunds"]
end

STG_FX --> F_FX
DIM_CUR --> F_FX

INT_ORD --> F_O
DIM_R_BF --> F_O

INT_OI_ENR --> F_OI
DIM_MI_BF --> F_OI

INT_REF_ENR --> F_R
F_O --> F_R
INT_ITEMS --> F_R
```

```mermaid
flowchart TB

%% =========================
%% Gold-only Star Schema
%% =========================
subgraph DIMS["Dimensions"]
  DIM_C["dim_customers"]
  DIM_CA["dim_customer_addresses"]
  DIM_R["dim_restaurants<br/>(SCD2)"]
  DIM_MI["dim_menu_items<br/>(SCD2)"]
  DIM_CO["dim_couriers"]
  DIM_CUR["dim_currencies"]
  DIM_DATE["dim_date"]
end

subgraph FACTS["Facts"]
  F_O["fct_orders"]
  F_OI["fct_order_items"]
  F_R["fct_refunds"]
  F_FX["fct_fx_rates"]
end

%% Orders grain = 1 row per order_id
DIM_C --> F_O
DIM_CA --> F_O
DIM_R --> F_O
DIM_CUR --> F_O
DIM_DATE --> F_O
DIM_CO --> F_O

%% Order Items grain = 1 row per order_item_id
F_O --> F_OI
DIM_MI --> F_OI
DIM_DATE --> F_OI
DIM_CUR --> F_OI

%% Refunds grain = 1 row per refund_id
F_O --> F_R
DIM_R --> F_R
DIM_DATE --> F_R
DIM_CUR --> F_R

%% FX rates grain = 1 row per (base_currency_id, quote_currency_id, rate_ts)
DIM_CUR --> F_FX
DIM_DATE --> F_FX
```




















