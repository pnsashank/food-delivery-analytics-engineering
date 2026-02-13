-- Create a dedicated schema for "raw" objects.
-- This schema acts as the landing zone in DuckDB for data read directly from the bronze Parquet files.
CREATE SCHEMA IF NOT EXISTS raw;

-- Customers raw view.
-- Purpose: Provide a stable SQL interface over the bronze customers Parquet file without copying data into DuckDB tables.
CREATE OR REPLACE VIEW raw.customers AS
SELECT * FROM read_parquet('data/bronze/customers.parquet');

-- Customer addresses raw view.
-- Purpose: Expose customer addresses exactly as landed in bronze so dbt sources can validate constraints before transformations.
CREATE OR REPLACE VIEW raw.customer_addresses AS
SELECT * FROM read_parquet('data/bronze/customer_addresses.parquet');

-- Restaurant brands raw view.
-- Purpose: Expose brand master data from bronze as a source for downstream joins and relationship tests.
CREATE OR REPLACE VIEW raw.restaurant_brands AS
SELECT * FROM read_parquet('data/bronze/restaurant_brands.parquet');

-- Restaurant outlets raw view.
-- Purpose: Expose outlet-level data (restaurants) from bronze; used to relate menu items and orders.
CREATE OR REPLACE VIEW raw.restaurant_outlets AS
SELECT * FROM read_parquet('data/bronze/restaurant_outlets.parquet');

-- Menu items raw view.
-- Purpose: Expose item master data from bronze; used for order line enrichment and SCD snapshots in dbt.
CREATE OR REPLACE VIEW raw.menu_items AS
SELECT * FROM read_parquet('data/bronze/menu_items.parquet');

-- Couriers raw view.
-- Purpose: Expose courier master data from bronze; used for delivery assignment enrichment and courier dimension.
CREATE OR REPLACE VIEW raw.couriers AS
SELECT * FROM read_parquet('data/bronze/couriers.parquet');

-- Currencies raw view.
-- Purpose: Expose currency master data from bronze; used to validate references and build currency dimensions.
CREATE OR REPLACE VIEW raw.currencies AS
SELECT * FROM read_parquet('data/bronze/currencies.parquet');

-- Delivery assignments raw view.
-- Purpose: Expose courier assignment events from bronze; used to enrich orders with last-mile operational details.
CREATE OR REPLACE VIEW raw.delivery_assignments AS
SELECT * FROM read_parquet('data/bronze/delivery_assignments.parquet');

-- Ratings raw view.
-- Purpose: Expose post-order rating feedback from bronze; used for quality KPIs and customer/restaurant analysis.
CREATE OR REPLACE VIEW raw.ratings AS
SELECT * FROM read_parquet('data/bronze/ratings.parquet');

-- Orders raw view (partitioned Parquet dataset).
-- Purpose: Read the entire partitioned orders dataset (partitioned by order_day in bronze) as a single logical relation.
-- Pattern: **/*.parquet expands to all leaf Parquet files across order_day partitions.
CREATE OR REPLACE VIEW raw.orders AS
SELECT * FROM read_parquet('data/bronze/orders/**/*.parquet');

-- Order items raw view (partitioned Parquet dataset).
-- Purpose: Read the entire partitioned order_items dataset as a single logical relation for line-level analytics.
CREATE OR REPLACE VIEW raw.order_items AS
SELECT * FROM read_parquet('data/bronze/order_items/**/*.parquet');

-- Order status events raw view (partitioned Parquet dataset).
-- Purpose: Read the partitioned order lifecycle event stream to support status rollups and operational duration metrics.
CREATE OR REPLACE VIEW raw.order_status_events AS
SELECT * FROM read_parquet('data/bronze/order_status_events/**/*.parquet');

-- Refunds raw view (partitioned Parquet dataset).
-- Purpose: Read refund records to support refund fact modeling and financial reconciliation.
CREATE OR REPLACE VIEW raw.refunds AS
SELECT * FROM read_parquet('data/bronze/refunds/**/*.parquet');

-- FX rates raw view (partitioned Parquet dataset).
-- Purpose: Read FX time series data to support currency conversion logic and FX rate facts.
CREATE OR REPLACE VIEW raw.fx_rates AS
SELECT * FROM read_parquet('data/bronze/fx_rates/**/*.parquet');
