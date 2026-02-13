import os
from pathlib import Path
import duckdb

out_path = Path("food_delivery_dbt/data/bronze")
out_path.mkdir(parents=True, exist_ok=True)

pg_host = os.getenv("PG_HOST", "localhost")
pg_port = os.getenv("PG_PORT", "5432")
pg_db = os.getenv("PG_DB", "food_delivery")
pg_user = os.getenv("PG_USER", "")

conn = duckdb.connect()
conn.execute("INSTALL postgres;")
conn.execute("LOAD postgres;")

attach_parts = [f"host={pg_host}", f"port={pg_port}", f"dbname={pg_db}"]
if pg_user:
    attach_parts.append(f"user={pg_user}")

conn.execute(
    f"""
    ATTACH '{' '.join(attach_parts)}'
    AS pg_data
    (TYPE postgres);
    """
)

non_partitioned_tables = [
    "customers",
    "customer_addresses",
    "restaurant_brands",
    "restaurant_outlets",
    "menu_items",
    "couriers",
    "delivery_assignments",
    "ratings",
    "currencies",
]

for table_name in non_partitioned_tables:
    final_path = out_path / f"{table_name}.parquet"
    conn.execute(
        f"""
        COPY (SELECT * FROM pg_data.oltp.{table_name})
        TO '{final_path.as_posix()}'
        (FORMAT PARQUET);
        """
    )
    print(f"Wrote {final_path}")

orders_path = out_path / "orders"
orders_path.mkdir(parents=True, exist_ok=True)
conn.execute(f"""
COPY(
  SELECT
    *,
    CAST(timezone('UTC', order_placed_at) AS DATE) AS order_day
  FROM pg_data.oltp.orders
)
TO '{orders_path.as_posix()}'
(FORMAT PARQUET, PARTITION_BY (order_day));
""")

print("Wrote the partitioned orders....")

order_items_path = out_path / "order_items"
order_items_path.mkdir(parents=True, exist_ok=True)
conn.execute(f"""
COPY (
  SELECT
    oi.*,
    CAST(timezone('UTC', o.order_placed_at) AS DATE) AS order_day
  FROM pg_data.oltp.order_items oi
  JOIN pg_data.oltp.orders o
    ON o.order_id = oi.order_id
)
TO '{order_items_path.as_posix()}'
(FORMAT PARQUET, PARTITION_BY (order_day));
""")
print("Wrote partitioned order_items.....")


events_path = out_path / "order_status_events"
events_path.mkdir(parents=True, exist_ok=True)
conn.execute(f"""
COPY(
  SELECT
    *,
    CAST(timezone('UTC', event_ts) AS DATE) AS event_day
  FROM pg_data.oltp.order_status_events
)
TO '{events_path.as_posix()}'
(FORMAT PARQUET, PARTITION_BY (event_day));
""")
print("Wrote the partitioned order_status_events....")

refunds_path = out_path / "refunds"
refunds_path.mkdir(parents=True, exist_ok=True)
conn.execute(f"""
COPY(
  SELECT
    *,
    CAST(timezone('UTC', refund_ts) AS DATE) AS refund_day
  FROM pg_data.oltp.refunds
)
TO '{refunds_path.as_posix()}'
(FORMAT PARQUET, PARTITION_BY (refund_day));
""")
print("Wrote the partitioned refunds....")

fx_path = out_path / "fx_rates"
fx_path.mkdir(parents=True, exist_ok=True)
conn.execute(f"""
COPY(
  SELECT
    *,
    CAST(timezone('UTC', rate_ts) AS DATE) AS rate_day
  FROM pg_data.oltp.fx_rates
)
TO '{fx_path.as_posix()}'
(FORMAT PARQUET, PARTITION_BY (rate_day));
""")
print("Wrote the partitioned fx_rates....")

conn.close()