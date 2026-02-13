import os
import io
import csv
import random
import getpass
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg.rows import dict_row


SCHEMA = "oltp"

# Reproducibility: keep a single base seed and derive all RNGs from it.
SEED = 123

# DSN composition
# - If PG_DSN is provided, it is used as-is.
# - Otherwise, build a DSN using environment variables and default to the OS username
#   (e.g., "spasumarthi" from the local machine account).
os_user = getpass.getuser()
pg_user = os.getenv("PG_USER", os_user)
pg_password = os.getenv("PG_PASSWORD", "postgres")
pg_host = os.getenv("PG_HOST", "localhost")
pg_port = os.getenv("PG_PORT", "5432")
pg_db = os.getenv("PG_DB", "food_delivery")

dsn = os.getenv(
    "PG_DSN",
    f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}",
)

# When True, the script truncates all OLTP tables and restarts identities.
RESET_DB_BEFORE_LOAD = False

# Scale parameters
N_CUSTOMERS = 10000
N_BRANDS = 60
N_OUTLETS = 450
MENU_ITEMS_PER_OUTLET = 50
N_COURIERS = 3_000
N_ORDERS = 200000

# FX generation window (daily ticks)
FX_DAYS = 120

# COPY buffering size
CHUNK_ROWS = 50000

# Orders time window
ORDERS_DAYS = 90
SAFETY_BUFFER_HOURS = 8

# City pools used to simulate multi-region operations
AU_CITIES = [
    ("Sydney", "NSW", "Australia"),
    ("Melbourne", "VIC", "Australia"),
    ("Brisbane", "QLD", "Australia"),
    ("Perth", "WA", "Australia"),
    ("Adelaide", "SA", "Australia"),
]
IN_CITIES = [
    ("Hyderabad", "TS", "India"),
    ("Bengaluru", "KA", "India"),
    ("Mumbai", "MH", "India"),
    ("Delhi", "DL", "India"),
    ("Chennai", "TN", "India"),
]

PAYMENT_METHODS = ["CARD", "DIGITAL_WALLET", "CONTACTLESS_NFC", "CASH", "PAYPAL", "BANK_TRANSFER"]
PAYMENT_STATUSES = ["PENDING", "AUTHORIZED", "PAID", "FAILED", "REFUNDED"]
VEHICLES = ["BIKE", "SCOOTER", "CAR"]

REFUND_REASONS = ["LATE_DELIVERY", "MISSING_ITEM", "WRONG_ITEM", "QUALITY_ISSUE", "OTHER"]
CATEGORIES = ["Burgers", "Pizza", "Indian", "Chinese", "Desserts", "Beverages", "Salads", "Snacks"]


def utc_now() -> datetime:
    """
    Return the current timestamp in UTC.

    All synthetic timestamps are generated in UTC to keep:
    - Postgres data consistent
    - DuckDB partitioning logic stable
    - dbt models (order_date_utc/event_date_utc/refund_date_utc/rate_date_utc) coherent
    """
    return datetime.now(timezone.utc)


def qround(x: float, nd: int = 2) -> float:
    """
    Round a float with a tiny epsilon to stabilize binary floating-point artifacts.

    This helps avoid values like 12.0000000003 from causing check constraints to fail
    when comparing computed totals after rounding.
    """
    return float(round(x + 1e-12, nd))


def copy_rows(conn, table: str, columns: list[str], rows_iterable, schema_qualify: bool = True) -> None:
    """
    Bulk load rows into Postgres using COPY FROM STDIN (CSV).

    Implementation notes:
    - Generates CSV rows into an in-memory buffer (StringIO) and streams to Postgres.
    - Uses \\N as the NULL sentinel to match Postgres COPY defaults for NULL in CSV mode.
    - Flushes the buffer every CHUNK_ROWS rows to bound memory usage.
    """
    cols_sql = ", ".join([f'"{c}"' for c in columns])

    # The staging table (_stg_orders) is a TEMP table without schema qualification.
    if schema_qualify:
        sql = f'COPY "{SCHEMA}"."{table}" ({cols_sql}) FROM STDIN WITH (FORMAT csv, NULL \'\\N\')'
    else:
        sql = f'COPY "{table}" ({cols_sql}) FROM STDIN WITH (FORMAT csv, NULL \'\\N\')'

    with conn.cursor() as cur:
        with cur.copy(sql) as cp:
            buf = io.StringIO()
            writer = csv.writer(buf, lineterminator="\n")

            buffered = 0
            for row in rows_iterable:
                # Convert None -> \N so Postgres COPY interprets it as NULL.
                writer.writerow(["\\N" if v is None else v for v in row])
                buffered += 1

                # Periodic flush prevents large memory spikes on big datasets.
                if buffered >= CHUNK_ROWS:
                    cp.write(buf.getvalue())
                    buf.seek(0)
                    buf.truncate(0)
                    buffered = 0

            # Flush any remaining buffered rows.
            if buf.tell():
                cp.write(buf.getvalue())


def reset_tables(conn) -> None:
    """
    TRUNCATE all OLTP tables and restart identities.

    Reverse dependency order + CASCADE ensures child tables are cleared safely.
    """
    sql = f"""
    TRUNCATE TABLE
      {SCHEMA}.ratings,
      {SCHEMA}.refunds,
      {SCHEMA}.delivery_assignments,
      {SCHEMA}.order_status_events,
      {SCHEMA}.order_items,
      {SCHEMA}.orders,
      {SCHEMA}.menu_items,
      {SCHEMA}.restaurant_outlets,
      {SCHEMA}.restaurant_brands,
      {SCHEMA}.customer_addresses,
      {SCHEMA}.customers,
      {SCHEMA}.couriers,
      {SCHEMA}.fx_rates,
      {SCHEMA}.currencies
    RESTART IDENTITY CASCADE;
    """
    with conn.cursor() as cur:
        cur.execute(sql)


def gen_customers(seed: int):
    """
    Generate customer rows with dummy phone numbers.

    Phone format:
    - "DUMMY-" + 10 digits (e.g., DUMMY-4930185720)
    - ~35% NULLs
    """
    rng = random.Random(seed)
    base_ts = utc_now() - timedelta(days=365 * 2)

    for i in range(N_CUSTOMERS):
        created_at = base_ts + timedelta(minutes=rng.randint(0, 365 * 2 * 24 * 60))
        full_name = f"Customer {i+1}"
        email = f"customer{i+1}@example.com"

        if rng.random() < 0.65:
            phone = "DUMMY-" + "".join(str(rng.randint(0, 9)) for _ in range(10))
        else:
            phone = None

        yield (full_name, email, phone, created_at)


def gen_customer_addresses(seed: int, customer_id_start: int, customer_id_end: int):
    """
    Generate address rows for existing customers.

    Ensures:
    - 1–3 addresses per customer (skewed toward 1)
    - at most one default address per customer (matches partial unique index)
    - geo/country fields are consistent with a simple AU/IN simulation
    """
    rng = random.Random(seed + 1000)
    addr_base_ts = utc_now() - timedelta(days=365 * 2)

    # Dummy (non-real) city/state codes, but country remains real for currency mapping.
    AU_CITY_CODES = [f"AU_CITY_{i:03d}" for i in range(1, 51)]
    AU_STATE_CODES = [f"AU_STATE_{i:02d}" for i in range(1, 11)]

    IN_CITY_CODES = [f"IN_CITY_{i:03d}" for i in range(1, 51)]
    IN_STATE_CODES = [f"IN_STATE_{i:02d}" for i in range(1, 11)]

    LABELS = ["HOME", "WORK", "OTHER"]

    for customer_id in range(customer_id_start, customer_id_end + 1):
        r = rng.random()
        if r < 0.70:
            n_addr = 1
        elif r < 0.95:
            n_addr = 2
        else:
            n_addr = 3

        # Choose country once per customer to keep things coherent.
        is_au = rng.random() < 0.55
        country = "Australia" if is_au else "India"

        if is_au:
            city_pool = AU_CITY_CODES
            state_pool = AU_STATE_CODES
            # Dummy-but-AU-shaped postal codes
            postal_code_fn = lambda: f"{rng.randint(1000, 9999)}"
        else:
            city_pool = IN_CITY_CODES
            state_pool = IN_STATE_CODES
            # Dummy-but-IN-shaped postal codes
            postal_code_fn = lambda: f"{rng.randint(100000, 999999)}"

        default_idx = rng.randrange(n_addr)

        for j in range(n_addr):
            city = rng.choice(city_pool)
            state = rng.choice(state_pool)
            label = rng.choice(LABELS)

            # Dummy address lines (clearly not real-world)
            line_1 = f"ADDR_{customer_id:05d}_{j+1:02d}"
            line_2 = f"UNIT_{rng.randint(1, 999):03d}" if rng.random() < 0.25 else None

            postal_code = postal_code_fn()

            # Dummy coordinates (not meaningful / not tied to real geography)
            lat = qround(rng.uniform(-10.0, 10.0), 6)
            lon = qround(rng.uniform(-10.0, 10.0), 6)

            is_default = (j == default_idx)
            created_at = addr_base_ts + timedelta(minutes=rng.randint(0, 365 * 2 * 24 * 60))

            yield (
                customer_id,
                label,
                line_1,
                line_2,
                city,
                state,
                country,
                postal_code,
                lat,
                lon,
                is_default,
                created_at,
            )


def gen_brands(seed: int):
    """
    Generate restaurant brand rows.

    Returns tuples matching:
    (brand_name, is_active, created_at)
    """
    rng = random.Random(seed + 2000)
    base_ts = utc_now() - timedelta(days=365 * 3)

    for i in range(N_BRANDS):
        created_at = base_ts + timedelta(days=rng.randint(0, 365 * 3))
        name = f"Brand {i+1}"
        is_active = True if rng.random() < 0.97 else False
        yield (name, is_active, created_at)


def gen_outlets(seed: int, brand_id_start: int, brand_id_end: int):
    """
    Generate outlet rows and ensure uniqueness on (brand_id, outlet_name, city, delivery_zone).

    This aligns with the Postgres constraint:
      constraint uq_brand_outlet unique (brand_id, outlet_name, city, delivery_zone)
    """
    rng = random.Random(seed + 3000)
    base_ts = utc_now() - timedelta(days=365 * 3)

    used_keys = set()

    for _ in range(N_OUTLETS):
        brand_id = rng.randint(brand_id_start, brand_id_end)
        is_au = rng.random() < 0.55
        city, _, _country = rng.choice(AU_CITIES if is_au else IN_CITIES)
        delivery_zone = f"Z{rng.randint(1, 25)}"

        outlet_name = f"Outlet {rng.randint(1, 50_000)}"
        address_line1 = f"{rng.randint(1, 999)} Main Rd"
        postal_code = str(rng.randint(2000, 7999)) if is_au else str(rng.randint(100000, 999999))

        is_active = True if rng.random() < 0.98 else False
        created_at = base_ts + timedelta(days=rng.randint(0, 365 * 3))

        # Resolve rare collisions by regenerating the outlet_name until unique.
        key = (brand_id, outlet_name, city, delivery_zone)
        while key in used_keys:
            outlet_name = f"Outlet {rng.randint(1, 50_000)}"
            key = (brand_id, outlet_name, city, delivery_zone)
        used_keys.add(key)

        yield (brand_id, outlet_name, city, delivery_zone, address_line1, postal_code, is_active, created_at)


def gen_menu_items(seed: int, restaurant_id_start: int, restaurant_id_end: int):
    """
    Generate menu items for each outlet.

    Returns tuples matching:
    (restaurant_id, item_name, category, price, is_available, created_at)
    """
    rng = random.Random(seed + 4000)
    base_ts = utc_now() - timedelta(days=365 * 2)

    for restaurant_id in range(restaurant_id_start, restaurant_id_end + 1):
        for j in range(MENU_ITEMS_PER_OUTLET):
            item_name = f"Item {restaurant_id}-{j+1}"
            category = rng.choice(CATEGORIES)
            price = qround(rng.uniform(3.5, 32.0), 2)
            is_available = True if rng.random() < 0.95 else False
            created_at = base_ts + timedelta(days=rng.randint(0, 365 * 2))
            yield (restaurant_id, item_name, category, price, is_available, created_at)


def gen_couriers(seed: int):
    """
    Generate courier rows.

    Returns tuples matching:
    (city, vehicle, is_active, created_at)
    """
    rng = random.Random(seed + 5000)
    base_ts = utc_now() - timedelta(days=365 * 2)

    cities = [c[0] for c in AU_CITIES] + [c[0] for c in IN_CITIES]
    for _ in range(N_COURIERS):
        city = rng.choice(cities)
        vehicle = rng.choice(VEHICLES)
        is_active = True if rng.random() < 0.97 else False
        created_at = base_ts + timedelta(days=rng.randint(0, 365 * 2))
        yield (city, vehicle, is_active, created_at)


def insert_currencies_and_fx(conn, seed: int):
    """
    Ensure baseline currencies exist and generate daily FX rates.

    Behavior:
    - Inserts AUD and INR if they don't exist.
    - Generates FX_DAYS daily timestamps (00:00 UTC) and creates both directions:
      AUD->INR and INR->AUD.
    - Uses COPY to load fx_rates efficiently.
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f'SELECT currency_id, currency_code FROM "{SCHEMA}".currencies WHERE currency_code IN (%s,%s)',
            ("AUD", "INR"),
        )
        existing = {r["currency_code"]: r["currency_id"] for r in cur.fetchall()}

        if "AUD" not in existing:
            cur.execute(
                f'INSERT INTO "{SCHEMA}".currencies (currency_code, currency_name, is_active) '
                f'VALUES (%s,%s,%s) RETURNING currency_id',
                ("AUD", "Australian Dollar", True),
            )
            existing["AUD"] = cur.fetchone()["currency_id"]

        if "INR" not in existing:
            cur.execute(
                f'INSERT INTO "{SCHEMA}".currencies (currency_code, currency_name, is_active) '
                f'VALUES (%s,%s,%s) RETURNING currency_id',
                ("INR", "Indian Rupee", True),
            )
            existing["INR"] = cur.fetchone()["currency_id"]

        aud_id = existing["AUD"]
        inr_id = existing["INR"]

        # Use midnight UTC so partitioning by date is clean and predictable.
        end = utc_now().replace(hour=0, minute=0, second=0, microsecond=0)
        start = end - timedelta(days=FX_DAYS)

        rows = []
        rng = random.Random(seed)

        # Start from a plausible AUD->INR baseline and apply small daily drift.
        rate = 55.0
        for d in range(FX_DAYS):
            ts = start + timedelta(days=d)

            # Daily change is small to keep rates within a reasonable band.
            rate *= (1.0 + rng.uniform(-0.002, 0.002))
            aud_to_inr = qround(rate, 6)
            inr_to_aud = qround(1.0 / rate, 8)

            rows.append((aud_id, inr_id, ts, aud_to_inr, "SIMULATED"))
            rows.append((inr_id, aud_id, ts, inr_to_aud, "SIMULATED"))

        copy_rows(conn, "fx_rates", ["base_currency_id", "quote_currency_id", "rate_ts", "rate", "source"], rows)

        return aud_id, inr_id


def load_reference_ids(conn):
    """
    Load min/max ID ranges for key tables.

    These ranges are used to generate realistic foreign key values when creating orders.
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(f'SELECT min(customer_id) AS lo, max(customer_id) AS hi FROM "{SCHEMA}".customers')
        cust = cur.fetchone()

        cur.execute(f'SELECT min(brand_id) AS lo, max(brand_id) AS hi FROM "{SCHEMA}".restaurant_brands')
        brand = cur.fetchone()

        cur.execute(f'SELECT min(restaurant_id) AS lo, max(restaurant_id) AS hi FROM "{SCHEMA}".restaurant_outlets')
        outlet = cur.fetchone()

        cur.execute(f'SELECT min(menu_item_id) AS lo, max(menu_item_id) AS hi FROM "{SCHEMA}".menu_items')
        menu = cur.fetchone()

        cur.execute(f'SELECT min(courier_id) AS lo, max(courier_id) AS hi FROM "{SCHEMA}".couriers')
        courier = cur.fetchone()

    return cust, brand, outlet, menu, courier


def build_address_lookup(conn):
    """
    Build a mapping: customer_id -> list[(address_id, city, country)].

    This enables:
    - choosing a valid delivery_address_id for each order
    - deriving currency_id from the address country in a consistent way
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT address_id, customer_id, city, country
            FROM "{SCHEMA}".customer_addresses
            ORDER BY customer_id, address_id
            """
        )
        m = {}
        for r in cur.fetchall():
            m.setdefault(r["customer_id"], []).append((r["address_id"], r["city"], r["country"]))
        return m


def build_order_timeline_delivered(rng, placed_at: datetime, now: datetime):
    """
    Build a monotonic order lifecycle for a delivered order.

    Output includes:
    - events: ordered list of (event_ts, status, actor)
    - assigned_at / pickup_eta / dropoff_eta
    - delivered_ts

    """
    placed_ts = placed_at

    accepted_ts = placed_ts + timedelta(minutes=rng.randint(1, 6))
    prep_start_ts = accepted_ts + timedelta(minutes=rng.randint(2, 10))
    ready_ts = prep_start_ts + timedelta(minutes=rng.randint(5, 20))

    assigned_at = ready_ts + timedelta(minutes=rng.randint(1, 8))
    pickup_eta = assigned_at + timedelta(minutes=rng.randint(10, 25))
    picked_up_ts = pickup_eta + timedelta(minutes=rng.randint(-3, 3))

    dropoff_eta = pickup_eta + timedelta(minutes=rng.randint(10, 35))
    delivered_ts = dropoff_eta + timedelta(minutes=rng.randint(-3, 3))

    # Prevent generating timestamps in the future relative to "now".
    latest_allowed = now - timedelta(minutes=1)

    def clamp(ts: datetime) -> datetime:
        # Clamp any future timestamps back into an allowed window.
        return ts if ts <= latest_allowed else latest_allowed

    accepted_ts = clamp(accepted_ts)
    prep_start_ts = clamp(prep_start_ts)
    ready_ts = clamp(ready_ts)

    assigned_at = clamp(assigned_at)
    pickup_eta = clamp(pickup_eta)
    picked_up_ts = clamp(picked_up_ts)

    dropoff_eta = clamp(dropoff_eta)
    delivered_ts = clamp(delivered_ts)

    # Enforce strictly increasing timestamps to avoid failing ordering tests.
    def ensure_after(ts: datetime, prev: datetime) -> datetime:
        if ts <= prev:
            return clamp(prev + timedelta(minutes=1))
        return ts

    accepted_ts = ensure_after(accepted_ts, placed_ts)
    prep_start_ts = ensure_after(prep_start_ts, accepted_ts)
    ready_ts = ensure_after(ready_ts, prep_start_ts)

    assigned_at = ensure_after(assigned_at, ready_ts)
    pickup_eta = ensure_after(pickup_eta, assigned_at)
    picked_up_ts = ensure_after(picked_up_ts, pickup_eta)

    dropoff_eta = ensure_after(dropoff_eta, picked_up_ts)
    delivered_ts = ensure_after(delivered_ts, dropoff_eta)

    events = [
        (placed_ts, "PLACED", "CUSTOMER"),
        (accepted_ts, "ACCEPTED", "RESTAURANT"),
        (prep_start_ts, "PREP_START", "RESTAURANT"),
        (ready_ts, "READY_FOR_PICKUP", "RESTAURANT"),
        (picked_up_ts, "PICKED_UP", "COURIER"),
        (delivered_ts, "DELIVERED", "SYSTEM"),
    ]

    return {
        "events": events,
        "assigned_at": assigned_at,
        "pickup_eta": pickup_eta,
        "dropoff_eta": dropoff_eta,
        "delivered_ts": delivered_ts,
    }


def build_order_timeline_canceled(rng, placed_at: datetime, now: datetime):
    """
    Build a minimal canceled lifecycle: PLACED -> CANCELED.
    """
    placed_ts = placed_at
    canceled_ts = placed_ts + timedelta(minutes=rng.randint(2, 30))

    latest_allowed = now - timedelta(minutes=1)

    # Clamp cancel event to allowed window.
    if canceled_ts > latest_allowed:
        canceled_ts = latest_allowed

    # Ensure canceled_ts is strictly after placed_ts.
    if canceled_ts <= placed_ts:
        canceled_ts = placed_ts + timedelta(minutes=1)
        if canceled_ts > latest_allowed:
            canceled_ts = latest_allowed

    events = [
        (placed_ts, "PLACED", "CUSTOMER"),
        (canceled_ts, "CANCELED", "SYSTEM"),
    ]

    return {
        "events": events,
        "canceled_ts": canceled_ts,
    }


def populate_orders_related(
    conn,
    seed: int,
    cust_range,
    outlet_range,
    menu_range,
    courier_range,
    aud_id: int,
    inr_id: int,
):
    """
    Populate orders and dependent OLTP tables:
    - orders
    - order_items
    - order_status_events
    - delivery_assignments (subset)
    - refunds (subset)
    - ratings (subset)

    Notes:
    - Orders are generated in UTC within the last ORDERS_DAYS (with a safety buffer).
    - Totals are generated to satisfy the orders_total_consistency CHECK constraint.
    - Status event sequences are generated to satisfy downstream dbt ordering tests.
    - currency_id is derived from delivery address country for consistent reporting.
    """
    rng = random.Random(seed)
    address_map = build_address_lookup(conn)

    cust_lo, cust_hi = cust_range["lo"], cust_range["hi"]
    outlet_lo, outlet_hi = outlet_range["lo"], outlet_range["hi"]
    menu_lo, menu_hi = menu_range["lo"], menu_range["hi"]
    courier_lo, courier_hi = courier_range["lo"], courier_range["hi"]

    orders_rows = []
    order_items_rows = []
    events_rows = []
    assignments_rows = []
    refunds_rows = []
    ratings_rows = []

    now = utc_now()
    start_ts = now - timedelta(days=ORDERS_DAYS)
    end_ts = now - timedelta(hours=SAFETY_BUFFER_HOURS)

    if end_ts <= start_ts:
        raise ValueError("Invalid time window: end_ts must be > start_ts.")

    def currency_for_country(country: str) -> int:
        # AU addresses map to AUD, everything else maps to INR for this dataset.
        return aud_id if country == "Australia" else inr_id

    # A temp staging table allows bulk loading with stable explicit order_id values.
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TEMP TABLE IF NOT EXISTS _stg_orders (
              order_id bigint,
              customer_id bigint,
              delivery_address_id bigint,
              restaurant_id bigint,
              order_placed_at timestamptz,
              scheduled_delivery timestamptz,
              subtotal numeric(12,2),
              tax numeric(12,2),
              delivery_fee numeric(12,2),
              discount numeric(12,2),
              total_amount numeric(12,2),
              payment_method {SCHEMA}.payment_method,
              payment_status {SCHEMA}.payment_status,
              currency_id bigint
            ) ON COMMIT PRESERVE ROWS;
            """
        )
        cur.execute("TRUNCATE _stg_orders;")
        cur.execute(f'SELECT COALESCE(MAX(order_id), 0) AS mx FROM "{SCHEMA}".orders')
        base_order_id = cur.fetchone()["mx"]

    for i in range(N_ORDERS):
        order_id = base_order_id + i + 1

        customer_id = rng.randint(cust_lo, cust_hi)
        address_id, _addr_city, addr_country = rng.choice(address_map[customer_id])

        restaurant_id = rng.randint(outlet_lo, outlet_hi)

        placed_at = start_ts + timedelta(seconds=rng.randint(0, int((end_ts - start_ts).total_seconds())))
        scheduled_delivery = None

        # Order items drive subtotal, which drives totals and check constraints.
        n_items = rng.randint(1, 5)
        items = []
        subtotal = 0.0

        for _ in range(n_items):
            menu_item_id = rng.randint(menu_lo, menu_hi)
            qty = rng.randint(1, 3)

            # unit_price is the paid price (not necessarily current menu price).
            unit_price = qround(rng.uniform(5.0, 35.0), 2)
            line_total = qround(qty * unit_price, 2)

            subtotal += line_total
            items.append((menu_item_id, qty, unit_price, line_total))

        subtotal = qround(subtotal, 2)
        tax = qround(subtotal * rng.uniform(0.05, 0.12), 2)
        delivery_fee = qround(rng.uniform(1.0, 8.0), 2)

        # Keep discount within a safe cap so expected totals do not become negative.
        discount_cap = max(0.0, subtotal + tax + delivery_fee)
        discount = qround(min(discount_cap, rng.uniform(0.0, subtotal * 0.20)), 2)

        # This must match the CHECK constraint logic on orders_total_consistency.
        total_amount = qround(subtotal + tax + delivery_fee - discount, 2)

        payment_method = rng.choice(PAYMENT_METHODS)

        delivered = rng.random() < 0.92

        if delivered:
            payment_status = "PAID"
            timeline = build_order_timeline_delivered(rng, placed_at, now)

            courier_id = rng.randint(courier_lo, courier_hi)
            assignments_rows.append(
                (order_id, courier_id, timeline["assigned_at"], timeline["pickup_eta"], timeline["dropoff_eta"])
            )

            for (menu_item_id, qty, unit_price, line_total) in items:
                order_items_rows.append((order_id, menu_item_id, qty, unit_price, line_total))

            for (ts, status, actor) in timeline["events"]:
                events_rows.append((order_id, ts, status, actor, None))

            # Refund subset (ensures refund_ts after delivery; ensures refund_amount <= total_amount).
            if rng.random() < 0.035:
                refund_ts = timeline["delivered_ts"] + timedelta(minutes=rng.randint(5, 180))
                refund_amount = qround(total_amount * rng.uniform(0.05, 0.80), 2)
                refund_reason = rng.choice(REFUND_REASONS)
                currency_id = currency_for_country(addr_country)

                refunds_rows.append((order_id, refund_ts, refund_reason, refund_amount, currency_id))
                if rng.random() < 0.75:
                    payment_status = "REFUNDED"

            # Ratings subset
            if rng.random() < 0.55:
                rating_created = timeline["delivered_ts"] + timedelta(minutes=rng.randint(2, 240))
                restaurant_rating = rng.randint(1, 5)
                courier_rating = rng.randint(1, 5) if rng.random() < 0.85 else None
                comment = None if rng.random() < 0.75 else "Tasty and fast delivery."
                ratings_rows.append((order_id, customer_id, restaurant_rating, courier_rating, comment, rating_created))

        else:
            # Canceled lifecycle; payment_status kept in non-final states.
            payment_status = rng.choice(["FAILED", "PENDING"])
            timeline = build_order_timeline_canceled(rng, placed_at, now)

            for (menu_item_id, qty, unit_price, line_total) in items:
                order_items_rows.append((order_id, menu_item_id, qty, unit_price, line_total))

            for (ts, status, actor) in timeline["events"]:
                events_rows.append((order_id, ts, status, actor, None))

        currency_id = currency_for_country(addr_country)

        orders_rows.append(
            (
                order_id,
                customer_id,
                address_id,
                restaurant_id,
                placed_at,
                scheduled_delivery,
                subtotal,
                tax,
                delivery_fee,
                discount,
                total_amount,
                payment_method,
                payment_status,
                currency_id,
            )
        )

        # Batch flush to bound memory and keep transactions manageable.
        if len(orders_rows) >= CHUNK_ROWS:
            _flush_orders_batch(conn, orders_rows, order_items_rows, events_rows, assignments_rows, refunds_rows, ratings_rows)
            orders_rows.clear()
            order_items_rows.clear()
            events_rows.clear()
            assignments_rows.clear()
            refunds_rows.clear()
            ratings_rows.clear()

    if orders_rows:
        _flush_orders_batch(conn, orders_rows, order_items_rows, events_rows, assignments_rows, refunds_rows, ratings_rows)


def _flush_orders_batch(conn, orders_rows, order_items_rows, events_rows, assignments_rows, refunds_rows, ratings_rows):
    """
    Flush a batch of orders and their dependent rows using COPY.

    Steps:
    1) COPY orders into temp _stg_orders
    2) INSERT into oltp.orders
    3) COPY child tables
    4) Align sequences with max values to avoid future collisions
    """
    copy_rows(
        conn,
        "_stg_orders",
        [
            "order_id",
            "customer_id",
            "delivery_address_id",
            "restaurant_id",
            "order_placed_at",
            "scheduled_delivery",
            "subtotal",
            "tax",
            "delivery_fee",
            "discount",
            "total_amount",
            "payment_method",
            "payment_status",
            "currency_id",
        ],
        orders_rows,
        schema_qualify=False,
    )

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO "{SCHEMA}".orders (
              order_id,
              customer_id,
              delivery_address_id,
              restaurant_id,
              order_placed_at,
              scheduled_delivery,
              subtotal,
              tax,
              delivery_fee,
              discount,
              total_amount,
              payment_method,
              payment_status,
              currency_id
            )
            SELECT
              order_id,
              customer_id,
              delivery_address_id,
              restaurant_id,
              order_placed_at,
              scheduled_delivery,
              subtotal,
              tax,
              delivery_fee,
              discount,
              total_amount,
              payment_method,
              payment_status,
              currency_id
            FROM _stg_orders
            ON CONFLICT (order_id) DO NOTHING;
            """
        )
        cur.execute("TRUNCATE _stg_orders;")

    if order_items_rows:
        copy_rows(
            conn,
            "order_items",
            ["order_id", "menu_item_id", "quantity", "unit_price", "line_total"],
            order_items_rows,
        )

    if events_rows:
        copy_rows(
            conn,
            "order_status_events",
            ["order_id", "event_ts", "status", "actor", "notes"],
            events_rows,
        )

    if assignments_rows:
        copy_rows(
            conn,
            "delivery_assignments",
            ["order_id", "courier_id", "assigned_at", "pickup_eta", "dropoff_eta"],
            assignments_rows,
        )

    if refunds_rows:
        copy_rows(
            conn,
            "refunds",
            ["order_id", "refund_ts", "refund_reason", "refund_amount", "currency_id"],
            refunds_rows,
        )

    if ratings_rows:
        copy_rows(
            conn,
            "ratings",
            ["order_id", "customer_id", "restaurant_rating", "courier_rating", "comment", "created_at"],
            ratings_rows,
        )

    # Keep sequences aligned to prevent collisions if later inserts omit explicit IDs.
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT setval(pg_get_serial_sequence('{SCHEMA}.orders','order_id'), "
            f"(SELECT COALESCE(MAX(order_id),1) FROM {SCHEMA}.orders))"
        )
        cur.execute(
            f"SELECT setval(pg_get_serial_sequence('{SCHEMA}.order_items','order_item_id'), "
            f"(SELECT COALESCE(MAX(order_item_id),1) FROM {SCHEMA}.order_items))"
        )
        cur.execute(
            f"SELECT setval(pg_get_serial_sequence('{SCHEMA}.order_status_events','event_id'), "
            f"(SELECT COALESCE(MAX(event_id),1) FROM {SCHEMA}.order_status_events))"
        )
        cur.execute(
            f"SELECT setval(pg_get_serial_sequence('{SCHEMA}.refunds','refund_id'), "
            f"(SELECT COALESCE(MAX(refund_id),1) FROM {SCHEMA}.refunds))"
        )
        cur.execute(
            f"SELECT setval(pg_get_serial_sequence('{SCHEMA}.ratings','rating_id'), "
            f"(SELECT COALESCE(MAX(rating_id),1) FROM {SCHEMA}.ratings))"
        )


def main():
    """
    Entry point.

    Loads synthetic OLTP data into Postgres using bulk COPY operations for speed.
    The resulting OLTP dataset is then used in the downstream pipeline:
    Postgres -> DuckDB Parquet bronze -> DuckDB raw views -> dbt staging/intermediate/marts.
    """
    with psycopg.connect(dsn, autocommit=False, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            # Ensure all unqualified table names resolve to the OLTP schema.
            cur.execute(f'SET search_path TO "{SCHEMA}", public;')

        if RESET_DB_BEFORE_LOAD:
            reset_tables(conn)
            conn.commit()

        # Customers
        copy_rows(conn, "customers", ["full_name", "email", "phone", "created_at"], gen_customers(seed=SEED + 1))
        conn.commit()

        # Customer id range drives address generation.
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f'SELECT min(customer_id) AS lo, max(customer_id) AS hi FROM "{SCHEMA}".customers')
            cust_rng = cur.fetchone()

        # Customer addresses
        copy_rows(
            conn,
            "customer_addresses",
            [
                "customer_id",
                "label",
                "line_1",
                "line_2",
                "city",
                "state",
                "country",
                "postal_code",
                "latitude",
                "longitude",
                "is_default",
                "created_at",
            ],
            gen_customer_addresses(seed=SEED + 2, customer_id_start=cust_rng["lo"], customer_id_end=cust_rng["hi"]),
        )
        conn.commit()

        # Brands
        copy_rows(conn, "restaurant_brands", ["brand_name", "is_active", "created_at"], gen_brands(seed=SEED + 3))
        conn.commit()

        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f'SELECT min(brand_id) AS lo, max(brand_id) AS hi FROM "{SCHEMA}".restaurant_brands')
            brand_rng = cur.fetchone()

        # Outlets
        copy_rows(
            conn,
            "restaurant_outlets",
            ["brand_id", "outlet_name", "city", "delivery_zone", "address_line1", "postal_code", "is_active", "created_at"],
            gen_outlets(seed=SEED + 4, brand_id_start=brand_rng["lo"], brand_id_end=brand_rng["hi"]),
        )
        conn.commit()

        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f'SELECT min(restaurant_id) AS lo, max(restaurant_id) AS hi FROM "{SCHEMA}".restaurant_outlets')
            outlet_rng = cur.fetchone()

        # Menu items
        copy_rows(
            conn,
            "menu_items",
            ["restaurant_id", "item_name", "category", "price", "is_available", "created_at"],
            gen_menu_items(seed=SEED + 5, restaurant_id_start=outlet_rng["lo"], restaurant_id_end=outlet_rng["hi"]),
        )
        conn.commit()

        # Couriers
        copy_rows(conn, "couriers", ["city", "vehicle", "is_active", "created_at"], gen_couriers(seed=SEED + 6))
        conn.commit()

        # Currencies + FX rates
        aud_id, inr_id = insert_currencies_and_fx(conn, seed=SEED + 7)
        conn.commit()

        # Load ID ranges for order generation.
        cust_rng, _brand_rng, outlet_rng, menu_rng, courier_rng = load_reference_ids(conn)

        # Orders and dependent tables
        populate_orders_related(
            conn,
            seed=SEED,
            cust_range=cust_rng,
            outlet_range=outlet_rng,
            menu_range=menu_rng,
            courier_range=courier_rng,
            aud_id=aud_id,
            inr_id=inr_id,
        )
        conn.commit()


if __name__ == "__main__":
    main()