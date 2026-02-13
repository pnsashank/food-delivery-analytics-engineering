create table if not exists oltp.currencies (
    currency_id bigserial primary key, -- Surrogate key used in facts (orders/refunds/fx).
    currency_code char(3) not null unique,   -- ISO-style code (e.g., AUD, INR); fixed length for consistency.
    currency_name text not null,  -- Human readable name for reporting.
    is_active boolean not null default true, -- Soft-active flag for filtering (e.g., deprecated currencies).
    created_at timestamptz not null default now() -- Insert time in UTC-aware timestamp.
);

create table if not exists oltp.fx_rates (
    fx_rate_id bigserial primary key,  -- Surrogate key for the FX row.
    base_currency_id bigint not null  -- Currency being converted FROM.
      references oltp.currencies(currency_id),
    quote_currency_id bigint not null  -- Currency being converted TO.
      references oltp.currencies(currency_id),
    rate_ts timestamptz not null,  -- Effective timestamp for the rate (typically daily at 00:00 UTC).
    rate numeric(18,8) not null check (rate > 0), -- Positive numeric rate (high precision for financial calc).
    source text, -- Provenance (e.g., SIMULATED, API provider name).
    constraint fx_rates_no_same_currency   -- Prevents identity conversions like AUD->AUD.
      check (base_currency_id <> quote_currency_id),
    constraint uq_fx_pair_ts  -- At most one rate per pair per timestamp.
      unique (base_currency_id, quote_currency_id, rate_ts)
);

alter table oltp.orders 
    add column if not exists currency_id bigint; -- Currency context for order financial fields.

alter table oltp.refunds 
    add column if not exists currency_id bigint; -- Currency context for refund_amount.

do $$
begin
  -- Adds the FK only if it doesn't already exist (safe to re-run migrations).
  if not exists (
    select 1
    from pg_constraint c
    join pg_class r on r.oid = c.conrelid
    join pg_namespace n on n.oid = r.relnamespace
    where c.conname = 'fk_orders_currency'
      and n.nspname = 'oltp'
      and r.relname = 'orders'
  ) then
    alter table oltp.orders
      add constraint fk_orders_currency
      foreign key (currency_id) references oltp.currencies(currency_id); -- Enforces valid currency references.
  end if;

  -- Adds the FK only if it doesn't already exist (safe to re-run migrations).
  if not exists (
    select 1
    from pg_constraint c
    join pg_class r on r.oid = c.conrelid
    join pg_namespace n on n.oid = r.relnamespace
    where c.conname = 'fk_refunds_currency'
      and n.nspname = 'oltp'
      and r.relname = 'refunds'
  ) then
    alter table oltp.refunds
      add constraint fk_refunds_currency
      foreign key (currency_id) references oltp.currencies(currency_id); -- Ensures refunds are tagged with a real currency.
  end if;
end $$;

create index if not exists idx_orders_currency
  on oltp.orders(currency_id);  -- Speeds reporting/aggregation by currency.

create index if not exists idx_refunds_currency
  on oltp.refunds(currency_id);  -- Speeds refund analytics by currency.

create index if not exists idx_fx_rates_quote_ts
  on oltp.fx_rates(quote_currency_id, rate_ts desc);  -- Supports querying latest rates by quote currency (e.g., *->INR).
