-- Drop raw_native_yields table if it exists
drop table if exists public.raw_native_yields;

-- Create raw_native_yields table
create table
  public.raw_native_yields (
    id text not null,
    chain_id bigint not null,
    market_type smallint not null,
    market_id text not null,
    annual_change_ratio numeric null,
    updated_at timestamp with time zone not null,
    constraint raw_native_yields_pkey primary key (id)
  ) tablespace pg_default;

-- Enable row level security on raw_native_yields table
alter table "raw_native_yields" enable row level security;

-- Grant read access to all users
drop policy if exists "Read access to raw_native_yields" on raw_native_yields;
create policy "Read access to raw_native_yields"
on raw_native_yields for select
to authenticated, anon
using ( true );
