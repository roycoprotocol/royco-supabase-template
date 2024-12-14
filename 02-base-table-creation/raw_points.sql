-- Drop raw_points table if it exists
drop table if exists public.raw_points;

-- Create raw_points table
create table
  public.raw_points (
    vid bigint not null,
    block_range text not null,
    id text not null,
    chain_id numeric not null,
    contract_address text not null,
    owner text not null,
    name text not null,
    symbol text not null,
    decimals numeric not null,
    total_supply numeric not null,
    block_number numeric not null,
    block_timestamp numeric not null,
    transaction_hash text not null,
    log_index numeric not null,
    _gs_chain text not null,
    _gs_gid text not null,
    constraint raw_points_pkey primary key (_gs_gid)
  ) tablespace pg_default;

-- Enable row level security on raw_points table
alter table "raw_points" enable row level security;

-- Grant read access to all users
drop policy if exists "Read access to raw_points" on raw_points;
create policy "Read access to raw_points"
on raw_points for select
to authenticated, anon
using ( true );
