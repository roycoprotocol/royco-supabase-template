-- Drop raw_point_balances table if it exists
drop table if exists public.raw_point_balances;

-- Create raw_point_balances table
create table
  public.raw_point_balances (
    vid bigint not null,
    block_range text not null,
    id text not null,
    chain_id numeric not null,
    contract_address text not null,
    account_address text not null,
    amount numeric not null,
    _gs_chain text not null,
    _gs_gid text not null,
    constraint raw_point_balances_pkey primary key (_gs_gid)
  ) tablespace pg_default;

-- Enable row level security on raw_point_balances table
alter table "raw_point_balances" enable row level security;

-- Grant read access to all users
drop policy if exists "Read access to raw_point_balances" on raw_point_balances;
create policy "Read access to raw_point_balances"
on raw_point_balances for select
to authenticated, anon
using ( true );
