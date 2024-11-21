-- Drop raw_underlying_vaults table if it exists
drop table if exists public.raw_underlying_vaults;

-- Create raw_underlying_vaults table
create table
  public.raw_underlying_vaults (
    chain_id bigint not null,
    prev_total_assets text null,
    prev_total_supply text null,
    prev_block_timestamp text null,
    curr_total_assets text null,
    curr_total_supply text null,
    curr_block_timestamp text null,
    native_annual_change_ratio numeric null,
    last_updated timestamp with time zone null,
    retries numeric not null default '0'::numeric,
    underlying_vault_address text not null,
    constraint raw_underlying_vaults_pkey primary key (chain_id, underlying_vault_address)
  ) tablespace pg_default;

-- Enable row level security on raw_underlying_vaults table
alter table "raw_underlying_vaults" enable row level security;

-- Grant read access to all users
drop policy if exists "Read access to raw_underlying_vaults" on raw_underlying_vaults;
create policy "Read access to raw_underlying_vaults"
on raw_underlying_vaults for select
to authenticated, anon
using ( true );
