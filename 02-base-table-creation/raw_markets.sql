-- Drop raw_markets table if it exists
drop table if exists public.raw_markets;

-- Create raw_markets table
create table
  public.raw_markets (
    vid bigint not null,
    block_range text not null,
    id text not null,
    chain_id numeric not null,
    market_type integer not null,
    market_id text not null,
    owner text not null,
    input_token_id text not null,
    lockup_time numeric not null,
    frontend_fee numeric not null,
    reward_style integer not null,
    incentives_asked_ids text[] not null,
    incentives_offered_ids text[] not null,
    incentives_asked_amount numeric[] not null,
    incentives_offered_amount numeric[] not null,
    quantity_asked numeric not null,
    quantity_offered numeric not null,
    quantity_asked_filled numeric not null,
    quantity_offered_filled numeric not null,
    volume_token_ids text[] not null,
    volume_amounts numeric[] not null,
    transaction_hash text not null,
    block_number numeric not null,
    block_timestamp numeric not null,
    log_index numeric not null,
    underlying_vault_address text not null,
    incentives_rates numeric[] not null,
    start_timestamps numeric[] not null,
    end_timestamps numeric[] not null,
    _gs_chain text not null,
    _gs_gid text not null,
    constraint raw_markets_pkey primary key (_gs_gid)
  ) tablespace pg_default;

-- Enable row level security on raw_markets table
alter table "raw_markets" enable row level security;

-- Grant read access to all users
drop policy if exists "Read access to raw_markets" on raw_markets;
create policy "Read access to raw_markets"
on raw_markets for select
to authenticated, anon
using ( true );
