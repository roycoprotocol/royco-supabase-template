-- Drop raw_offers table if it exists
drop table if exists public.raw_offers;

-- Create raw_offers table
create table
  public.raw_offers (
    vid bigint not null,
    block_range text not null,
    id text not null,
    chain_id numeric not null,
    market_type integer not null,
    offer_side integer not null,
    offer_id text not null,
    market_id text not null,
    creator text not null,
    funding_vault text not null,
    input_token_id text not null,
    quantity numeric not null,
    quantity_remaining numeric not null,
    expiry numeric not null,
    token_ids text[] not null,
    token_amounts numeric[] not null,
    protocol_fee_amounts numeric[] not null,
    frontend_fee_amounts numeric[] not null,
    is_cancelled boolean not null,
    transaction_hash text not null,
    block_number numeric not null,
    block_timestamp numeric not null,
    log_index numeric not null,
    _gs_chain text not null,
    _gs_gid text not null,
    is_valid boolean null default true,
    constraint raw_offers_pkey primary key (_gs_gid)
  ) tablespace pg_default;

-- Enable row level security on raw_offers table
alter table "raw_offers" enable row level security;

-- Grant read access to all users
drop policy if exists "Read access to raw_offers" on raw_offers;
create policy "Read access to raw_offers"
on raw_offers for select
to authenticated, anon
using ( true );
