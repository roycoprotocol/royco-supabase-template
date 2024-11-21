-- Drop raw_positions_recipe table if it exists
drop table if exists public.raw_positions_recipe;

-- Create raw_positions_recipe table
create table
  public.raw_positions_recipe (
    vid bigint not null,
    block_range text not null,
    id text not null,
    chain_id numeric not null,
    weiroll_wallet text not null,
    offer_side integer not null,
    market_id text not null,
    reward_style integer not null,
    raw_offer_side integer not null,
    raw_offer_id text not null,
    account_address text not null,
    ap text not null,
    ip text not null,
    input_token_id text not null,
    quantity numeric not null,
    token_ids text[] not null,
    token_amounts numeric[] not null,
    protocol_fee_amounts numeric[] not null,
    frontend_fee_amounts numeric[] not null,
    is_claimed boolean[] not null,
    is_forfeited boolean not null,
    is_withdrawn boolean not null,
    unlock_timestamp numeric not null,
    block_number numeric not null,
    block_timestamp numeric not null,
    transaction_hash text not null,
    log_index numeric not null,
    _gs_chain text not null,
    _gs_gid text not null,
    constraint raw_positions_recipe_pkey primary key (_gs_gid)
  ) tablespace pg_default;

-- Enable row level security on raw_positions_recipe table
alter table "raw_positions_recipe" enable row level security;

-- Grant read access to all users
drop policy if exists "Read access to raw_positions_recipe" on raw_positions_recipe;
create policy "Read access to raw_positions_recipe"
on raw_positions_recipe for select
to authenticated, anon
using ( true );

