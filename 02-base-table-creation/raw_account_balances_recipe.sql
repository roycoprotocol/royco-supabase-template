-- Drop raw_account_balances_recipe table if it exists
drop table if exists public.raw_account_balances_recipe;

-- Create raw_account_balances_recipe table
create table
  public.raw_account_balances_recipe (
    vid bigint not null,
    block_range text not null,
    id text not null,
    chain_id numeric not null,
    market_type integer not null,
    market_id text not null,
    account_address text not null,
    input_token_id text not null,
    quantity_received_amount numeric not null,
    quantity_given_amount numeric not null,
    incentives_received_ids text[] not null,
    incentives_received_amount numeric[] not null,
    incentives_given_ids text[] not null,
    incentives_given_amount numeric[] not null,
    protocol_fee_amounts numeric[] not null,
    frontend_fee_amounts numeric[] not null,
    _gs_chain text not null,
    _gs_gid text not null,
    constraint raw_account_balances_recipe_pkey primary key (_gs_gid)
  ) tablespace pg_default;

-- Enable row level security on raw_account_balances_recipe table
alter table "raw_account_balances_recipe" enable row level security;

-- Grant read access to all users
drop policy if exists "Read access to raw_account_balances_recipe" on raw_account_balances_recipe;
create policy "Read access to raw_account_balances_recipe"
on raw_account_balances_recipe for select
to authenticated, anon
using ( true );
