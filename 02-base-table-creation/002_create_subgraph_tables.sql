-- Create raw_markets table
create table
  public.raw_markets (
    vid bigint not null,
    block_range text not null,
    id text not null,
    chain_id numeric not null,
    market_type integer not null,
    market_id text not null,
    creator text not null,
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

-- Create raw_activities table
create table
  public.raw_activities (
    vid bigint not null,
    block_range text not null,
    id text not null,
    chain_id numeric not null,
    market_type integer not null,
    market_id text not null,
    account_address text not null,
    activity_type text not null,
    tokens_given_ids text[] not null,
    tokens_given_amount numeric[] not null,
    tokens_received_ids text[] not null,
    tokens_received_amount numeric[] not null,
    block_number numeric not null,
    block_timestamp numeric not null,
    transaction_hash text not null,
    log_index numeric not null,
    _gs_chain text not null,
    _gs_gid text not null,
    constraint raw_activities_pkey primary key (_gs_gid)
  ) tablespace pg_default;

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

-- Create raw_account_balances_vault table
create table
  public.raw_account_balances_vault (
    vid bigint not null,
    block_range text not null,
    id text not null,
    chain_id numeric not null,
    market_type integer not null,
    market_id text not null,
    account_address text not null,
    input_token_id text not null,
    quantity_received_amount numeric not null,
    incentives_given_ids text[] not null,
    incentives_given_amount numeric[] not null,
    _gs_chain text not null,
    _gs_gid text not null,
    constraint raw_account_balances_vault_pkey primary key (_gs_gid)
  ) tablespace pg_default;

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