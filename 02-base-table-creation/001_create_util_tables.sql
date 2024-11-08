-- Create chains table
create table
  public.chains (
    id text not null,
    name text not null,
    chain_id bigint not null,
    image text not null,
    native_token_id text null,
    type text null,
    is_supported boolean not null default true,
    symbol text not null default ''::text,
    constraint chains_pkey primary key (id),
    constraint chains_chain_id_key unique (chain_id)
  ) tablespace pg_default;

-- Create contracts table
create table
  public.contracts (
    id text not null,
    chain_id bigint not null,
    address text not null,
    source text not null default ''::text,
    contract_name text null,
    label text null,
    type text null,
    image text null,
    updated_at timestamp with time zone not null default now(),
    abi jsonb null,
    implementation_id text null,
    proxy_type text null,
    is_whitelisted boolean not null default false,
    constraint contracts_pkey primary key (id),
    constraint contracts_address_check check ((address ~ '^0x[a-fA-F0-9]{40}$'::text))
  ) tablespace pg_default;

-- Create market_userdata table
create table
  public.market_userdata (
    id text not null,
    name text null,
    description text null,
    last_updated timestamp with time zone not null default (now() at time zone 'utc'::text),
    is_verified boolean not null default false,
    constraint market_userdata_pkey primary key (id)
  ) tablespace pg_default;

-- Create token_index table
create table
  public.token_index (
    token_id text not null,
    chain_id bigint not null,
    contract_address text not null,
    source text not null,
    name text not null,
    symbol text not null,
    is_active boolean not null default false,
    last_updated timestamp with time zone not null,
    search_id text not null,
    decimals smallint not null default '0'::smallint,
    constraint token_index_pkey primary key (token_id),
    constraint token_index_token_address_check check ((contract_address = lower(contract_address))),
    constraint token_index_token_id_check check ((token_id = lower(token_id)))
  ) tablespace pg_default;