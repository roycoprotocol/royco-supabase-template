-- Drop token_index table if it exists
drop table if exists public.token_index;

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

-- Enable row level security on token_index table
alter table "token_index" enable row level security;

-- Grant read access to all users
drop policy if exists "Read access to token_index" on token_index;
create policy "Read access to token_index"
on token_index for select
to authenticated, anon
using ( true );
