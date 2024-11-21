-- Drop chains table if it exists
drop table if exists public.chains;

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

-- Enable row level security on chains table
alter table "chains" enable row level security;

-- Grant read access to all users
drop policy if exists "Read access to chains" on chains;
create policy "Read access to chains"
on chains for select
to authenticated, anon
using ( true );

