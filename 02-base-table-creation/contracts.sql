-- Drop contracts table if it exists
drop table if exists public.contracts;

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

-- Enable row level security on contracts table
alter table "contracts" enable row level security;

-- Grant read access to all users
drop policy if exists "Read access to contracts" on contracts;
create policy "Read access to contracts"
on contracts for select
to authenticated, anon
using ( true );

