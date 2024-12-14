-- Drop raw_authorized_point_issuers table if it exists
drop table if exists public.raw_authorized_point_issuers;

-- Create raw_authorized_point_issuers table
create table
  public.raw_authorized_point_issuers (
    vid bigint not null,
    block_range text not null,
    id text not null,
    chain_id numeric not null,
    contract_address text not null,
    account_address text not null,
    _gs_chain text not null,
    _gs_gid text not null,
    constraint raw_authorized_point_issuers_pkey primary key (_gs_gid)
  ) tablespace pg_default;

-- Enable row level security on raw_authorized_point_issuers table
alter table "raw_authorized_point_issuers" enable row level security;

-- Grant read access to all users
drop policy if exists "Read access to raw_authorized_point_issuers" on raw_authorized_point_issuers;
create policy "Read access to raw_authorized_point_issuers"
on raw_authorized_point_issuers for select
to authenticated, anon
using ( true );
