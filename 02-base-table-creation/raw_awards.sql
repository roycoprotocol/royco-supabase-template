-- Drop raw_awards table if it exists
drop table if exists public.raw_awards;

-- Create raw_awards table
create table
  public.raw_awards (
    vid bigint not null,
    block_range text not null,
    id text not null,
    chain_id numeric not null,
    contract_address text not null,
    "from" text not null,
    "to" text not null,
    amount numeric not null,
    block_number numeric not null,
    block_timestamp numeric not null,
    transaction_hash text not null,
    log_index numeric not null,
    _gs_chain text not null,
    _gs_gid text not null,
    constraint raw_awards_pkey primary key (_gs_gid)
  ) tablespace pg_default;

-- Enable row level security on raw_awards table
alter table "raw_awards" enable row level security;

-- Grant read access to all users
drop policy if exists "Read access to raw_awards" on raw_awards;
create policy "Read access to raw_awards"
on raw_awards for select
to authenticated, anon
using ( true );
