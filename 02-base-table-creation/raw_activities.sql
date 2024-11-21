-- Drop raw_activities table if it exists
drop table if exists public.raw_activities;

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

-- Enable row level security on raw_activities table
alter table "raw_activities" enable row level security;

-- Grant read access to all users
drop policy if exists "Read access to raw_activities" on raw_activities;
create policy "Read access to raw_activities"
on raw_activities for select
to authenticated, anon
using ( true );