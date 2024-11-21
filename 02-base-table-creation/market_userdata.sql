-- Drop market_userdata table if it exists
drop table if exists public.market_userdata;

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

-- Enable row level security on market_userdata table
alter table "market_userdata" enable row level security;

-- Grant read access to all users
drop policy if exists "Read access to market_userdata" on market_userdata;
create policy "Read access to market_userdata"
on market_userdata for select
to authenticated, anon
using ( true );

