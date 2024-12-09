-- Drop token_quotes_historical table if it exists
drop table if exists public.token_quotes_historical;

-- Create token_quotes_historical table
create table
  public.token_quotes_historical (
    track_id uuid not null default gen_random_uuid (),
    source text not null,
    search_id text not null,
    price double precision not null,
    total_supply double precision not null,
    volume_24h double precision not null,
    market_cap double precision not null,
    fully_diluted_market_cap double precision not null,
    last_updated timestamp with time zone not null,
    constraint token_quotes_historical_pkey primary key (track_id)
  ) tablespace pg_default;

-- Enable row level security on token_quotes_historical table
alter table "token_quotes_historical" enable row level security;

-- Grant read access to all users
drop policy if exists "Read access to token_quotes_historical" on token_quotes_historical;
create policy "Read access to token_quotes_historical"
on token_quotes_historical for select
to authenticated, anon
using ( true );
