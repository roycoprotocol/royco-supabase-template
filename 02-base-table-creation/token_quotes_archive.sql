-- Drop token_quotes_archive table if it exists
drop table if exists public.token_quotes_archive;

-- Create token_quotes_archive table
create table
  public.token_quotes_archive (
    source text not null,
    search_id text not null,
    price double precision not null,
    total_supply double precision not null,
    volume_24h double precision not null,
    market_cap double precision not null,
    fully_diluted_market_cap double precision not null,
    last_updated timestamp with time zone not null,
    constraint token_quotes_archive_pkey primary key (source, search_id, last_updated)
  ) tablespace pg_default;

-- Enable row level security on token_quotes_archive table
alter table "token_quotes_archive" enable row level security;

-- Grant read access to all users
drop policy if exists "Read access to token_quotes_archive" on token_quotes_archive;
create policy "Read access to token_quotes_archive"
on token_quotes_archive for select
to authenticated, anon
using ( true );
