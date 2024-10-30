-- Util tables
alter table "chains" enable row level security;
alter table "contracts" enable row level security;
alter table "market_userdata" enable row level security;
alter table "token_index" enable row level security;
alter table "token_quotes_archive" enable row level security;
alter table "token_quotes_historical" enable row level security;

-- Read access to util tables
create policy "Read access to chains"
on chains for select
to authenticated, anon
using ( true );

create policy "Read access to contracts"
on contracts for select
to authenticated, anon
using ( true );

create policy "Read access to market_userdata"
on market_userdata for select
to authenticated, anon
using ( true );

create policy "Read access to token_index"
on token_index for select
to authenticated, anon
using ( true );

create policy "Read access to token_quotes_archive"
on token_quotes_archive for select
to authenticated, anon
using ( true );

create policy "Read access to token_quotes_historical"
on token_quotes_historical for select
to authenticated, anon
using ( true );

-- Subgraph tables
alter table "raw_markets" enable row level security;
alter table "raw_offers" enable row level security;
alter table "raw_activities" enable row level security;
alter table "raw_account_balances_recipe" enable row level security;
alter table "raw_account_balances_vault" enable row level security;
alter table "raw_positions_recipe" enable row level security;

-- Read access to subgraph tables
create policy "Read access to raw_markets"
on raw_markets for select
to authenticated, anon
using ( true );

create policy "Read access to raw_offers"
on raw_offers for select
to authenticated, anon
using ( true ); 

create policy "Read access to raw_activities"
on raw_activities for select
to authenticated, anon
using ( true ); 

create policy "Read access to raw_account_balances_recipe"
on raw_account_balances_recipe for select
to authenticated, anon
using ( true );

create policy "Read access to raw_account_balances_vault"
on raw_account_balances_vault for select
to authenticated, anon
using ( true );

create policy "Read access to raw_positions_recipe"
on raw_positions_recipe for select
to authenticated, anon
using ( true );