# Supabase Setup Instructions

## Steps

> **Note:** Execute all these SQL commands in your Supabase SQL editor and ensure that they are executed sequentially in the order mentioned below.

1. One Time Setup

Enable following extensions in your supabase project instance (Go to Database -> Extensions -> Search for the extension and click on the extension name to enable it):

- `pg_cron`
- `pg_net`
- `fuzzystrmatch`
- `wrappers`
- `http`

2. Create base tables

- Execute the file [001_create_util_tables.sql](./02-base-table-creation/001_create_util_tables.sql) in your SQL editor.
- Execute the file [002_create_subgraph_tables.sql](./02-base-table-creation/002_create_subgraph_tables.sql) in your SQL editor.

3. Insert data into base tables

- Upload the csv files in the [03-base-table-data-insertion](./03-base-table-data-insertion/) folder to your created `chains` and `token_index` tables.

4. Function Setup

- Execute the file [001_token_index.sql](./04-functions/001_token_index.sql) in your SQL editor. Don't forget to update the `<COINMARKETCAP_API_KEY>` in the script.
- Execute the file [002_get_enriched_markets.sql](./04-functions/002_get_enriched_markets.sql) in your SQL editor.
- Execute the file [003_get_enriched_offers.sql](./04-functions/003_get_enriched_offers.sql) in your SQL editor.
- Execute the file [004_get_market_offers.sql](./04-functions/004_get_market_offers.sql) in your SQL editor.
- Execute the file [005_search_contracts.sql](./04-functions/005_search_contracts.sql) in your SQL editor.
- Execute the file [006_get_contracts.sql](./04-functions/006_get_contracts.sql) in your SQL editor.
- Execute the file [007_get_contracts_permissions.sql](./04-functions/007_get_contracts_permissions.sql) in your SQL editor.

5. Materialized Views

- Execute the file [001_enriched_market_stats.sql](./05-materialized-views/001_enriched_market_stats.sql) in your SQL editor.
- Execute the file [002_distinct_token_filters.sql](./05-materialized-views/002_distinct_token_filters.sql) in your SQL editor.
- Execute the file [003_market_search_index.sql](./05-materialized-views/003_market_search_index.sql) in your SQL editor.
- Execute the file [004_search_index_contracts.sql](./05-materialized-views/004_search_index_contracts.sql) in your SQL editor.
- Execute the file [005_enriched_royco_stats.sql](./05-materialized-views/005_enriched_royco_stats.sql) in your SQL editor.

6. Enable Row Level Security (RLS)

- Execute the file [001_enable_rls_read_access.sql](./06-security/001_enable_rls_read_access.sql) in your SQL editor.
