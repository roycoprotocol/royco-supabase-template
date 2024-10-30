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

4. Materialized Views

- Execute all the sql files sequentially in the [05-materialized-views](./05-materialized-views/) folder in your SQL editor.

5. Function Setup

- Execute all the sql files sequentially in the [04-functions](./04-functions/) folder in your SQL editor.

6. Enable Row Level Security (RLS)

- Execute the file [001_enable_rls_read_access.sql](./06-security/001_enable_rls_read_access.sql) in your SQL editor.
