-- Drop existing policies
DROP POLICY IF EXISTS "Allow insert for get_contracts function" ON public.contracts;
DROP POLICY IF EXISTS "Allow update for get_contracts function" ON public.contracts;
DROP POLICY IF EXISTS "Allow delete for get_contracts function" ON public.contracts;

-- @note: `WITH CHECK` is required for `insert`
CREATE POLICY "Allow insert for get_contracts function"
ON contracts
FOR INSERT
TO anon
WITH CHECK (
  current_setting('pg_temp.role') = 'rpc_get_contracts'
);

-- @note: `WITH CHECK` is required for `update`
CREATE POLICY "Allow update for get_contracts function"
ON contracts
FOR UPDATE
TO anon
USING (
  current_setting('pg_temp.role') = 'rpc_get_contracts'
)
WITH CHECK (
  current_setting('pg_temp.role') = 'rpc_get_contracts'
);