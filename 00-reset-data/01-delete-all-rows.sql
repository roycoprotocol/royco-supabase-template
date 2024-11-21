DO $$ 
BEGIN
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'raw_markets') THEN
        DELETE FROM public.raw_markets;
    END IF;
    
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'raw_offers') THEN
        DELETE FROM public.raw_offers;
    END IF;
    
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'raw_activities') THEN
        DELETE FROM public.raw_activities;
    END IF;
    
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'raw_account_balances_recipe') THEN
        DELETE FROM public.raw_account_balances_recipe;
    END IF;
    
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'raw_account_balances_vault') THEN
        DELETE FROM public.raw_account_balances_vault;
    END IF;
    
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'raw_positions_recipe') THEN
        DELETE FROM public.raw_positions_recipe;
    END IF;
END $$;
