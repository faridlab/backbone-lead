-- Hand-authored (user-owned). Not regenerated.
--
-- Strip every company-fence artifact from the lead tables (ADR-0029): the module is
-- tenant-agnostic; org scoping is installed by the COMPOSING service's tenancy decorator,
-- never by the module. Dropped here, per table: the company-leading indexes, the
-- <table>_company_isolation RLS policy, and the company_id column itself.
--
-- Tables: leads and lead_website_visitors.
--
-- Ordering guard (the decorator must run FIRST on any database with data): the module
-- never moves tenancy data. A table is safe to strip when EITHER
--   a) it carries org_unit_id with no NULLs — the decorator backfilled it from company_id —
--      or b) it is empty (a fresh database: the earlier chain files created it empty).
-- Otherwise the strip RAISEs, naming the decorator step, rather than dropping a column
-- that still holds the only tenancy key. The file is re-runnable (every drop is IF EXISTS
-- and the tracker has no checksums), so a failed run retries cleanly after the decorator
-- lands.
--
-- RLS enable/force flags are deliberately NOT touched: the decorator owns those now.
--
-- The duplicate-candidate match keys (phone_key / whatsapp_key / email_key / org_key)
-- are MODULE DOMAIN (the dedup/merge scan), not tenancy posture: their indexes are
-- re-declared here tenant-free so the scan keeps its index paths under any deployment.
-- The pre-strip company-leading variants are dropped; the decorator may add its own
-- per-unit variants at composition time.

DO $$
DECLARE
    t text;
    has_org boolean;
    org_nulls bigint;
    total bigint;
    offenders text := '';
BEGIN
    FOREACH t IN ARRAY ARRAY['leads', 'lead_website_visitors']
    LOOP
        IF to_regclass(format('lead.%I', t)) IS NULL THEN
            CONTINUE; -- chain not fully applied on this database; nothing to strip
        END IF;

        SELECT EXISTS (
                   SELECT 1 FROM information_schema.columns
                   WHERE table_schema = 'lead' AND table_name = t AND column_name = 'org_unit_id'
               )
        INTO has_org;

        EXECUTE format('SELECT count(*) FROM lead.%I', t) INTO total;

        IF has_org THEN
            EXECUTE format(
                'SELECT count(*) FROM lead.%I WHERE org_unit_id IS NULL', t)
            INTO org_nulls;
        ELSE
            org_nulls := total; -- no org column: every row's only tenancy key is company_id
        END IF;

        IF has_org AND org_nulls = 0 THEN
            CONTINUE; -- decorator backfilled: safe
        END IF;
        IF total = 0 THEN
            CONTINUE; -- empty table (fresh database): safe
        END IF;
        offenders := offenders || format(' lead.%s (%s rows, %s rows not covered by org_unit_id);', t, total, org_nulls);
    END LOOP;

    IF offenders <> '' THEN
        RAISE EXCEPTION 'refusing to strip company_id — these tables are not yet covered by the tenancy decorator:%. Apply the composing service''s tenancy decorator (it backfills org_unit_id from company_id) and re-run; it is the only step that moves tenancy data.', offenders;
    END IF;
END $$;

-- ── leads ─────────────────────────────────────────────────────────────────────
DROP INDEX IF EXISTS lead.idx_leads_company_id_status;
DROP INDEX IF EXISTS lead.idx_leads_company_id_whatsapp_no;
DROP INDEX IF EXISTS lead.idx_leads_company_phone_key;
DROP INDEX IF EXISTS lead.idx_leads_company_whatsapp_key;
DROP INDEX IF EXISTS lead.idx_leads_company_email_key;
DROP INDEX IF EXISTS lead.idx_leads_company_org_key;
DROP INDEX IF EXISTS lead.idx_leads_company_owner;
DROP INDEX IF EXISTS lead.idx_leads_company_team;
DROP POLICY IF EXISTS leads_company_isolation ON lead.leads;
ALTER TABLE lead.leads DROP COLUMN IF EXISTS company_id;

-- ── lead_website_visitors ─────────────────────────────────────────────────────
DROP INDEX IF EXISTS lead.idx_lead_website_visitors_company_id;
DROP POLICY IF EXISTS lead_website_visitors_company_isolation ON lead.lead_website_visitors;
ALTER TABLE lead.lead_website_visitors DROP COLUMN IF EXISTS company_id;

-- ── Restore the tenant-free match-key indexes (module domain, not tenancy) ────
-- The dedup/merge candidate scan and the website capture match both seek on the
-- normalized keys; the partial predicate keeps NULL keys out of the index exactly
-- as the company-leading variants did.
CREATE INDEX IF NOT EXISTS idx_leads_phone_key
    ON lead.leads (phone_key) WHERE phone_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_leads_whatsapp_key
    ON lead.leads (whatsapp_key) WHERE whatsapp_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_leads_email_key
    ON lead.leads (email_key) WHERE email_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_leads_org_key
    ON lead.leads (org_key) WHERE org_key IS NOT NULL;
