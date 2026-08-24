-- Revert the dedup/merge + assignment reshape: drop the match/assignment
-- indexes first, then the generated key columns, then the assignment and
-- merge columns. Nothing else depends on them. Absorbed leads lose their
-- master pointer (the revert un-merges) — the reshape is reverted only in
-- dev/test databases.

-- Schema-qualified: Postgres places an index in its table's schema (lead), so an unqualified
-- DROP would silently resolve against the search_path and skip it.
DROP INDEX IF EXISTS lead.idx_leads_merged_into;
DROP INDEX IF EXISTS lead.idx_leads_company_team;
DROP INDEX IF EXISTS lead.idx_leads_company_owner;
DROP INDEX IF EXISTS lead.idx_leads_company_org_key;
DROP INDEX IF EXISTS lead.idx_leads_company_email_key;
DROP INDEX IF EXISTS lead.idx_leads_company_whatsapp_key;
DROP INDEX IF EXISTS lead.idx_leads_company_phone_key;

ALTER TABLE lead.leads DROP COLUMN IF EXISTS org_key;
ALTER TABLE lead.leads DROP COLUMN IF EXISTS email_key;
ALTER TABLE lead.leads DROP COLUMN IF EXISTS whatsapp_key;
ALTER TABLE lead.leads DROP COLUMN IF EXISTS phone_key;

ALTER TABLE lead.leads DROP COLUMN IF EXISTS merged_at;
ALTER TABLE lead.leads DROP COLUMN IF EXISTS merged_into_lead_id;
ALTER TABLE lead.leads DROP COLUMN IF EXISTS sales_team_id;
ALTER TABLE lead.leads DROP COLUMN IF EXISTS owner_user_id;
