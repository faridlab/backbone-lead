-- Hand-authored (user-owned). Not regenerated.
--
-- Best-effort restore sketch for the tenancy strip. Rows dropped with company_id
-- are gone for good (the decorator's org_unit_id cannot reconstruct company_id for
-- rows written after the strip), so treat this down as a schema-shape sketch for
-- archaeology, not a usable rollback. It restores the column as nullable, re-adds
-- the company-leading indexes and the pre-strip RLS isolation policies, and removes
-- the tenant-free match-key indexes so the chain returns to its pre-strip shape.

-- ── leads ─────────────────────────────────────────────────────────────────────
DROP INDEX IF EXISTS lead.idx_leads_phone_key;
DROP INDEX IF EXISTS lead.idx_leads_whatsapp_key;
DROP INDEX IF EXISTS lead.idx_leads_email_key;
DROP INDEX IF EXISTS lead.idx_leads_org_key;
ALTER TABLE lead.leads ADD COLUMN IF NOT EXISTS company_id uuid;

CREATE INDEX IF NOT EXISTS idx_leads_company_id_status
    ON lead.leads (company_id, status);
CREATE INDEX IF NOT EXISTS idx_leads_company_id_whatsapp_no
    ON lead.leads (company_id, whatsapp_no);
CREATE INDEX IF NOT EXISTS idx_leads_company_phone_key
    ON lead.leads (company_id, phone_key) WHERE phone_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_leads_company_whatsapp_key
    ON lead.leads (company_id, whatsapp_key) WHERE whatsapp_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_leads_company_email_key
    ON lead.leads (company_id, email_key) WHERE email_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_leads_company_org_key
    ON lead.leads (company_id, org_key) WHERE org_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_leads_company_owner
    ON lead.leads (company_id, owner_user_id);
CREATE INDEX IF NOT EXISTS idx_leads_company_team
    ON lead.leads (company_id, sales_team_id);

DROP POLICY IF EXISTS leads_company_isolation ON lead.leads;
CREATE POLICY leads_company_isolation ON lead.leads
    USING (company_id = current_setting('app.company_id', true)::uuid);

-- ── lead_website_visitors ─────────────────────────────────────────────────────
ALTER TABLE lead.lead_website_visitors ADD COLUMN IF NOT EXISTS company_id uuid;

CREATE INDEX IF NOT EXISTS idx_lead_website_visitors_company_id
    ON lead.lead_website_visitors (company_id);

DROP POLICY IF EXISTS lead_website_visitors_company_isolation ON lead.lead_website_visitors;
CREATE POLICY lead_website_visitors_company_isolation ON lead.lead_website_visitors
    USING (company_id = current_setting('app.company_id', true)::uuid);
