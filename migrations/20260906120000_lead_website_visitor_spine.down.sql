-- Down: drop the lead ↔ website-visitor attribution spine.
DROP POLICY IF EXISTS lead_website_visitors_company_isolation ON lead.lead_website_visitors;
DROP TRIGGER IF EXISTS lead_website_visitors_update_audit ON lead.lead_website_visitors;
DROP TRIGGER IF EXISTS lead_website_visitors_insert_audit ON lead.lead_website_visitors;
DROP FUNCTION IF EXISTS lead.lead_website_visitors_audit_timestamp();
DROP TABLE IF EXISTS lead.lead_website_visitors;
