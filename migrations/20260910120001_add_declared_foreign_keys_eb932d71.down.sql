-- Down: drop the declared foreign keys for lead module
ALTER TABLE IF EXISTS lead.lead_website_visitors DROP CONSTRAINT IF EXISTS fk_lead_website_visitors_lead_id;
