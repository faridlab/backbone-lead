-- Lead UTM attribution columns.
--
-- Why: a lead must carry its inbound-link attribution (utm_source / utm_medium /
-- utm_campaign) from capture all the way to the won roll-up — attribution is
-- surfaced, not reconstructed later. The trio is nullable free text capped at
-- 255 (the schema's @max(255)); no enum, no FK, no key generation: attribution
-- values are reported verbatim.
--
-- Everything here is additive, matching schema/models/lead.model.yaml. The
-- table-wide leads_company_isolation policy is FOR ALL, so the new columns are
-- fenced by it already — no RLS statements needed here. The capture INSERT, the
-- duplicate-candidate member projection, and the merge field-fill all write or
-- read these columns in the same statements as their sibling lead fields.

ALTER TABLE lead.leads ADD COLUMN IF NOT EXISTS utm_source VARCHAR(255);
ALTER TABLE lead.leads ADD COLUMN IF NOT EXISTS utm_medium VARCHAR(255);
ALTER TABLE lead.leads ADD COLUMN IF NOT EXISTS utm_campaign VARCHAR(255);
