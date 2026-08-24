-- Lead dedup/merge + assignment reshape.
--
-- Why: leads arrive from WhatsApp-first capture with unnormalized contact data,
-- so the same person enters twice as '+62 812-3456-789' and '0812-3456-789' and
-- the raw (company_id, whatsapp_no) index cannot join the variants. This reshape
-- adds normalized match keys as generated columns plus the merge/assignment
-- vocabulary:
--
--   owner_user_id / sales_team_id  — assignment, STORED only (logical refs, no
--                                    FK; assignment policy is the composing
--                                    service's job).
--   merged_into_lead_id/merged_at  — soft absorb marker. Dupes are never
--                                    deleted; non-null merged_into_lead_id
--                                    excludes a lead from candidate scans and
--                                    from ever being a master or dupe again.
--                                    'merged' is deliberately NOT a new
--                                    lead_status variant: WHERE
--                                    merged_into_lead_id IS NULL already
--                                    excludes absorbed leads everywhere, and an
--                                    enum churn would ripple through every
--                                    status filter for no query power.
--
-- The normalized keys (phone_key / whatsapp_key / email_key / org_key) are
-- GENERATED ALWAYS ... STORED columns computed by the DB on EVERY write path
-- (guarded verbs, generated CRUD, seeds) with no trigger maintenance. They are
-- deliberately NOT declared in schema/models/lead.model.yaml: they are DB-level
-- indexing detail, the same tier as RLS policies and audit triggers.
--
-- Everything here is additive: no data to backfill, no enum change, no drops,
-- no seed change (lead_seed.sql is fully commented out). The table-wide
-- leads_company_isolation policy is FOR ALL, so the new columns are fenced by
-- it already — no RLS statements needed here.

-- 1) Assignment + merge columns (nullable, instant).
ALTER TABLE lead.leads ADD COLUMN IF NOT EXISTS owner_user_id UUID;
ALTER TABLE lead.leads ADD COLUMN IF NOT EXISTS sales_team_id UUID;
ALTER TABLE lead.leads ADD COLUMN IF NOT EXISTS merged_into_lead_id UUID;
ALTER TABLE lead.leads ADD COLUMN IF NOT EXISTS merged_at TIMESTAMPTZ;

-- 2) Normalized match keys.
--
-- Phone canonicalization is Indonesia-first: digits only, then a leading '0'
-- (domestic trunk prefix) or a leading '8' (missing country code) becomes the
-- '62' country code; numbers already starting '62' or any other international
-- form pass through digit-only. '+62 812-3456-789', '0812-3456-789' and
-- '812-3456-789' all canonicalize to '628123456789'. Empty/absent input is
-- NULL so keyless leads never cluster. Generated-column expressions must be
-- immutable and cannot reference aliases, so the digit extraction repeats per
-- branch.
ALTER TABLE lead.leads ADD COLUMN IF NOT EXISTS phone_key TEXT
    GENERATED ALWAYS AS (
        CASE
            WHEN COALESCE(regexp_replace(phone, '[^0-9]', '', 'g'), '') = '' THEN NULL
            WHEN regexp_replace(phone, '[^0-9]', '', 'g') LIKE '62%' THEN regexp_replace(phone, '[^0-9]', '', 'g')
            WHEN regexp_replace(phone, '[^0-9]', '', 'g') LIKE '0%'  THEN '62' || substr(regexp_replace(phone, '[^0-9]', '', 'g'), 2)
            WHEN regexp_replace(phone, '[^0-9]', '', 'g') LIKE '8%'  THEN '62' || regexp_replace(phone, '[^0-9]', '', 'g')
            ELSE regexp_replace(phone, '[^0-9]', '', 'g')
        END
    ) STORED;

ALTER TABLE lead.leads ADD COLUMN IF NOT EXISTS whatsapp_key TEXT
    GENERATED ALWAYS AS (
        CASE
            WHEN COALESCE(regexp_replace(whatsapp_no, '[^0-9]', '', 'g'), '') = '' THEN NULL
            WHEN regexp_replace(whatsapp_no, '[^0-9]', '', 'g') LIKE '62%' THEN regexp_replace(whatsapp_no, '[^0-9]', '', 'g')
            WHEN regexp_replace(whatsapp_no, '[^0-9]', '', 'g') LIKE '0%'  THEN '62' || substr(regexp_replace(whatsapp_no, '[^0-9]', '', 'g'), 2)
            WHEN regexp_replace(whatsapp_no, '[^0-9]', '', 'g') LIKE '8%'  THEN '62' || regexp_replace(whatsapp_no, '[^0-9]', '', 'g')
            ELSE regexp_replace(whatsapp_no, '[^0-9]', '', 'g')
        END
    ) STORED;

-- Email key: trimmed + lowered; empty/absent input is NULL.
ALTER TABLE lead.leads ADD COLUMN IF NOT EXISTS email_key TEXT
    GENERATED ALWAYS AS (
        CASE
            WHEN COALESCE(lower(btrim(email)), '') = '' THEN NULL
            ELSE lower(btrim(email))
        END
    ) STORED;

-- Organization key: trimmed, lowered, internal whitespace collapsed
-- ('  PT   Cipta  ' → 'pt cipta'); empty/absent input is NULL.
ALTER TABLE lead.leads ADD COLUMN IF NOT EXISTS org_key TEXT
    GENERATED ALWAYS AS (
        CASE
            WHEN COALESCE(lower(regexp_replace(btrim(organization_name), '\s+', ' ', 'g')), '') = '' THEN NULL
            ELSE lower(regexp_replace(btrim(organization_name), '\s+', ' ', 'g'))
        END
    ) STORED;

-- 3) Match + assignment indexes. Match indexes are partial (keys only where
--    non-null) so NULL keys never bloat the index.
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

CREATE INDEX IF NOT EXISTS idx_leads_merged_into
    ON lead.leads (merged_into_lead_id) WHERE merged_into_lead_id IS NOT NULL;
