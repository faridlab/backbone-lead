-- The lead ↔ website-visitor attribution spine.
--
-- Why: a lead captured from a website form must keep a durable attribution
-- link to the website visitor row whose session submitted it. The link lives
-- on the LEAD side (the consumer side of the bridge): the website visitor id
-- and website id are plain uuids with NO DB foreign key — the promotion
-- contract for cross-module references. backbone-website keeps no lead-shaped
-- edge, and its visitor lifecycle (the 60-day partnerless GC sweep and the
-- erasure verb) stays free to reclaim visitor rows while this table keeps the
-- attribution; reads over the visitor side therefore tolerate dangling ids by
-- design (the lead row is the record of truth, never the visitor row).
--
-- PII posture: no lead contact field is ever copied onto a website-visible row
-- through this link — the lead's email/phone stay in lead.leads behind the
-- company fence, and the website visitor row remains the digest-only
-- analytics grain. There is no visitor-held email/mobile for any downstream
-- surface to compare against; per-visitor messaging must target the lead's
-- own contact columns.
--
-- lead_id carries the one real (same-schema) foreign key: links die with
-- their lead on hard delete. Everything is additive and matches
-- schema/models/lead_website_visitor.model.yaml.

CREATE TABLE IF NOT EXISTS lead.lead_website_visitors (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    company_id UUID NOT NULL,
    lead_id UUID NOT NULL REFERENCES lead.leads(id),
    website_id UUID NOT NULL,
    website_visitor_id UUID NOT NULL,
    linked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata JSONB NOT NULL DEFAULT '{"created_at":null,"updated_at":null,"deleted_at":null,"created_by":null,"updated_by":null,"deleted_by":null}'::jsonb,
    PRIMARY KEY (id),
    CONSTRAINT uq_lead_website_visitors_lead_visitor
        UNIQUE (lead_id, website_visitor_id)
);

CREATE INDEX IF NOT EXISTS idx_lead_website_visitors_website_visitor
    ON lead.lead_website_visitors (website_id, website_visitor_id);
CREATE INDEX IF NOT EXISTS idx_lead_website_visitors_company_id
    ON lead.lead_website_visitors (company_id);

-- GIN index for audit metadata JSONB queries (the family convention).
CREATE INDEX IF NOT EXISTS idx_lead_website_visitors_metadata_gin
    ON lead.lead_website_visitors USING GIN (metadata);
CREATE INDEX IF NOT EXISTS idx_lead_website_visitors_metadata_deleted_at
    ON lead.lead_website_visitors ((metadata->>'deleted_at'));

-- Triggers for automatic metadata timestamp management (the family convention).
CREATE OR REPLACE FUNCTION lead.lead_website_visitors_audit_timestamp() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        NEW.metadata = jsonb_set(NEW.metadata::jsonb, '{created_at}', to_jsonb(NOW()));
        NEW.metadata = jsonb_set(NEW.metadata::jsonb, '{updated_at}', to_jsonb(NOW()));
    ELSIF TG_OP = 'UPDATE' THEN
        NEW.metadata = jsonb_set(NEW.metadata::jsonb, '{updated_at}', to_jsonb(NOW()));
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS lead_website_visitors_insert_audit ON lead.lead_website_visitors;
CREATE TRIGGER lead_website_visitors_insert_audit BEFORE INSERT ON lead.lead_website_visitors
    FOR EACH ROW EXECUTE FUNCTION lead.lead_website_visitors_audit_timestamp();

DROP TRIGGER IF EXISTS lead_website_visitors_update_audit ON lead.lead_website_visitors;
CREATE TRIGGER lead_website_visitors_update_audit BEFORE UPDATE ON lead.lead_website_visitors
    FOR EACH ROW EXECUTE FUNCTION lead.lead_website_visitors_audit_timestamp();

-- Company RLS fence (ADR-0008) — the same posture as lead.leads: scoped per
-- request via `set_config('app.company_id', <uuid>, true)`; an unset var sees
-- zero rows. Requires the app to connect as a non-superuser role; migrations
-- and seeders run as the owner and bypass.
ALTER TABLE lead.lead_website_visitors ENABLE ROW LEVEL SECURITY;
ALTER TABLE lead.lead_website_visitors FORCE  ROW LEVEL SECURITY;
DROP POLICY IF EXISTS lead_website_visitors_company_isolation ON lead.lead_website_visitors;
CREATE POLICY lead_website_visitors_company_isolation ON lead.lead_website_visitors
    FOR ALL
    USING      (company_id = NULLIF(current_setting('app.company_id', true), '')::uuid)
    WITH CHECK (company_id = NULLIF(current_setting('app.company_id', true), '')::uuid);
