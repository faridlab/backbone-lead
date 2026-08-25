-- Revert the UTM attribution columns: drop the trio. Attribution is derived
-- data captured at intake; dropping it loses stored attribution but breaks no
-- constraint (nullable, no index, no generated column depends on them).

ALTER TABLE lead.leads DROP COLUMN IF EXISTS utm_campaign;
ALTER TABLE lead.leads DROP COLUMN IF EXISTS utm_medium;
ALTER TABLE lead.leads DROP COLUMN IF EXISTS utm_source;
