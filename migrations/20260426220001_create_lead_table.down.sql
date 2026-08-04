-- Down: drop lead.leads table
DROP TABLE IF EXISTS lead.leads CASCADE;
DROP FUNCTION IF EXISTS lead.leads_audit_timestamp() CASCADE;
