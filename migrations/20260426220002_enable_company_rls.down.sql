-- Down: remove the company RLS fence for lead module

-- Reverse the company RLS fence for lead.leads
DROP POLICY IF EXISTS leads_company_isolation ON lead.leads;
ALTER TABLE lead.leads NO FORCE ROW LEVEL SECURITY;
ALTER TABLE lead.leads DISABLE ROW LEVEL SECURITY;

