-- Add the livechat attribution source: a lead minted from a live-chat
-- conversation by the CRM bridge. Additive enum value on lead_source
-- (ALTER TYPE ... ADD VALUE appends to the enum's sort order; existing rows
-- and every other variant are untouched). PG 12+ allows ADD VALUE inside the
-- migration's transaction as long as the new value is not used within it.
-- The type is referenced UNQUALIFIED, matching the generated create-enums
-- migration (which also creates it unqualified, i.e. on the connection's
-- default schema).
ALTER TYPE lead_source ADD VALUE IF NOT EXISTS 'livechat';
