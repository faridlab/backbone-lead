-- No-op: PostgreSQL cannot drop a value from an enum type. Reverting this
-- migration leaves the 'livechat' value in place; rows referencing it must be
-- rewritten to another source first if the value is to be truly abandoned.
SELECT 1;
