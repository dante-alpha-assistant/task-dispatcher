-- Fix research-worker capabilities: add "research" so scheduler can dispatch research tasks
UPDATE agent_cards
SET capabilities = '["research", "web_search", "web_fetch", "exec"]'::jsonb
WHERE name = 'research-worker'
  AND NOT capabilities @> '["research"]';
