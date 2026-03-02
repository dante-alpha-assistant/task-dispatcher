-- Add connection fields to agent_cards for dynamic agent routing
-- Replaces hardcoded AGENTS object in dispatcher

ALTER TABLE agent_cards ADD COLUMN IF NOT EXISTS hooks_url TEXT;
ALTER TABLE agent_cards ADD COLUMN IF NOT EXISTS hooks_token TEXT;
ALTER TABLE agent_cards ADD COLUMN IF NOT EXISTS gateway_token TEXT;

-- Populate existing agents with their connection info
-- URL pattern: http://<name>.agents.svc.cluster.local:18789/hooks/agent
UPDATE agent_cards SET
  hooks_url = 'http://' || name || '.agents.svc.cluster.local:18789/hooks/agent'
WHERE hooks_url IS NULL;

COMMENT ON COLUMN agent_cards.hooks_url IS 'Full URL for the agent hooks endpoint';
COMMENT ON COLUMN agent_cards.hooks_token IS 'Bearer token for authenticating to the agent hooks endpoint';
COMMENT ON COLUMN agent_cards.gateway_token IS 'Bearer token for the agent gateway (tools/invoke, sessions_list)';
