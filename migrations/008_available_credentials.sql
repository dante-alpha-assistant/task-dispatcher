-- Add available_credentials column to agent_cards
-- Stores which credential env vars each agent has available (names only, never values)
-- Example: {"GH_TOKEN", "SUPABASE_SERVICE_ROLE_KEY", "KUBECONFIG"}
ALTER TABLE agent_cards ADD COLUMN IF NOT EXISTS available_credentials TEXT[] DEFAULT '{}';

-- Add index for queries filtering by credential availability
CREATE INDEX IF NOT EXISTS idx_agent_cards_available_credentials ON agent_cards USING GIN (available_credentials);
