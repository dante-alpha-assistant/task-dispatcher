-- Add top-level blocker JSONB column for direct access (avoids nested metadata queries)
-- Stores structured blocker info: type, title, description, required_inputs, suggested_action
ALTER TABLE agent_tasks ADD COLUMN IF NOT EXISTS blocker jsonb;

-- Index for querying blocked tasks by blocker type
CREATE INDEX IF NOT EXISTS idx_agent_tasks_blocker_type_col ON agent_tasks ((blocker->>'type')) WHERE blocker IS NOT NULL;
