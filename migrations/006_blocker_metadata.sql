-- Blocker metadata schema documentation
-- Stored in existing agent_tasks.metadata JSONB column under the "blocker" key
-- No new columns needed — metadata column already exists
--
-- Schema: metadata.blocker = {
--   "type": "missing_credential|missing_config|ambiguous_requirement|permission_denied|external_dependency|infrastructure|human_decision",
--   "title": "Short human-readable title",
--   "description": "Detailed explanation",
--   "required_inputs": [{"key": "ENV_VAR", "label": "Label", "type": "text|password|select|url", "placeholder": "example"}],
--   "suggested_action": "What the human should do"
-- }

-- Index for querying tasks by blocker type (uses existing metadata column)
CREATE INDEX IF NOT EXISTS idx_agent_tasks_blocker_type 
  ON agent_tasks ((metadata->'blocker'->>'type')) 
  WHERE metadata->'blocker' IS NOT NULL;
