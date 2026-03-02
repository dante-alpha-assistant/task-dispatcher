-- Add progress_log: persistent array of {message, at} entries
-- Replaces the single-value progress column with an append-only log
ALTER TABLE agent_tasks ADD COLUMN IF NOT EXISTS progress_log JSONB DEFAULT '[]'::jsonb;

-- Index for realtime subscriptions filtering on this column
CREATE INDEX IF NOT EXISTS idx_agent_tasks_progress_log ON agent_tasks USING gin(progress_log);
