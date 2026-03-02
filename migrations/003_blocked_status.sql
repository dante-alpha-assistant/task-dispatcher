-- Add blocked status to the check constraint
-- First drop the existing constraint, then recreate with blocked included
ALTER TABLE agent_tasks DROP CONSTRAINT IF EXISTS agent_tasks_status_check;
ALTER TABLE agent_tasks ADD CONSTRAINT agent_tasks_status_check
  CHECK (status IN ('todo', 'assigned', 'in_progress', 'done', 'qa_testing', 'completed', 'failed', 'deployed', 'deprecated', 'blocked'));

-- Add blocked_reason and human_input columns
ALTER TABLE agent_tasks ADD COLUMN IF NOT EXISTS blocked_reason TEXT;
ALTER TABLE agent_tasks ADD COLUMN IF NOT EXISTS human_input TEXT;

-- Index for quick lookup of blocked tasks
CREATE INDEX IF NOT EXISTS idx_agent_tasks_blocked ON agent_tasks(status) WHERE status = 'blocked';
