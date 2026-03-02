-- Add status_history jsonb column to agent_tasks
-- Stores lifecycle transitions: [{status, at, agent, reason}]
ALTER TABLE agent_tasks
  ADD COLUMN IF NOT EXISTS status_history jsonb DEFAULT '[]'::jsonb;

-- DB trigger: automatically append to status_history on every status change
CREATE OR REPLACE FUNCTION append_status_history()
RETURNS TRIGGER AS $$
DECLARE
  entry jsonb;
  history jsonb;
BEGIN
  -- Only fire when status actually changes
  IF OLD.status IS DISTINCT FROM NEW.status THEN
    history := COALESCE(NEW.status_history, '[]'::jsonb);
    entry := jsonb_build_object(
      'status', NEW.status,
      'at', NOW()::text,
      'agent', COALESCE(NEW.assigned_agent, OLD.assigned_agent)
    );
    -- Add reason for blocked status
    IF NEW.status = 'blocked' AND NEW.blocked_reason IS NOT NULL THEN
      entry := entry || jsonb_build_object('reason', NEW.blocked_reason);
    END IF;
    -- Add error for failed status
    IF NEW.status = 'failed' AND NEW.error IS NOT NULL THEN
      entry := entry || jsonb_build_object('reason', LEFT(NEW.error::text, 200));
    END IF;
    -- Add qa_agent for qa_testing status
    IF NEW.status = 'qa_testing' AND NEW.qa_agent IS NOT NULL THEN
      entry := entry || jsonb_build_object('agent', NEW.qa_agent);
    END IF;
    NEW.status_history := history || jsonb_build_array(entry);
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_status_history ON agent_tasks;
CREATE TRIGGER trg_status_history
  BEFORE UPDATE ON agent_tasks
  FOR EACH ROW
  EXECUTE FUNCTION append_status_history();

-- Also fire on INSERT to capture initial 'todo' status
CREATE OR REPLACE FUNCTION init_status_history()
RETURNS TRIGGER AS $$
BEGIN
  NEW.status_history := jsonb_build_array(
    jsonb_build_object(
      'status', COALESCE(NEW.status, 'todo'),
      'at', NOW()::text,
      'agent', NEW.assigned_agent
    )
  );
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_status_history_init ON agent_tasks;
CREATE TRIGGER trg_status_history_init
  BEFORE INSERT ON agent_tasks
  FOR EACH ROW
  EXECUTE FUNCTION init_status_history();

-- Backfill existing tasks with basic history from known timestamps
UPDATE agent_tasks
SET status_history = (
  SELECT COALESCE(jsonb_agg(entry ORDER BY (entry->>'at')::timestamptz), '[]'::jsonb)
  FROM (
    SELECT jsonb_build_object('status', 'todo', 'at', t.created_at::text, 'agent', NULL) AS entry
    FROM agent_tasks t WHERE t.id = agent_tasks.id AND t.created_at IS NOT NULL
    UNION ALL
    SELECT jsonb_build_object('status', 'in_progress', 'at', t.started_at::text, 'agent', t.assigned_agent)
    FROM agent_tasks t WHERE t.id = agent_tasks.id AND t.started_at IS NOT NULL
    UNION ALL
    SELECT jsonb_build_object('status', t.status, 'at', COALESCE(t.completed_at, t.updated_at)::text, 'agent', t.assigned_agent)
    FROM agent_tasks t WHERE t.id = agent_tasks.id AND t.status NOT IN ('todo', 'in_progress') AND (t.completed_at IS NOT NULL OR t.updated_at IS NOT NULL)
  ) sub
)
WHERE status_history IS NULL OR status_history = '[]'::jsonb;

-- Index for efficient querying
CREATE INDEX IF NOT EXISTS idx_agent_tasks_status_history
  ON agent_tasks USING gin (status_history);
