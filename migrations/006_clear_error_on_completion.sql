-- Migration 006: Clear error field on task completion/deployment
-- Part of: Migrate errors to activity log — stop using error field for transient issues
--
-- This trigger ensures the error field is cleared when a task reaches
-- completed or deployed status, since any previous errors were transient
-- and have already been logged to task_activity_log.

CREATE OR REPLACE FUNCTION clear_error_on_completion()
RETURNS TRIGGER AS $$
BEGIN
  -- Clear error when task moves to completed or deployed
  -- These statuses mean the task succeeded, so any lingering error is stale
  IF NEW.status IN ('completed', 'deployed') AND OLD.status IS DISTINCT FROM NEW.status THEN
    NEW.error := NULL;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop if exists to make idempotent
DROP TRIGGER IF EXISTS trg_clear_error_on_completion ON agent_tasks;

CREATE TRIGGER trg_clear_error_on_completion
  BEFORE UPDATE ON agent_tasks
  FOR EACH ROW
  EXECUTE FUNCTION clear_error_on_completion();
