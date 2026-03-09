-- Migration 007: Enforce task dependencies at database level
-- Prevents forward status transitions when dependencies are unmet
-- Safety net: even if application code misses a check, DB blocks it

-- Drop existing trigger if any (idempotent)
DROP TRIGGER IF EXISTS enforce_task_dependencies ON agent_tasks;
DROP FUNCTION IF EXISTS check_task_dependencies();

CREATE OR REPLACE FUNCTION check_task_dependencies()
RETURNS TRIGGER AS $$
DECLARE
  unmet_dep RECORD;
BEGIN
  -- Only check on forward transitions to in_progress or qa_testing
  -- Skip if old status was already in_progress/qa_testing (not a new forward move)
  IF NEW.status IN ('in_progress', 'qa_testing') 
     AND (OLD.status IS NULL OR OLD.status NOT IN ('in_progress', 'qa_testing') OR OLD.status != NEW.status) THEN
    
    -- Find unmet dependencies
    SELECT at.id, at.title, at.status INTO unmet_dep
    FROM task_relationships tr
    JOIN agent_tasks at ON at.id = tr.target_task_id
    WHERE tr.source_task_id = NEW.id
      AND tr.relationship_type = 'depends_on'
      AND at.status NOT IN ('completed', 'deployed', 'deploying')
    LIMIT 1;

    IF FOUND THEN
      RAISE EXCEPTION 'Unmet dependency: task "%" (%) is still in status "%"', 
        unmet_dep.title, unmet_dep.id, unmet_dep.status;
    END IF;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER enforce_task_dependencies
  BEFORE UPDATE ON agent_tasks
  FOR EACH ROW
  EXECUTE FUNCTION check_task_dependencies();

-- Note: reopen_task() RPC moves tasks to 'todo', not forward — so it naturally bypasses this trigger.
-- If reopen_task needs to set in_progress directly in the future, it should be modified to
-- temporarily disable the trigger or use a session variable to skip the check.
