-- Trigger: auto-remap assigned_agent when it points to a disabled agent
-- If a disabled agent has a -worker variant that is NOT disabled, remap to it.
-- Otherwise reject the assignment (set assigned_agent to NULL).

CREATE OR REPLACE FUNCTION remap_disabled_agent()
RETURNS TRIGGER AS $$
DECLARE
  agent_status TEXT;
  worker_name TEXT;
  worker_status TEXT;
BEGIN
  -- Only act when assigned_agent is being set/changed
  IF NEW.assigned_agent IS NULL THEN
    RETURN NEW;
  END IF;

  -- Check if assigned agent is disabled
  SELECT status INTO agent_status
  FROM agent_cards
  WHERE LOWER(name) = LOWER(NEW.assigned_agent)
  LIMIT 1;

  IF agent_status = 'disabled' THEN
    -- Try the -worker variant
    worker_name := LOWER(NEW.assigned_agent) || '-worker';

    SELECT status INTO worker_status
    FROM agent_cards
    WHERE LOWER(name) = worker_name
    LIMIT 1;

    IF worker_status IS NOT NULL AND worker_status != 'disabled' THEN
      RAISE NOTICE 'Remapping disabled agent % to %', NEW.assigned_agent, worker_name;
      NEW.assigned_agent := worker_name;
    ELSE
      RAISE NOTICE 'Agent % is disabled and no valid worker found, clearing assignment', NEW.assigned_agent;
      NEW.assigned_agent := NULL;
      -- If status was being set to 'assigned', revert to 'todo'
      IF NEW.status = 'assigned' THEN
        NEW.status := 'todo';
      END IF;
    END IF;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop if exists, then create
DROP TRIGGER IF EXISTS trg_remap_disabled_agent ON agent_tasks;

CREATE TRIGGER trg_remap_disabled_agent
  BEFORE INSERT OR UPDATE ON agent_tasks
  FOR EACH ROW
  EXECUTE FUNCTION remap_disabled_agent();
