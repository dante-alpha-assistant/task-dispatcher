-- One-time: create an RPC function that applies the remap trigger
-- Run this via Supabase Dashboard SQL Editor, then call via REST API

CREATE OR REPLACE FUNCTION apply_remap_disabled_agent_trigger()
RETURNS TEXT AS $$
BEGIN
  -- Create the remap function
  CREATE OR REPLACE FUNCTION remap_disabled_agent()
  RETURNS TRIGGER AS $trig$
  DECLARE
    agent_status TEXT;
    worker_name TEXT;
    worker_status TEXT;
  BEGIN
    IF NEW.assigned_agent IS NULL THEN
      RETURN NEW;
    END IF;

    SELECT status INTO agent_status
    FROM agent_cards
    WHERE LOWER(name) = LOWER(NEW.assigned_agent)
    LIMIT 1;

    IF agent_status = 'disabled' THEN
      worker_name := LOWER(NEW.assigned_agent) || '-worker';

      SELECT status INTO worker_status
      FROM agent_cards
      WHERE LOWER(name) = worker_name
      LIMIT 1;

      IF worker_status IS NOT NULL AND worker_status != 'disabled' THEN
        NEW.assigned_agent := worker_name;
      ELSE
        NEW.assigned_agent := NULL;
        IF NEW.status = 'assigned' THEN
          NEW.status := 'todo';
        END IF;
      END IF;
    END IF;

    RETURN NEW;
  END;
  $trig$ LANGUAGE plpgsql;

  -- Drop and recreate trigger
  DROP TRIGGER IF EXISTS trg_remap_disabled_agent ON agent_tasks;

  CREATE TRIGGER trg_remap_disabled_agent
    BEFORE INSERT OR UPDATE ON agent_tasks
    FOR EACH ROW
    EXECUTE FUNCTION remap_disabled_agent();

  RETURN 'Trigger applied successfully';
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
