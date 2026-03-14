-- Migration: Enforce result JSONB schema on agent_tasks
-- Validates and normalizes the result field on INSERT/UPDATE

CREATE OR REPLACE FUNCTION validate_task_result()
RETURNS TRIGGER AS $$
DECLARE
  result_val JSONB;
  normalized JSONB;
  artifacts JSONB;
  normalized_artifacts JSONB := '[]'::jsonb;
  artifact JSONB;
  i INT;
BEGIN
  -- Only validate if result is being set/changed
  IF NEW.result IS NULL THEN
    RETURN NEW;
  END IF;

  result_val := NEW.result;

  -- If result is a plain string, wrap it
  IF jsonb_typeof(result_val) = 'string' THEN
    NEW.result := jsonb_build_object(
      'summary', result_val,
      'artifacts', '[]'::jsonb,
      'test_results', 'null'::jsonb
    );
    RETURN NEW;
  END IF;

  -- Must be an object
  IF jsonb_typeof(result_val) != 'object' THEN
    RAISE EXCEPTION 'result must be an object or string, got %', jsonb_typeof(result_val);
  END IF;

  -- Ensure summary exists (required)
  IF NOT (result_val ? 'summary') THEN
    -- Try to extract summary from common agent patterns
    IF result_val ? 'output' THEN
      result_val := result_val || jsonb_build_object('summary', result_val->>'output');
    ELSIF result_val ? 'message' THEN
      result_val := result_val || jsonb_build_object('summary', result_val->>'message');
    ELSE
      result_val := result_val || jsonb_build_object('summary', '(no summary provided)');
    END IF;
  END IF;

  -- Ensure summary is a string
  IF jsonb_typeof(result_val->'summary') != 'string' THEN
    result_val := jsonb_set(result_val, '{summary}', to_jsonb((result_val->>'summary')::text));
  END IF;

  -- Normalize artifacts
  IF result_val ? 'artifacts' AND result_val->'artifacts' IS NOT NULL AND jsonb_typeof(result_val->'artifacts') = 'array' THEN
    artifacts := result_val->'artifacts';
    FOR i IN 0..jsonb_array_length(artifacts) - 1 LOOP
      artifact := artifacts->i;
      IF jsonb_typeof(artifact) = 'string' THEN
        -- Plain string URL → normalize to {url, type}
        normalized_artifacts := normalized_artifacts || jsonb_build_array(
          jsonb_build_object('url', artifact, 'type', 'url')
        );
      ELSIF jsonb_typeof(artifact) = 'object' THEN
        -- Validate required fields
        IF NOT (artifact ? 'url') THEN
          -- Skip artifacts without URL
          CONTINUE;
        END IF;
        -- Ensure type exists, default to 'url'
        IF NOT (artifact ? 'type') THEN
          artifact := artifact || jsonb_build_object('type', 'url');
        END IF;
        -- Validate type is one of allowed values
        IF NOT (artifact->>'type' = ANY(ARRAY['github_repo', 'pull_request', 'deployment', 'file', 'url', 'repo'])) THEN
          artifact := jsonb_set(artifact, '{type}', '"url"'::jsonb);
        END IF;
        normalized_artifacts := normalized_artifacts || jsonb_build_array(artifact);
      END IF;
      -- Skip non-string, non-object artifacts
    END LOOP;
    result_val := jsonb_set(result_val, '{artifacts}', normalized_artifacts);
  ELSE
    -- No artifacts or invalid → default to empty array
    result_val := jsonb_set(result_val, '{artifacts}', '[]'::jsonb);
  END IF;

  -- Ensure test_results is null or valid object
  IF result_val ? 'test_results' AND result_val->'test_results' IS NOT NULL THEN
    IF jsonb_typeof(result_val->'test_results') != 'object' THEN
      result_val := jsonb_set(result_val, '{test_results}', 'null'::jsonb);
    END IF;
  ELSE
    result_val := result_val || jsonb_build_object('test_results', null);
  END IF;

  NEW.result := result_val;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop existing trigger if any
DROP TRIGGER IF EXISTS validate_task_result_trigger ON agent_tasks;

-- Create trigger
CREATE TRIGGER validate_task_result_trigger
  BEFORE INSERT OR UPDATE OF result ON agent_tasks
  FOR EACH ROW
  EXECUTE FUNCTION validate_task_result();
