-- Migrate existing "done" tasks to "qa_testing"
-- The "done" status is being removed from the lifecycle.
-- New flow: in_progress → qa_testing (unassigned) → qa_testing (assigned) → completed
UPDATE agent_tasks SET status = 'qa_testing' WHERE status = 'done';
