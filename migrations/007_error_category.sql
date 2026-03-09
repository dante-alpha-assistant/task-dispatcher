-- Add error_category column to task_activity_log for error classification
ALTER TABLE task_activity_log ADD COLUMN IF NOT EXISTS error_category text;

-- Index for efficient error stats queries
CREATE INDEX IF NOT EXISTS idx_task_activity_log_error_category
  ON task_activity_log (error_category, changed_at)
  WHERE error_category IS NOT NULL;
