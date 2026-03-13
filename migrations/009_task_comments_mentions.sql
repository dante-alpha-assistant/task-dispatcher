-- Add mentions and reply_to columns to task_comments
-- Required for @mention routing: dispatcher parses @mentions from new comments
-- and routes hooks to the mentioned agent(s) with full task context.

ALTER TABLE task_comments ADD COLUMN IF NOT EXISTS mentions text[] DEFAULT NULL;
ALTER TABLE task_comments ADD COLUMN IF NOT EXISTS reply_to uuid DEFAULT NULL;

-- Index for finding comments that mention a specific agent
CREATE INDEX IF NOT EXISTS idx_task_comments_mentions ON task_comments USING GIN (mentions);
