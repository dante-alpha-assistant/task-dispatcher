ALTER TABLE agent_tasks ADD COLUMN IF NOT EXISTS parent_task_id UUID REFERENCES agent_tasks(id);
CREATE INDEX IF NOT EXISTS idx_agent_tasks_parent ON agent_tasks(parent_task_id) WHERE parent_task_id IS NOT NULL;
