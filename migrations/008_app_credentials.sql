-- Add credential columns to apps table for pre-flight dispatch checks
-- required_credentials: credentials an agent needs for coding tasks on this app
-- required_qa_credentials: credentials a QA agent needs to verify tasks on this app
ALTER TABLE apps ADD COLUMN IF NOT EXISTS required_credentials text[] DEFAULT '{}';
ALTER TABLE apps ADD COLUMN IF NOT EXISTS required_qa_credentials text[] DEFAULT '{}';

-- Add available_credentials to agent_cards so dispatcher can match
ALTER TABLE agent_cards ADD COLUMN IF NOT EXISTS available_credentials text[] DEFAULT '{}';

-- Seed initial credential requirements for apps that need them
UPDATE apps SET required_credentials = ARRAY['GH_TOKEN'] WHERE slug = 'queue-dashboard';
UPDATE apps SET required_credentials = ARRAY['GH_TOKEN'] WHERE slug = 'task-dispatcher';
UPDATE apps SET required_credentials = ARRAY['GH_TOKEN'] WHERE slug = 'dante-crm';
UPDATE apps SET required_credentials = ARRAY['GH_TOKEN', 'VERCEL_TOKEN'] WHERE slug = 'game-landing';
UPDATE apps SET required_credentials = ARRAY['GH_TOKEN'] WHERE slug = 'agent-skills';
UPDATE apps SET required_credentials = ARRAY['GH_TOKEN'] WHERE slug = 'gitops';

-- QA agents typically only need GH_TOKEN to read PRs
UPDATE apps SET required_qa_credentials = ARRAY['GH_TOKEN'] WHERE slug IN ('queue-dashboard', 'task-dispatcher', 'dante-crm', 'game-landing', 'agent-skills', 'gitops');
