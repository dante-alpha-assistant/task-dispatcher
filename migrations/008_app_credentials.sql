-- Migration: Add credentials columns for app-scoped credential checking
-- Apps table gets required_credentials and required_qa_credentials
-- Agent cards get available_credentials

-- Add available_credentials to agent_cards (what credentials each agent has)
ALTER TABLE agent_cards ADD COLUMN IF NOT EXISTS available_credentials text[] DEFAULT '{}';

-- Add required_credentials to apps table (what credentials a coding agent needs)
-- These will only apply if the apps table exists (created by queue-dashboard migration)
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'apps' AND table_schema = 'public') THEN
    EXECUTE 'ALTER TABLE apps ADD COLUMN IF NOT EXISTS required_credentials text[] DEFAULT ''{}''';
    EXECUTE 'ALTER TABLE apps ADD COLUMN IF NOT EXISTS required_qa_credentials text[] DEFAULT ''{}''';
  END IF;
END $$;
