CREATE TABLE IF NOT EXISTS agent_cards (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  name TEXT UNIQUE NOT NULL,
  capabilities TEXT[] NOT NULL DEFAULT '{}',
  max_concurrent INTEGER NOT NULL DEFAULT 2,
  priority_affinity JSONB DEFAULT '{}',
  active BOOLEAN DEFAULT true,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Seed with current config
INSERT INTO agent_cards (name, capabilities, max_concurrent) VALUES
  ('neo', ARRAY['coding','ops','general','research'], 2),
  ('mu', ARRAY['coding','ops','general'], 2),
  ('beta', ARRAY['qa'], 1),
  ('flow', ARRAY['general','research','ops'], 2);
