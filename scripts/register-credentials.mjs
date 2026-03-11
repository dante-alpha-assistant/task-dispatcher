#!/usr/bin/env node
/**
 * register-credentials.mjs — Agent credential self-registration (Node.js)
 *
 * On startup, checks which known env vars are present (non-empty)
 * and PATCHes agent_cards.available_credentials in Supabase.
 *
 * SECURITY: Only checks for EXISTENCE of env vars. Never logs or
 * transmits actual values.
 *
 * Usage:
 *   AGENT_NAME=ifra-worker node register-credentials.mjs
 *   node register-credentials.mjs ifra-worker
 *
 * Can also be imported:
 *   import { registerCredentials } from './register-credentials.mjs';
 *   await registerCredentials('ifra-worker');
 */

// Known credential env vars to check
const KNOWN_CREDENTIALS = [
  'GH_TOKEN',
  'SUPABASE_SERVICE_ROLE_KEY',
  'SUPABASE_MGMT_TOKEN',
  'VERCEL_TOKEN',
  'KUBECONFIG',
  'ANTHROPIC_API_KEY',
  'OPENROUTER_API_KEY',
  'DOCKER_TOKEN',
  'NPM_TOKEN',
  'AWS_ACCESS_KEY_ID',
];

/**
 * Check which known credentials are available and register them.
 * @param {string} agentName - The agent's name (must match agent_cards.id)
 * @param {object} [options]
 * @param {string} [options.supabaseUrl] - Supabase REST URL (defaults to env)
 * @param {string} [options.supabaseKey] - Supabase service role key (defaults to env)
 * @param {string[]} [options.extraCredentials] - Additional env var names to check
 * @returns {Promise<{found: string[], registered: boolean}>}
 */
export async function registerCredentials(agentName, options = {}) {
  const supabaseUrl = options.supabaseUrl || process.env.SUPABASE_URL;
  const supabaseKey = options.supabaseKey || process.env.SUPABASE_SERVICE_ROLE_KEY;

  if (!agentName) throw new Error('[CRED-REG] agentName is required');
  if (!supabaseUrl) throw new Error('[CRED-REG] SUPABASE_URL not set');
  if (!supabaseKey) throw new Error('[CRED-REG] SUPABASE_SERVICE_ROLE_KEY not set');

  // Merge known + extra credentials
  const allCredentials = [...KNOWN_CREDENTIALS, ...(options.extraCredentials || [])];
  const unique = [...new Set(allCredentials)];

  // Check which are present (non-empty)
  const found = unique.filter((name) => {
    const val = process.env[name];
    return val !== undefined && val !== '';
  });

  console.log(`[CRED-REG] Agent: ${agentName}`);
  console.log(`[CRED-REG] Found credentials: ${found.length > 0 ? found.join(', ') : 'none'}`);

  // PATCH agent_cards
  const url = `${supabaseUrl}/rest/v1/agent_cards?id=eq.${encodeURIComponent(agentName)}`;
  const res = await fetch(url, {
    method: 'PATCH',
    headers: {
      apikey: supabaseKey,
      Authorization: `Bearer ${supabaseKey}`,
      'Content-Type': 'application/json',
      Prefer: 'return=minimal',
    },
    body: JSON.stringify({ available_credentials: found }),
  });

  if (res.ok) {
    console.log(`[CRED-REG] SUCCESS: Updated agent_cards for ${agentName} (HTTP ${res.status})`);
    console.log(`[CRED-REG] Registered ${found.length} credential(s)`);
    return { found, registered: true };
  } else {
    const body = await res.text();
    console.error(`[CRED-REG] FAILED: HTTP ${res.status} — ${body}`);
    return { found, registered: false };
  }
}

// CLI entrypoint
const isMain = process.argv[1] && (
  process.argv[1].endsWith('register-credentials.mjs') ||
  process.argv[1].endsWith('register-credentials')
);

if (isMain) {
  const agentName = process.argv[2] || process.env.AGENT_NAME;
  if (!agentName) {
    console.error('[CRED-REG] Usage: node register-credentials.mjs <agent-name>');
    console.error('[CRED-REG] Or set AGENT_NAME env var');
    process.exit(1);
  }

  registerCredentials(agentName)
    .then(({ registered }) => {
      if (!registered) process.exit(1);
    })
    .catch((err) => {
      console.error(err.message);
      process.exit(1);
    });
}
