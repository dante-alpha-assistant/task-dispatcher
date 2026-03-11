#!/usr/bin/env node
/**
 * Agent Credential Self-Registration
 * 
 * Checks which credential env vars are present (non-empty) and registers
 * them in the agent_cards table. Only stores variable NAMES, never values.
 * 
 * Usage:
 *   node register-credentials.js [--agent-name <name>]
 * 
 * Environment:
 *   AGENT_NAME                  - Agent name (fallback: hostname, --agent-name flag)
 *   SUPABASE_URL                - Supabase project URL
 *   SUPABASE_SERVICE_ROLE_KEY   - Supabase service role key (for write access)
 * 
 * Can be called from:
 *   - Agent init container / postStart hook
 *   - OpenClaw startup hook
 *   - Cron/heartbeat for periodic refresh
 */

// Known credential env vars to check
const KNOWN_CREDENTIALS = [
  'GH_TOKEN',
  'SUPABASE_SERVICE_ROLE_KEY',
  'SUPABASE_MGMT_TOKEN',
  'VERCEL_TOKEN',
  'KUBECONFIG',
  'OPENROUTER_API_KEY',
  'ANTHROPIC_API_KEY',
  'LANGFUSE_SECRET_KEY',
];

function getAgentName() {
  // CLI flag
  const flagIdx = process.argv.indexOf('--agent-name');
  if (flagIdx !== -1 && process.argv[flagIdx + 1]) {
    return process.argv[flagIdx + 1];
  }
  // Env var
  if (process.env.AGENT_NAME) {
    return process.env.AGENT_NAME;
  }
  // Hostname fallback (K8s pod names contain the deployment name)
  const hostname = process.env.HOSTNAME || '';
  // Strip K8s hash suffixes: e.g. "neo-worker-6cd57b9bf5-p85dq" → "neo-worker"
  const match = hostname.match(/^([a-z][\w-]*?)-[a-f0-9]{8,10}-[a-z0-9]{5}$/);
  if (match) return match[1];
  return hostname || null;
}

function detectCredentials() {
  return KNOWN_CREDENTIALS.filter(name => {
    const val = process.env[name];
    return val !== undefined && val !== null && val.trim() !== '';
  });
}

async function registerCredentials(agentName, credentials) {
  const supabaseUrl = process.env.SUPABASE_URL || 'https://lessxkxujvcmublgwdaa.supabase.co';
  const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

  if (!supabaseKey) {
    console.error('[CRED-REG] SUPABASE_SERVICE_ROLE_KEY not set — cannot register credentials');
    process.exit(1);
  }

  const url = `${supabaseUrl}/rest/v1/agent_cards?name=eq.${encodeURIComponent(agentName)}`;
  
  const res = await fetch(url, {
    method: 'PATCH',
    headers: {
      'apikey': supabaseKey,
      'Authorization': `Bearer ${supabaseKey}`,
      'Content-Type': 'application/json',
      'Prefer': 'return=minimal',
    },
    body: JSON.stringify({
      available_credentials: credentials,
    }),
  });

  if (!res.ok) {
    const body = await res.text();
    console.error(`[CRED-REG] Failed to update agent_cards for ${agentName}: ${res.status} ${body}`);
    process.exit(1);
  }

  console.log(`[CRED-REG] Registered ${credentials.length} credentials for ${agentName}: [${credentials.join(', ')}]`);
}

async function main() {
  const agentName = getAgentName();
  if (!agentName) {
    console.error('[CRED-REG] Cannot determine agent name. Set AGENT_NAME env var or use --agent-name flag.');
    process.exit(1);
  }

  const credentials = detectCredentials();
  console.log(`[CRED-REG] Agent: ${agentName}`);
  console.log(`[CRED-REG] Detected credentials: [${credentials.join(', ')}] (${credentials.length}/${KNOWN_CREDENTIALS.length})`);

  await registerCredentials(agentName, credentials);
}

main().catch(err => {
  console.error('[CRED-REG] Unexpected error:', err.message);
  process.exit(1);
});
