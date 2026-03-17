import { createClient } from "@supabase/supabase-js";
import * as k8s from "@kubernetes/client-node";
import { execSync } from "child_process";
import { startMergeQueue } from "./merge-queue.js";
import { initLangfuse, traceTaskPhase, logGeneration, recordPhaseCost, flushLangfuse } from "./langfuse.js";
import { detectBlockerPattern, buildBlockerMetadata } from "./blocker-patterns.js";
import { classifyError, getErrorCategories } from "./error-classifier.js";

// K8s client setup
const kc = new k8s.KubeConfig();
kc.loadFromCluster();
const batchApi = kc.makeApiClient(k8s.BatchV1Api);
const coreApi = kc.makeApiClient(k8s.CoreV1Api);
const customApi = kc.makeApiClient(k8s.CustomObjectsApi);
const MAX_QA_WORKERS = 2;


// --- Vercel integration ---
const VERCEL_TOKEN = process.env.VERCEL_TOKEN;
// Team slug for the Vercel account (lautaro450)
const VERCEL_TEAM_SLUG = process.env.VERCEL_TEAM_SLUG || 'lautaro450';

/**
 * Build the predictable Vercel URL for a repo as a fallback.
 * Pattern: https://{repo-name}.vercel.app
 * @param {string} repoFullName - e.g. "dante-alpha-assistant/castlevania-landing"
 * @returns {string} predictable Vercel URL
 */
function getVercelFallbackUrl(repoFullName) {
  const repoName = repoFullName.split('/').pop()?.toLowerCase();
  return repoName ? `https://${repoName}.vercel.app` : null;
}

/**
 * Get the Vercel deployment URL for a GitHub repo.
 * Queries the Vercel API for the latest deployment matching the repo.
 * Falls back to the predictable https://{repo-name}.vercel.app pattern when:
 * - VERCEL_TOKEN is not configured
 * - The API call fails or finds no matching deployment
 * @param {string} repoFullName - e.g. "dante-alpha-assistant/castlevania-landing"
 * @param {string} [branch] - optional branch filter (e.g. "main")
 * @returns {Promise<string|null>} The deployment URL or null
 */
async function getVercelDeploymentUrl(repoFullName, branch) {
  if (!VERCEL_TOKEN) {
    console.warn('[VERCEL] No VERCEL_TOKEN configured — using predictable URL fallback');
    return getVercelFallbackUrl(repoFullName);
  }
  try {
    // First, find the Vercel project linked to this GitHub repo
    // Include teamId so projects scoped to the lautaro450 team are returned
    const projectsQuery = new URLSearchParams({ limit: '100', teamId: VERCEL_TEAM_SLUG });
    const projectsRes = await fetch(`https://api.vercel.com/v9/projects?${projectsQuery}`, {
      headers: { Authorization: `Bearer ${VERCEL_TOKEN}` },
    });
    if (!projectsRes.ok) {
      console.error(`[VERCEL] Failed to list projects: ${projectsRes.status} — falling back to predictable URL`);
      return getVercelFallbackUrl(repoFullName);
    }
    const { projects } = await projectsRes.json();
    
    // Find project matching the repo
    const repoName = repoFullName.split('/').pop()?.toLowerCase();
    const project = projects?.find(p => {
      const linkedRepo = p.link?.repo?.toLowerCase();
      const linkedSlug = `${p.link?.org}/${p.link?.repo}`?.toLowerCase();
      return linkedRepo === repoName || linkedSlug === repoFullName.toLowerCase() || p.name?.toLowerCase() === repoName;
    });

    if (!project) {
      console.warn(`[VERCEL] No project found for repo ${repoFullName} — falling back to predictable URL`);
      return getVercelFallbackUrl(repoFullName);
    }

    // Get latest deployment for this project (include teamId)
    const query = new URLSearchParams({ projectId: project.id, limit: '5', target: 'production', teamId: VERCEL_TEAM_SLUG });
    if (branch) query.set('meta-githubCommitRef', branch);
    
    const deploymentsRes = await fetch(`https://api.vercel.com/v6/deployments?${query}`, {
      headers: { Authorization: `Bearer ${VERCEL_TOKEN}` },
    });
    if (!deploymentsRes.ok) {
      console.error(`[VERCEL] Failed to list deployments: ${deploymentsRes.status} — falling back to predictable URL`);
      return getVercelFallbackUrl(repoFullName);
    }
    const { deployments } = await deploymentsRes.json();
    
    // Get the latest ready deployment
    const readyDeploy = deployments?.find(d => d.state === 'READY') || deployments?.[0];
    if (!readyDeploy) {
      // Try preview deployments (also include teamId)
      const previewQuery = new URLSearchParams({ projectId: project.id, limit: '5', teamId: VERCEL_TEAM_SLUG });
      const previewRes = await fetch(`https://api.vercel.com/v6/deployments?${previewQuery}`, {
        headers: { Authorization: `Bearer ${VERCEL_TOKEN}` },
      });
      if (previewRes.ok) {
        const { deployments: previewDeps } = await previewRes.json();
        const previewDeploy = previewDeps?.find(d => d.state === 'READY') || previewDeps?.[0];
        if (previewDeploy) {
          const url = previewDeploy.url ? `https://${previewDeploy.url}` : null;
          console.log(`[VERCEL] Found preview deployment for ${repoFullName}: ${url}`);
          return url;
        }
      }
      // Fall back to predictable URL rather than returning null
      console.warn(`[VERCEL] No deployments found for project ${project.name} — falling back to predictable URL`);
      return getVercelFallbackUrl(repoFullName);
    }
    
    const url = readyDeploy.url ? `https://${readyDeploy.url}` : getVercelFallbackUrl(repoFullName);
    console.log(`[VERCEL] Found deployment for ${repoFullName}: ${url}`);
    return url;
  } catch (err) {
    console.error(`[VERCEL] Error fetching deployment URL for ${repoFullName}:`, err.message);
    // Always fall back to predictable URL on error
    return getVercelFallbackUrl(repoFullName);
  }
}

// --- Config ---
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

// Agent registry: name → gateway hook URL + token
const AGENTS = {
  neo: {
    url: process.env.NEO_HOOKS_URL || `http://neo.agents.svc.cluster.local:18789/hooks/agent`,
    token: process.env.NEO_HOOKS_TOKEN,
    gatewayToken: process.env.NEO_GATEWAY_TOKEN,
  },
  mu: {
    url: process.env.MU_HOOKS_URL || `http://mu.agents.svc.cluster.local:18789/hooks/agent`,
    token: process.env.MU_HOOKS_TOKEN,
    gatewayToken: process.env.MU_GATEWAY_TOKEN,
  },
  beta: {
    url: process.env.BETA_HOOKS_URL || `http://beta.agents.svc.cluster.local:18789/hooks/agent`,
    token: process.env.BETA_HOOKS_TOKEN,
    gatewayToken: process.env.BETA_GATEWAY_TOKEN,
  },
  flow: {
    url: process.env.FLOW_HOOKS_URL || `http://flow.agents.svc.cluster.local:18789/hooks/agent`,
    token: process.env.FLOW_HOOKS_TOKEN,
    gatewayToken: process.env.FLOW_GATEWAY_TOKEN,
  },
  ifra: {
    url: process.env.IFRA_HOOKS_URL || `http://ifra.agents.svc.cluster.local:18789/hooks/agent`,
    token: process.env.IFRA_HOOKS_TOKEN,
    gatewayToken: process.env.IFRA_GATEWAY_TOKEN,
  },
  "neo-worker": {
    url: process.env.NEO_WORKER_HOOKS_URL || `http://neo-worker.agents.svc.cluster.local:18789/hooks/agent`,
    token: process.env.NEO_WORKER_HOOKS_TOKEN,
    gatewayToken: process.env.NEO_WORKER_GATEWAY_TOKEN,
  },
  "ifra-worker": {
    url: process.env.IFRA_WORKER_HOOKS_URL || `http://ifra-worker.agents.svc.cluster.local:18789/hooks/agent`,
    token: process.env.IFRA_WORKER_HOOKS_TOKEN,
    gatewayToken: process.env.IFRA_WORKER_GATEWAY_TOKEN,
  },
  "research-worker": {
    url: process.env.RESEARCH_WORKER_HOOKS_URL || `http://research-worker.agents.svc.cluster.local:18789/hooks/agent`,
    token: process.env.RESEARCH_WORKER_HOOKS_TOKEN || "research-worker-hooks-tok-2026",
    gatewayToken: process.env.RESEARCH_WORKER_GATEWAY_TOKEN || "research-worker-gw-tok-2026",
  },
  "beta-worker": {
    url: process.env.BETA_WORKER_HOOKS_URL || `http://beta-worker.agents.svc.cluster.local:18789/hooks/agent`,
    token: process.env.BETA_WORKER_HOOKS_TOKEN || "beta-worker-hooks-tok-2026",
    gatewayToken: process.env.BETA_WORKER_GATEWAY_TOKEN || "beta-worker-gw-tok-2026",
  },
};


// Close a hook session on the agent gateway when a task is done/reset
async function closeAgentSession(agentName, taskId, isQa = false) {
  const agent = AGENTS[agentName];
  if (!agent?.gatewayToken) return;
  const gwUrl = agent.url.replace(/\/hooks\/agent$/, "");
  const sessionKey = isQa ? `agent:main:hook:qa:${taskId}` : `agent:main:hook:task:${taskId}`;
  try {
    // Send a "close" message to the session to terminate it gracefully
    const resp = await fetch(`${gwUrl}/tools/invoke`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${agent.gatewayToken}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ tool: 'sessions_send', parameters: { sessionKey, message: '[SYSTEM] Task session closed by dispatcher. Stop all work.' } }),
      signal: AbortSignal.timeout(5000),
    });
    console.log(`[SESSION-CLEANUP] Closed session ${sessionKey} on ${agentName}: ${resp.status}`);
  } catch (e) {
    console.warn(`[SESSION-CLEANUP] Failed to close ${sessionKey} on ${agentName}: ${e.message}`);
  }
}

// Only fetch online agents — disabled agents are never auto-assigned or dispatched to
async function getAgentCards() {
  const { data, error } = await supabase
    .from('agent_cards')
    .select('name, capabilities, task_types, max_capacity, priority_affinity, status, available_credentials')
    .eq('status', 'online');
  if (error) {
    console.error('[CARDS] Failed to fetch agent cards:', error.message);
    return [];
  }
  // Normalize to common shape used by scheduler
  return (data || []).map(c => ({
    name: c.name.toLowerCase(),
    capabilities: c.task_types || [],
    max_concurrent: c.max_capacity != null ? c.max_capacity : 2,
    priority_affinity: c.priority_affinity || {},
    available_credentials: c.available_credentials || [],
  }));
}

const DANTE_ID_API_URL = process.env.DANTE_ID_API_URL || "https://api.dante.id";

// --- App Scope Helpers ---
// Build the app scope section for agent prompts (shared between dispatch and QA)
function buildAppScopeSection(appContext) {
  const repos = (appContext.repos || []);
  const repoList = repos.join(', ');
  let section = `
## 🔒 App Scope: ${appContext.name}

**ALLOWED REPOS: ${repoList}. You MUST ONLY clone, modify, and push to these repos. Pushing to any other repo is a HARD FAILURE.**

- **App:** ${appContext.name} (${appContext.slug})
- **Allowed repos:** ${repos.map(r => '`' + r + '`').join(', ')}`;
  if (appContext.supabase_project_ref) {
    section += `\n- **SUPABASE PROJECT:** ${appContext.supabase_project_ref} (URL: https://${appContext.supabase_project_ref}.supabase.co)`;
  }
  if (appContext.deploy_target) {
    section += `\n- **DEPLOY TARGET:** ${appContext.deploy_target}`;
  }
  if (appContext.deploy_config && Object.keys(appContext.deploy_config).length > 0) {
    section += `\n- **Deploy config:** ${JSON.stringify(appContext.deploy_config)}`;
  }
  if (appContext.description) {
    section += `\n- **Description:** ${appContext.description}`;
  }
  section += `

⚠️ **CRITICAL:** Any push to a repo NOT in the allowed list above is a cross-repo contamination incident and a HARD FAILURE.
`;
  return section;
}

// Build the QA repo validation section for app-scoped tasks
function buildQaRepoValidation(appContext) {
  const repos = (appContext.repos || []);
  const repoList = repos.map(r => '`' + r + '`').join(', ');
  return `
### 🔒 REPO SCOPE VALIDATION (App: ${appContext.name})

**MANDATORY CHECK:** This task is scoped to app **${appContext.name}**. Allowed repos: ${repoList}.

You MUST verify:
1. The PR targets one of the allowed repos: ${repoList}
2. No commits or file changes reference repos outside the allowed list
3. If the PR targets a repo NOT in the allowed list → **IMMEDIATE FAIL** with: "Cross-repo contamination: PR targets unauthorized repo"
${appContext.supabase_project_ref ? `4. If Supabase queries are present, verify they target project ref \`${appContext.supabase_project_ref}\`` : ''}
`;
}

// --- App Credential Checking ---
// Check if an agent has the required credentials for an app-scoped task
// Returns { ok: true } if credentials are satisfied, or { ok: false, missing: [...] } if not
function checkAgentCredentials(appContext, agentCredentials, credentialType = 'coding') {
  if (!appContext) return { ok: true, missing: [] };
  const required = credentialType === 'qa'
    ? (appContext.required_qa_credentials || [])
    : (appContext.required_credentials || []);
  if (!required.length) return { ok: true, missing: [] };
  const available = new Set(agentCredentials || []);
  const missing = required.filter(c => !available.has(c));
  return { ok: missing.length === 0, missing };
}

// Fetch agent available_credentials from agent_cards by name
async function getAgentCredentials(agentName) {
  if (!agentName) return [];
  try {
    const { data, error } = await supabase
      .from('agent_cards')
      .select('available_credentials')
      .ilike('name', agentName)
      .single();
    if (error || !data) return [];
    return data.available_credentials || [];
  } catch (e) {
    console.warn(`[CREDENTIALS] Error fetching credentials for ${agentName}:`, e.message);
    return [];
  }
}

// Fetch app context from Supabase by app_id
async function fetchAppContext(taskId, appId) {
  if (!appId) return null;
  try {
    const { data: app, error: appErr } = await supabase
      .from('apps')
      .select('*')
      .eq('id', appId)
      .single();
    if (!appErr && app) {
      console.log(`[APP] Fetched app "${app.name}" for task ${taskId} (repos: ${(app.repos || []).join(', ')})`);
      return app;
    }
    if (appErr) console.warn(`[APP] Failed to fetch app ${appId} for task ${taskId}:`, appErr.message);
  } catch (e) {
    console.warn(`[APP] Error fetching app for task ${taskId}:`, e.message);
  }
  return null;
}
const PRIORITY_ORDER = { urgent: 0, high: 1, normal: 2, low: 3 };
// === GUARD: State transition cooldown (prevents rapid cycling) ===
const taskLastTransition = new Map();
const TRANSITION_COOLDOWN_MS = 60_000;
function canTransition(taskId) {
  const last = taskLastTransition.get(taskId);
  if (!last) return true;
  return (Date.now() - last) >= TRANSITION_COOLDOWN_MS;
}
function recordTransition(taskId) {
  taskLastTransition.set(taskId, Date.now());
  if (taskLastTransition.size > 200) {
    const cutoff = Date.now() - 600000;
    for (const [k, v] of taskLastTransition) { if (v < cutoff) taskLastTransition.delete(k); }
  }
}


// --- Discord Webhook for Blocked Task Alerts ---
const DISCORD_WEBHOOK_URL = process.env.DISCORD_BLOCKED_WEBHOOK_URL;
const DANTE_DISCORD_ID = process.env.DANTE_DISCORD_ID || "185059032531206146";

async function notifyBlockedTask(task) {
  if (!DISCORD_WEBHOOK_URL) {
    console.log(`[BLOCKED-ALERT] No DISCORD_BLOCKED_WEBHOOK_URL configured, skipping notification for task ${task.id}`);
    return;
  }
  const reason = task.blocked_reason || task.error || "No reason provided";
  const taskUrl = `https://tasks.dante.id/task/${task.id}`;
  const agent = task.assigned_agent || "unknown";
  const content = `🚫 **Task Blocked — Manual Intervention Required** <@${DANTE_DISCORD_ID}>

**Task:** [${task.title}](${taskUrl})
**Agent:** ${agent}
**Priority:** ${task.priority || "normal"}
**Blocked Reason:** ${reason}

_Task ID: \`${task.id}\`_`;

  try {
    const resp = await fetch(DISCORD_WEBHOOK_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ content }),
      signal: AbortSignal.timeout(10000),
    });
    if (resp.ok) {
      console.log(`[BLOCKED-ALERT] Discord notification sent for task ${task.id}`);
    } else {
      console.error(`[BLOCKED-ALERT] Discord webhook returned ${resp.status} for task ${task.id}`);
    }
  } catch (e) {
    console.error(`[BLOCKED-ALERT] Failed to send Discord notification for task ${task.id}:`, e.message);
  }
}

// --- Gateway Concurrency Check ---
// Check if an agent is currently busy by querying their gateway's sessions_list.
// Returns true if the agent has active sessions (Discord, hooks, sub-agents) updated recently.
async function checkAgentBusy(agentName) {
  const agent = AGENTS[agentName];
  if (!agent?.gatewayToken) return false; // No token = can't check = assume free
  
  const gatewayBase = agent.url.replace(/\/hooks\/agent$/, '');
  const invokeUrl = `${gatewayBase}/tools/invoke`;
  
  try {
    const res = await fetch(invokeUrl, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${agent.gatewayToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ tool: 'sessions_list', parameters: { activeMinutes: 2, messageLimit: 0 } }),
      signal: AbortSignal.timeout(5000),
    });
    
    if (!res.ok) {
      if (res.status === 401 || res.status === 403) {
        console.log(`[BUSY-CHECK] ${agentName}: AUTH FAILED (${res.status}), credentials invalid`);
      } else {
        console.log(`[BUSY-CHECK] ${agentName}: gateway returned ${res.status}, treating as busy`);
      }
      return true; // Fail safe: if we can't check, assume busy
    }
    
    const data = await res.json();
    const sessions = data?.result?.details?.sessions || data?.details?.sessions || [];
    
    // Build set of task IDs that are NOT active (completed/failed/todo/done) — these are zombie sessions
    const hookTaskIds = sessions
      .map(s => (s.key || '').match(/hook:task:(.+)/)?.[1])
      .filter(Boolean);
    const zombieTaskIds = new Set();
    if (hookTaskIds.length > 0) {
      const { data: tasks } = await supabase
        .from('agent_tasks')
        .select('id, status')
        .in('id', hookTaskIds);
      for (const t of (tasks || [])) {
        if (!['in_progress', 'qa_testing'].includes(t.status)) {
          zombieTaskIds.add(t.id);
        }
      }
    }

    // Filter out noise: only count sessions where the agent is ACTIVELY working
    // Discord sessions update on every received message — that doesn't mean the agent is busy
    const activeSessions = sessions.filter(s => {
      const key = s.key || '';
      // Skip cron and heartbeat sessions
      if (key.includes(':cron:')) return false;
      if (key.includes(':main') && s.displayName === 'heartbeat') return false;
      // Skip sessions older than 2 minutes
      const age = Date.now() - (s.updatedAt || 0);
      if (age > 2 * 60 * 1000) return false;
      // Discord sessions: only "busy" if the agent is mid-turn (abortedLastRun === false 
      // means a run completed, not that one is active). Check if the session has been 
      // actively generating tokens very recently — use a tighter window for Discord.
      if (key.includes('discord:channel:')) {
        // Only count as busy if agent responded very recently (< 30s)
        // This catches active conversations but not idle channels
        if (age > 30 * 1000) return false;
      }
      // Hook task sessions: only count as busy if the task is actually active
      if (key.includes('hook:task:')) {
        const taskId = key.split('hook:task:')[1];
        if (taskId && zombieTaskIds.has(taskId)) return false;
      }
      // Sub-agent sessions: always count as busy
      return true;
    });
    
    if (activeSessions.length > 0) {
      const types = activeSessions.map(s => {
        if (s.key?.includes('discord:')) return 'discord';
        if (s.key?.includes('hook:')) return 'hook';
        if (s.key?.includes('subagent:')) return 'subagent';
        return 'other';
      });
      console.log(`[BUSY-CHECK] ${agentName}: BUSY (${activeSessions.length} active: ${[...new Set(types)].join(', ')})`);
      return true;
    }
    
    console.log(`[BUSY-CHECK] ${agentName}: FREE`);
    return false;
  } catch (err) {
    console.log(`[BUSY-CHECK] ${agentName}: error (${err.message}), treating as busy`);
    return true; // Fail safe
  }
}
// --- Auth Preflight Check ---
// Verify agent gateway credentials before dispatching a task.
// Prevents: token expires → dispatch → session can't call LLM → 10min timeout → requeue loop
async function preflightAuthCheck(agentName) {
  const agent = AGENTS[agentName];
  if (!agent?.gatewayToken) return { ok: true, status: 0 }; // No token configured = skip check

  const gatewayBase = agent.url.replace(/\/hooks\/agent$/, '');
  const invokeUrl = `${gatewayBase}/tools/invoke`;

  try {
    const res = await fetch(invokeUrl, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${agent.gatewayToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ tool: 'sessions_list', parameters: { limit: 1, messageLimit: 0 } }),
      signal: AbortSignal.timeout(5000),
    });

    if (res.ok) {
      return { ok: true, status: 200 };
    }
    console.log(`[AUTH-PREFLIGHT] ${agentName}: gateway returned ${res.status}`);
    return { ok: false, status: res.status };
  } catch (err) {
    console.log(`[AUTH-PREFLIGHT] ${agentName}: network error (${err.message})`);
    return { ok: false, status: 0, error: err.message };
  }
}

const SCHEDULER_INTERVAL = 30_000; // 30 seconds
const FACTORY_POLL_INTERVAL = 15_000; // 15 seconds
const FACTORY_MAX_WAIT = 10 * 60 * 1000; // 10 minutes

// Factory pipeline stage order
const STAGE_PIPELINE = ['refinery', 'foundry', 'builder', 'inspector', 'deployer'];

function getNextStage(currentStage) {
  const idx = STAGE_PIPELINE.indexOf(currentStage);
  if (idx < 0 || idx >= STAGE_PIPELINE.length - 1) return null;
  return STAGE_PIPELINE[idx + 1];
}

function isFinalStage(stage) {
  return stage === STAGE_PIPELINE[STAGE_PIPELINE.length - 1];
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// Log an event to the task's activity log (visible in dashboard Activity tab)
async function logTaskActivity(taskId, field, oldValue, newValue, changedBy = 'dispatcher') {
  try {
    const entry = {
      task_id: taskId,
      field,
      old_value: oldValue,
      new_value: newValue,
      changed_by: changedBy,
      changed_at: new Date().toISOString(),
    };
    // Auto-classify errors
    if ((field === 'error' || field === 'dispatch_error') && newValue) {
      const classification = classifyError(newValue);
      entry.error_category = classification.category;
    }
    await supabase.from('task_activity_log').insert(entry);
  } catch (e) {
    console.error('[ACTIVITY-LOG] Failed to log activity for task', taskId, ':', e.message);
  }
}

// Log a transient error to activity log instead of the error field.
// Transient errors are recoverable issues (session lost, timeout, idle retry, dispatch failure).
// Only TERMINAL errors (max retries, permanent failure) should set the error field.
async function logTransientError(taskId, message, changedBy = 'dispatcher') {
  const classification = classifyError(message);
  console.log(`[TRANSIENT-ERROR] Task ${taskId} [${classification.category}]: ${message}`);
  await logTaskActivity(taskId, 'error', null, message, changedBy);
}

// Boot-time: detect orphaned tasks (assigned but never dispatched due to restart)
async function detectOrphanedTasks() {
  try {
    const fiveMinAgo = new Date(Date.now() - 5 * 60 * 1000).toISOString();
    const { data: orphaned, error } = await supabase
      .from('agent_tasks')
      .select('id, title, assigned_agent, status, updated_at')
      .eq('status', 'todo')
      .not('assigned_agent', 'is', null)
      .lt('updated_at', fiveMinAgo);

    if (error || !orphaned?.length) return;

    console.log('[BOOT-ORPHAN] Found ' + orphaned.length + ' orphaned tasks (assigned but stuck in todo)');
    for (const task of orphaned) {
      const agent = task.assigned_agent;
      console.log('[BOOT-ORPHAN] Re-dispatching ' + task.id.slice(0,8) + ' ("' + task.title.slice(0,40) + '") — was assigned to ' + agent);
      
      // Log the error to activity
      await logTaskActivity(task.id, 'dispatch_error', null,
        'Task was assigned to ' + agent + ' but dispatch was lost (dispatcher restarted). Auto-recovering — clearing assignment for re-dispatch.',
        'dispatcher');

      // Clear assignment so scheduler picks it up fresh
      await supabase.from('agent_tasks').update({
        assigned_agent: null,
        updated_at: new Date().toISOString(),
      }).eq('id', task.id);
      await logTransientError(task.id, 'Dispatch lost during restart — re-queued automatically');
    }
  } catch (e) {
    console.error('[BOOT-ORPHAN] Error:', e.message);
  }
}


// Map project status → task stage
const STATUS_TO_STAGE = {
  new: "refinery",
  refining: "refinery",
  designed: "foundry",
  building: "builder",
  testing: "inspector",
  deploying: "deployer",
  live: "deployer",
};

// --- Dispatch coding task via Dante ID factory pipeline ---
async function dispatchViaFactory(task) {
  const idea = task.description || task.prompt;
  if (!idea) {
    console.error(`[FACTORY] Task ${task.id} has no description/prompt, falling back to agent dispatch`);
    return false;
  }

  try {
    // 1. Create project in Supabase
    const { data: project, error: projectErr } = await supabase
      .from("projects")
      .insert({
        name: task.title,
        idea,
        user_id: "system",
        type: "external",
        status: "new",
      })
      .select("id")
      .single();

    if (projectErr || !project) {
      console.error(`[FACTORY] Failed to create project for task ${task.id}:`, projectErr?.message);
      return false;
    }

    const projectId = project.id;
    console.log(`[FACTORY] Created project ${projectId} for task ${task.id} ("${task.title}")`);

    // 2. Link project to task and set initial stage
    await supabase
      .from("agent_tasks")
      .update({
        project_id: projectId,
        stage: "refinery",
        status: "in_progress",
        started_at: new Date().toISOString(),
      })
      .eq("id", task.id);

    // 3. Trigger the factory pipeline
    const resp = await fetch(`${DANTE_ID_API_URL}/api/refinery/generate-all`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${SUPABASE_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ project_id: projectId, idea }),
    });

    if (!resp.ok) {
      const errText = await resp.text();
      console.error(`[FACTORY] API returned ${resp.status} for task ${task.id}: ${errText}`);
      await supabase.from("agent_tasks").update({
        status: "failed",
        error: `Factory API error: ${resp.status} - ${errText}`,
        completed_at: new Date().toISOString(),
      }).eq("id", task.id);
      return true;
    }

    console.log(`[FACTORY] Pipeline triggered for project ${projectId}, polling status...`);

    // 4. Poll project status
    const startTime = Date.now();
    let lastStage = "refinery";
    const stageResults = {};
    let staleCount = 0;

    const pollInterval = setInterval(async () => {
      try {
        const elapsed = Date.now() - startTime;
        if (elapsed > FACTORY_MAX_WAIT) {
          clearInterval(pollInterval);
          console.error(`[FACTORY] Timeout for task ${task.id} (project ${projectId})`);
          await supabase.from("agent_tasks").update({
            status: "failed",
            error: `Factory pipeline timed out after ${Math.round(elapsed / 60000)} minutes`,
            completed_at: new Date().toISOString(),
          }).eq("id", task.id);
          return;
        }

        const { data: proj, error: pollErr } = await supabase
          .from("projects")
          .select("status")
          .eq("id", projectId)
          .single();

        if (pollErr || !proj) {
          console.error(`[FACTORY] Poll error for project ${projectId}:`, pollErr?.message);
          return;
        }

        const projectStatus = proj.status;
        const newStage = STATUS_TO_STAGE[projectStatus] || lastStage;

        if (newStage !== lastStage) {
          console.log(`[FACTORY] Task ${task.id}: stage ${lastStage} → ${newStage} (project status: ${projectStatus})`);
          await supabase.from("agent_tasks").update({ stage: newStage }).eq("id", task.id);
          staleCount = 0;

          // Capture results from completed stage
          try {
            if (lastStage === "refinery") {
              const { data: prds } = await supabase.from("prds").select("id, title, content").eq("project_id", projectId);
              const { data: features } = await supabase.from("features").select("id, name, description").eq("project_id", projectId);
              stageResults.refinery = { prds: prds || [], features: features || [] };
            } else if (lastStage === "foundry") {
              const { data: blueprints } = await supabase.from("blueprints").select("id, feature_id, content").eq("project_id", projectId);
              stageResults.foundry = { blueprints: blueprints || [] };
            } else if (lastStage === "builder") {
              const { data: builds } = await supabase.from("builds").select("id, feature_id, status, code").eq("project_id", projectId);
              stageResults.builder = { builds: builds || [] };
            } else if (lastStage === "inspector") {
              const { data: testResults } = await supabase.from("test_results").select("id, feature_id, score, summary").eq("project_id", projectId);
              stageResults.inspector = { test_results: testResults || [] };
            } else if (lastStage === "deployer") {
              const { data: deployments } = await supabase.from("deployments").select("id, url, status").eq("project_id", projectId);
              stageResults.deployer = { deployments: deployments || [] };
            }
          } catch (stageErr) {
            console.warn(`[FACTORY] Failed to capture ${lastStage} results for project ${projectId}:`, stageErr.message);
          }

          lastStage = newStage;
        } else {
          staleCount++;
          if (staleCount >= 5) {
            console.warn(`[FACTORY] Task ${task.id}: project ${projectId} stale — no stage change for ${staleCount} polls (~${staleCount * 15}s)`);
          }
        }

        // Terminal: live
        if (projectStatus === "live") {
          clearInterval(pollInterval);

          // Try to get deployment URL
          let deploymentUrl = null;
          try {
            const { data: deployment } = await supabase
              .from("deployments")
              .select("url")
              .eq("project_id", projectId)
              .order("created_at", { ascending: false })
              .limit(1)
              .single();
            deploymentUrl = deployment?.url;
          } catch {}

          console.log(`[FACTORY] Task ${task.id} completed! Deployment: ${deploymentUrl || "N/A"}`);
          const factoryUpdate = {
            status: "qa_testing",
            result: { output: "Factory pipeline completed", deployment_url: deploymentUrl, project_id: projectId, stage_results: stageResults },
            completed_at: new Date().toISOString(),
          };
          // Also set the top-level deployment_url column
          if (deploymentUrl) factoryUpdate.deployment_url = deploymentUrl;
          await supabase.from("agent_tasks").update(factoryUpdate).eq("id", task.id);
          return;
        }

        // Terminal: error/failed
        if (projectStatus === "error" || projectStatus === "failed") {
          clearInterval(pollInterval);
          console.error(`[FACTORY] Task ${task.id} failed (project status: ${projectStatus})`);
          await supabase.from("agent_tasks").update({
            status: "failed",
            error: `Factory pipeline failed with status: ${projectStatus}`,
            completed_at: new Date().toISOString(),
          }).eq("id", task.id);
          return;
        }
      } catch (e) {
        console.error(`[FACTORY] Poll error for task ${task.id}: ${e.message}`);
      }
    }, FACTORY_POLL_INTERVAL);

    return true;
  } catch (e) {
    console.error(`[FACTORY] Error dispatching task ${task.id}: ${e.message}`);
    return false;
  }
}

// --- A2A: Parent-child task completion ---
async function checkParentCompletion(task) {
  if (!task.parent_task_id) return;

  console.log(`[A2A] Task ${task.id} has parent ${task.parent_task_id}, checking siblings...`);

  const { data: siblings, error } = await supabase
    .from('agent_tasks')
    .select('id, status, result, error, title')
    .eq('parent_task_id', task.parent_task_id);

  if (error) {
    console.error(`[A2A] Error querying siblings for parent ${task.parent_task_id}:`, error.message);
    return;
  }

  if (!siblings?.length) return;

  const terminal = ['qa_testing', 'completed', 'failed'];
  const allTerminal = siblings.every(s => terminal.includes(s.status));

  if (!allTerminal) {
    const pending = siblings.filter(s => !terminal.includes(s.status)).length;
    console.log(`[A2A] Parent ${task.parent_task_id}: ${pending}/${siblings.length} sub-tasks still pending`);
    return;
  }

  console.log(`[A2A] All ${siblings.length} sub-tasks for parent ${task.parent_task_id} are terminal, aggregating...`);

  const failed = siblings.filter(s => s.status === 'failed');
  const succeeded = siblings.filter(s => s.status !== 'failed');

  const aggregated = {
    sub_tasks: siblings.map(s => ({
      id: s.id,
      title: s.title,
      status: s.status,
      result: s.result || null,
      error: s.error || null,
    })),
    total: siblings.length,
    succeeded: succeeded.length,
    failed: failed.length,
  };

  if (failed.length > 0) {
    aggregated.partial_failures = failed.map(f => ({ id: f.id, title: f.title, error: f.error }));
  }

  const { error: updateErr } = await supabase
    .from('agent_tasks')
    .update({
      status: 'qa_testing',
      result: aggregated,
      completed_at: new Date().toISOString(),
    })
    .eq('id', task.parent_task_id);

  if (updateErr) {
    console.error(`[A2A] Failed to update parent ${task.parent_task_id}:`, updateErr.message);
  } else {
    console.log(`[A2A] Parent ${task.parent_task_id} → done (${succeeded.length} succeeded, ${failed.length} failed)`);
  }
}

// Track active dispatched tasks: taskId → { agentName, dispatchedAt, sessionKey }
const activeTasks = new Map();

// --- SSE Client Management ---
const sseClients = new Set();
const latestProgress = new Map();

function broadcast(eventType, data, taskId) {
  const payload = `event: ${eventType}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const client of sseClients) {
    if (taskId && client.taskFilter && !client.taskFilter.has(taskId)) continue;
    try { client.res.write(payload); } catch { sseClients.delete(client); }
  }
}

// --- Context Manager: build institutional memory block ---
// --- Context Manager: inject institutional memory into agent sessions ---
function extractKeywords(title) {
  const stopWords = new Set(['the','a','an','and','or','for','to','in','on','with','of','is','it','fix','add','update','task','feat','feature','bug','refactor','v2','v1']);
  return (title || '').toLowerCase().split(/[\s\-_:,|/]+/).filter(w => w.length > 2 && !stopWords.has(w));
}

async function buildContextBlock(task) {
  const sections = [];
  let previousResultsSection = '';
  let retryAlert = '';
  let failedSection = '';

  try {
    // 1. Previous task results — by repo/project or title keyword similarity
    try {
      let data = null;
      if (task.repo || task.project_id) {
        let query = supabase.from('agent_tasks')
          .select('title, status, result, assigned_agent, completed_at')
          .in('status', ['done', 'completed'])
          .order('completed_at', { ascending: false })
          .limit(5);
        if (task.repo) query = query.eq('repo', task.repo);
        else if (task.project_id) query = query.eq('project_id', task.project_id);
        const res = await query;
        data = res.data;
      } else {
        // Title-based similarity: query by top keywords using ilike
        const keywords = extractKeywords(task.title).slice(0, 3);
        if (keywords.length) {
          const filters = keywords.map(k => `title.ilike.%${k}%`);
          const { data: kwData } = await supabase.from('agent_tasks')
            .select('title, status, result, assigned_agent, completed_at')
            .in('status', ['done', 'completed'])
            .or(filters.join(','))
            .order('completed_at', { ascending: false })
            .limit(5);
          data = kwData;
        }
      }
      if (data?.length) {
        const lines = ['### Previous Task Results'];
        for (const t of data) {
          const summary = t.result?.output || t.result?.summary || (t.result ? JSON.stringify(t.result).slice(0, 200) : '(no result)');
          lines.push(`- **${t.title}** (${t.status}, by ${t.assigned_agent}): ${summary}`);
        }
        previousResultsSection = lines.join('\n');
      }
    } catch (e) { console.error('[CONTEXT] Previous results query failed:', e.message); }

    // 2. Failed tasks — by repo/type AND title keyword similarity
    try {
      const taskKeywords = extractKeywords(task.title);
      // Fetch recent failed tasks (broad query, filter in JS for keyword match)
      let query = supabase.from('agent_tasks')
        .select('title, error, type, repo, completed_at')
        .eq('status', 'failed')
        .order('completed_at', { ascending: false })
        .limit(10);
      if (task.repo) query = query.eq('repo', task.repo);
      else if (task.type) query = query.eq('type', task.type);
      const { data: repoFailed } = await query;

      // Also fetch by keyword similarity if we have keywords
      let keywordFailed = [];
      if (taskKeywords.length >= 2) {
        const filters = taskKeywords.slice(0, 3).map(k => `title.ilike.%${k}%`);
        const { data: kwFailed } = await supabase.from('agent_tasks')
          .select('title, error, type, repo, completed_at')
          .eq('status', 'failed')
          .or(filters.join(','))
          .order('completed_at', { ascending: false })
          .limit(10);
        keywordFailed = kwFailed || [];
      }

      // Merge and deduplicate
      const seenTitles = new Set();
      const allFailed = [];
      for (const t of [...(repoFailed || []), ...keywordFailed]) {
        const key = t.title + '|' + t.completed_at;
        if (!seenTitles.has(key)) {
          seenTitles.add(key);
          // Include if repo/type matched OR shares 2+ keywords
          const fKeywords = extractKeywords(t.title);
          const overlap = taskKeywords.filter(k => fKeywords.includes(k)).length;
          if ((repoFailed || []).includes(t) || overlap >= 2) {
            allFailed.push(t);
          }
        }
      }

      // 3. Retry detection — very similar failed task within 24h
      const now = Date.now();
      const dayAgo = new Date(now - 24 * 60 * 60 * 1000).toISOString();
      for (const t of allFailed) {
        if (t.completed_at && t.completed_at > dayAgo) {
          const fKeywords = extractKeywords(t.title);
          const overlap = taskKeywords.filter(k => fKeywords.includes(k)).length;
          if (overlap >= 2 || (taskKeywords.length > 0 && overlap === taskKeywords.length)) {
            const errorMsg = typeof t.error === 'string' ? t.error : (t.error ? JSON.stringify(t.error).slice(0, 500) : '(unknown error)');
            retryAlert = `### 🔴 RETRY ALERT\nThis task appears to be a retry of recently failed: **${t.title}**\nFull failure reason: ${errorMsg}`;
            break;
          }
        }
      }

      if (allFailed.length) {
        const lines = ['### ⚠️ Previously Failed Tasks (avoid repeating these mistakes)'];
        for (const t of allFailed.slice(0, 5)) {
          const errorMsg = typeof t.error === 'string' ? t.error : (t.error ? JSON.stringify(t.error).slice(0, 300) : '(unknown error)');
          lines.push(`- **${t.title}**: ${errorMsg}`);
        }
        failedSection = lines.join('\n');
      }
    } catch (e) { console.error('[CONTEXT] Failed tasks query failed:', e.message); }

    // 4. Agent cards context — online agents with capabilities and task counts
    try {
      const { data: agents } = await supabase.from('agent_cards')
        .select('name, task_types, status')
        .eq('status', 'online')
        .limit(20);
      if (agents?.length) {
        // Get active task counts per agent
        const { data: activeTasks } = await supabase.from('agent_tasks')
          .select('assigned_agent')
          .in('status', ['in_progress'])
          .limit(100);
        const taskCounts = {};
        for (const t of (activeTasks || [])) {
          taskCounts[t.assigned_agent] = (taskCounts[t.assigned_agent] || 0) + 1;
        }
        const lines = ['### Online Agents'];
        for (const a of agents) {
          const caps = Array.isArray(a.task_types) ? a.task_types.join(', ') : (a.task_types || 'general');
          const count = taskCounts[a.name] || 0;
          lines.push(`- **${a.name}** [${caps}] — ${count} active task${count !== 1 ? 's' : ''}`);
        }
        sections.push(lines.join('\n'));
      }
    } catch (e) { console.error('[CONTEXT] Agent cards query failed:', e.message); }

    // 5. Project context
    if (task.project_id) {
      try {
        const { data: project } = await supabase.from('projects')
          .select('name, idea, description, status, type')
          .eq('id', task.project_id)
          .single();
        if (project) {
          sections.push('### Project Context');
          sections.push(`- **${project.name}** (${project.status}): ${project.idea || project.description || '(no description)'}`);
        }
      } catch (e) { console.error('[CONTEXT] Project context query failed:', e.message); }
    }
  } catch (e) {
    console.error('[CONTEXT] Unexpected error building context:', e.message);
  }

  // Assemble with token budget (max 2000 chars)
  // Priority: retry alert > failed tasks > agent cards/project > previous results (truncated first)
  const TOKEN_BUDGET = 2000;

  // Fixed sections (high priority — keep at full length)
  const fixedParts = [retryAlert, failedSection, ...sections].filter(Boolean);
  const fixedText = fixedParts.join('\n');

  let result = '';
  if (previousResultsSection) {
    const available = TOKEN_BUDGET - fixedText.length - 50; // 50 for header/separators
    if (available > 100 && previousResultsSection.length > available) {
      previousResultsSection = previousResultsSection.slice(0, available) + '…';
    } else if (available <= 100) {
      previousResultsSection = '';
    }
  }

  const allParts = [retryAlert, previousResultsSection, failedSection, ...sections].filter(Boolean);
  if (!allParts.length) return '';

  result = '\n## Context (Institutional Memory)\n\n' + allParts.join('\n\n') + '\n';

  // Final hard truncation
  if (result.length > TOKEN_BUDGET) {
    result = result.slice(0, TOKEN_BUDGET - 1) + '…';
  }

  console.log(`[CONTEXT] Built context v2 for task ${task.id}: ${result.length} chars`);
  return result;
}

async function buildContextBlockWithTimeout(task) {
  try {
    return await Promise.race([
      buildContextBlock(task),
      new Promise(resolve => setTimeout(() => {
        console.log(`[CONTEXT] Timeout building context for task ${task.id}`);
        resolve('');
      }, 5000)),
    ]);
  } catch (e) {
    console.error('[CONTEXT] Error:', e.message);
    return '';
  }
}

// --- Build blocker resolution context for retry dispatch ---
function buildBlockerContext(task) {
  const metadata = task.metadata;
  if (!metadata?.blocker) return '';

  const blocker = metadata.blocker;
  const hasProvidedValues = blocker.provided_values && Object.keys(blocker.provided_values).length > 0;
  const hasHumanResponse = !!blocker.human_response;

  if (!hasProvidedValues && !hasHumanResponse) return '';

  const lines = ['## Human Input (Blocker Resolved)', ''];
  lines.push(`This task was previously blocked. **Reason:** ${blocker.reason || blocker.blocked_reason || '(not specified)'}`);
  lines.push('');

  if (hasProvidedValues) {
    lines.push('### Provided Values');
    for (const [key, value] of Object.entries(blocker.provided_values)) {
      // Mask credential-like values in the prompt for logging safety
      const isCredential = /key|secret|token|password|api_key/i.test(key);
      if (isCredential) {
        lines.push(`- **${key}:** \`[PROVIDED — access via task metadata]\``);
      } else {
        lines.push(`- **${key}:** \`${value}\``);
      }
    }
    lines.push('');
    lines.push('> **For credential values:** Fetch full values from the task metadata via Supabase API:');
    lines.push('> ```bash');
    lines.push(`> curl -s "https://lessxkxujvcmublgwdaa.supabase.co/rest/v1/agent_tasks?id=eq.${task.id}&select=metadata" \\`);
    lines.push(`>   -H "apikey: ${SUPABASE_KEY}" -H "Authorization: Bearer ${SUPABASE_KEY}" | jq '.[0].metadata.blocker.provided_values'`);
    lines.push('> ```');
    lines.push('');
  }

  if (hasHumanResponse) {
    lines.push('### Human Response');
    lines.push(blocker.human_response);
    lines.push('');
  }

  lines.push('Use these inputs to continue the task.\n');
  return lines.join('\n');
}

// Archive blocker metadata after successful dispatch
async function archiveBlockerMetadata(task) {
  const metadata = task.metadata;
  if (!metadata?.blocker) return;

  const blocker = metadata.blocker;
  const hasProvidedValues = blocker.provided_values && Object.keys(blocker.provided_values).length > 0;
  const hasHumanResponse = !!blocker.human_response;
  if (!hasProvidedValues && !hasHumanResponse) return;

  try {
    const updatedMetadata = { ...metadata };
    // Move blocker to resolved_blockers history
    if (!updatedMetadata.resolved_blockers) updatedMetadata.resolved_blockers = [];
    updatedMetadata.resolved_blockers.push({
      ...blocker,
      resolved_at: new Date().toISOString(),
    });
    delete updatedMetadata.blocker;

    await supabase.from('agent_tasks').update({ metadata: updatedMetadata }).eq('id', task.id);
    console.log(`[BLOCKER] Archived resolved blocker for task ${task.id}`);
  } catch (e) {
    console.error(`[BLOCKER] Failed to archive blocker for task ${task.id}:`, e.message);
  }
}

// --- Dispatch task to agent via /hooks/agent ---
// Fetch task comments to include in dispatch (for retries / context)
async function fetchTaskComments(taskId) {
  try {
    const { data, error } = await supabase
      .from('task_comments')
      .select('author, author_type, body, created_at')
      .eq('task_id', taskId)
      .order('created_at', { ascending: true })
      .limit(20);
    if (error || !data?.length) return '';
    const lines = ['## 💬 Comments Thread\n'];
    for (const c of data) {
      const time = new Date(c.created_at).toISOString().replace('T', ' ').slice(0, 16);
      lines.push(`**${c.author}** (${c.author_type}) — ${time}:`);
      lines.push(`> ${c.body.replace(/\n/g, '\n> ')}\n`);
    }
    return lines.join('\n');
  } catch (e) {
    console.error(`[COMMENTS] Failed to fetch comments for task ${taskId}:`, e.message);
    return '';
  }
}

async function dispatchToAgent(task) {
  // Never dispatch to disabled/degraded agents — remap or reset to todo
  if (task.assigned_agent) {
    const { data: agentCard } = await supabase
      .from('agent_cards')
      .select('status')
      .ilike('name', task.assigned_agent)
      .single();
    if (agentCard?.status === 'disabled' || agentCard?.status === 'degraded') {
      // Try to find the -worker variant of this agent
      const workerName = task.assigned_agent.toLowerCase() + '-worker';
      const { data: workerCard } = await supabase
        .from('agent_cards')
        .select('status')
        .ilike('name', workerName)
        .single();
      if (workerCard && workerCard.status !== 'disabled' && workerCard.status !== 'degraded') {
        console.log(`[REMAP] Agent ${task.assigned_agent} is disabled/degraded, remapping task ${task.id} → ${workerName}`);
        task.assigned_agent = workerName;
        await supabase
          .from('agent_tasks')
          .update({ assigned_agent: workerName, error: null })
          .eq('id', task.id);
      } else {
        // No worker variant available — reset for scheduler to pick up
        const skipReason = `Agent ${task.assigned_agent} is disabled/degraded — unassigning for re-dispatch`;
        console.log(`[SKIP] ${skipReason}`);
        await supabase
          .from('agent_tasks')
          .update({ assigned_agent: null, started_at: null })
          .eq('id', task.id);
        await logTransientError(task.id, skipReason);
        return;
      }
    }
  }

  const agentName = task.assigned_agent?.toLowerCase();

  // DISABLED: Session cleanup was restarting pods based on PVC session count,
  // but pod restart doesn't clear PVC sessions → infinite restart loop.
  // Agent health should be checked via heartbeat + agent_cards.status instead.
  if (false && agentName?.endsWith('-worker')) {
    try {
      const agent = AGENTS[agentName];
      if (agent?.gatewayToken) {
        const sessResp = await fetch(`http://${agentName}.agents.svc.cluster.local:18789/tools/invoke`, {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${agent.gatewayToken}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ tool: 'sessions_list', parameters: { activeMinutes: 60, messageLimit: 0 } }),
          signal: AbortSignal.timeout(5000),
        });
        if (sessResp.ok) {
          const sessData = await sessResp.json();
          const sessionCount = sessData?.result?.details?.count || sessData?.result?.details?.sessions?.length || 0;
          if (sessionCount > 5) {
            console.log(`[SESSION-CLEANUP] ${agentName} has ${sessionCount} sessions — logging only (cleanup disabled)`);
          }
        }
      }
    } catch (e) {
      console.warn(`[SESSION-CLEANUP] Error checking sessions for ${agentName}:`, e.message);
    }
  }

  // Auth preflight: verify gateway credentials before dispatching
  const authCheck = await preflightAuthCheck(agentName);
  if (!authCheck.ok) {
    const authReason = `Auth preflight failed for ${agentName} (HTTP ${authCheck.status}) — agent may have expired credentials`;
    console.log(`[AUTH-PREFLIGHT] ${authReason}`);
    await logTaskActivity(task.id, 'dispatch_error', null, authReason, 'dispatcher');
    // Re-check task state to avoid race conditions
    const { data: authCurrentTask } = await supabase
      .from('agent_tasks')
      .select('status, result, pull_request_url')
      .eq('id', task.id)
      .single();
    if (authCurrentTask && ['qa_testing', 'completed', 'deployed', 'done'].includes(authCurrentTask.status)) {
      console.log(`[AUTH-PREFLIGHT] Task ${task.id} already moved to ${authCurrentTask.status} — skipping`);
      return;
    }
    const authHasWork = !!(authCurrentTask?.result || (authCurrentTask?.pull_request_url && authCurrentTask.pull_request_url.length > 0));

    // NEVER reset in_progress tasks to todo — the agent may be actively working.
    // Auth failures during in_progress are transient (pod restart, token rotation, etc.)
    if (task.status === 'in_progress' && !authHasWork) {
      // Agent is working but auth failed — log warning and skip, do NOT reset
      const warnMsg = `Auth preflight failed for ${agentName} but task is in_progress — agent may be actively working. Not resetting. Will retry next cycle.`;
      console.log(`[AUTH-PREFLIGHT] ${warnMsg}`);
      await logTaskActivity(task.id, 'auth_warning', null, warnMsg, 'orchestration layer');
      return;
    }

    // If task was in_progress WITH a result/PR, the agent finished — move to qa_testing
    if (task.status === 'in_progress' && authHasWork) {
      const qaMsg = `Auth preflight failed for ${agentName} but task has results — moving to QA. Agent likely completed work before gateway went down.`;
      console.log(`[AUTH-PREFLIGHT] ${qaMsg}`);
      await supabase
        .from('agent_tasks')
        .update({
          status: 'qa_testing',
          assigned_agent: null,
          completed_at: new Date().toISOString(),
        })
        .eq('id', task.id);
      await logTaskActivity(task.id, 'status', 'in_progress', 'qa_testing', agentName);
      await logTaskActivity(task.id, 'auth_recovery', null, qaMsg, 'orchestration layer');
      return;
    }

    // For non-in_progress tasks (e.g. todo with assigned_agent), clear assignment
    const clearMsg = `Auth preflight failed for ${agentName} — clearing assignment. Task was ${task.status} (not in_progress).`;
    console.log(`[AUTH-PREFLIGHT] ${clearMsg}`);
    await supabase
      .from('agent_tasks')
      .update({
        assigned_agent: null,
        started_at: null,
      })
      .eq('id', task.id);
    await logTaskActivity(task.id, 'auth_reset', agentName, null, 'orchestration layer');
    await logTransientError(task.id, authReason);
    return;
  }

  // Route coding tasks through the Dante ID factory pipeline
  if (task.type === "coding") {
    const handled = await dispatchViaFactory(task);
    if (handled) return;
    console.log(`[DISPATCH] Factory fallback: dispatching coding task ${task.id} to agent`);
  }

  const agent = AGENTS[agentName];

  if (!agent) {
    console.error(`[SKIP] Unknown agent: ${agentName} for task ${task.id}`);
    return;
  }

  if (!agent.token) {
    console.error(`[SKIP] No token for agent: ${agentName}`);
    return;
  }

  // Fetch app context if task has app_id
  const appContext = await fetchAppContext(task.id, task.app_id);

  // Pre-flight credential check (BLOCKING) — only for app-scoped tasks
  if (appContext) {
    const agentCreds = await getAgentCredentials(agentName);
    const credCheck = checkAgentCredentials(appContext, agentCreds, 'coding');
    if (!credCheck.ok) {
      const credMsg = `[DISPATCH] Agent ${agentName} missing required credentials for app "${appContext.name}": ${credCheck.missing.join(', ')}`;
      console.warn(credMsg);
      // Unassign this agent so scheduler can try another
      await supabase.from('agent_tasks').update({ assigned_agent: null, started_at: null }).eq('id', task.id);
      await logTransientError(task.id, credMsg);
      return;
    }
  }

  const taskPayload = JSON.stringify({
    task_id: task.id,
    title: task.title,
    description: task.description,
    acceptance_criteria: task.acceptance_criteria,
    type: task.type,
    priority: task.priority,
    stage: task.stage,
    parent_task_id: task.parent_task_id,
    dispatched_by: task.dispatched_by,
    pull_request_url: task.pull_request_url,
    repo: task.repo,
    branch: task.branch,
    context: task.context,
    app_id: task.app_id || null,
  }, null, 2);

  const contextBlock = await buildContextBlockWithTimeout(task);
  const commentsBlock = await fetchTaskComments(task.id);
  const blockerContext = buildBlockerContext(task);

  // Build app scope section for repo enforcement
  const appScopeSection = appContext ? buildAppScopeSection(appContext) : "";

  // Build rebase section if metadata indicates rebase requested
  const rebaseSection = task.metadata?.rebase_requested && task.metadata?.rebase_pr ? (() => {
    const rp = task.metadata.rebase_pr;
    return `
## 🔄 Rebase Required

**This task is being re-dispatched to resolve merge conflicts on PR #${rp.number}.**

### Instructions:
1. Clone the repo: \`git clone https://x-access-token:\${GH_TOKEN}@github.com/${rp.repo}.git /tmp/${rp.repo.split('/')[1]}\`
2. Checkout the PR branch: \`cd /tmp/${rp.repo.split('/')[1]} && git checkout ${rp.branch}\`
3. Rebase against ${rp.base}: \`git rebase origin/${rp.base}\`
4. Resolve any merge conflicts (edit files, \`git add\`, \`git rebase --continue\`)
5. Force push: \`git push --force-with-lease origin ${rp.branch}\`
6. Verify the PR is now mergeable: \`gh pr view ${rp.number} -R ${rp.repo} --json mergeable\`
7. Update task status when done

**If conflicts are too complex to resolve automatically, set the task to blocked with an explanation of which files conflict and why.**
`;
  })() : "";

  // Build coding task section if applicable
  const deployTaskSection = task.type === "deploy" ? `
## Batch Deploy Task

**Use the deploy-batch skill for this work.** Read \`skills/deploy-batch/SKILL.md\` and follow it step by step.

This is a batch deploy task. You must:
1. Clone each repo listed in metadata.repos
2. For each PR: rebase onto main, force-push branch, then merge via \`gh pr merge --rebase --admin\`
3. ALWAYS verify PR state is MERGED after each merge: \`gh pr view --json state\`
4. Wait for CI to pass
5. Verify ArgoCD deploys the new image and pod is healthy
6. Update ALL subtask statuses via Supabase Management API (trigger bypass required)
7. Verify subtask statuses are correct (deployed or deploy_failed)

**CRITICAL RULES:**
- NEVER push directly to main — always use \`gh pr merge\`
- NEVER leave subtasks stuck in \`deploying\` — every one must end as \`deployed\` or \`deploy_failed\`
- Use \`SUPABASE_MGMT_TOKEN\` env var for the Management API trigger bypass
- If a PR fails, mark that subtask as deploy_failed and continue with the rest
` : "";

  const codingTaskSection = task.type === "coding" ? `
## Coding Task

**Use the coding-task skill for this work.** Read \`skills/coding-task/SKILL.md\` and follow it step by step.

- **Repo:** ${task.repo || "See task description for target repo(s)"}
- **Branch convention:** \`feat/<short-description>\` or \`fix/<short-description>\`
- **PR template:** Include task ID, summary of changes, and testing notes
- **Known repos:** queue-dashboard, task-dispatcher, dante-gitops (all under dante-alpha-assistant)
` : "";

  const message = `\`\`\`json
${taskPayload}
\`\`\`

${contextBlock}## Task Assigned: ${task.title}
${appScopeSection}
${blockerContext}${rebaseSection || (task.description || "")}
${!rebaseSection && task.prompt ? `**Prompt:** ${task.prompt}` : ""}
${!rebaseSection ? codingTaskSection : ""}${deployTaskSection}

**Task ID:** ${task.id}
**Type:** ${task.type}
**Priority:** ${task.priority}
**Dispatched by:** ${task.dispatched_by}${task.parent_task_id ? `\n\n**Parent Task:** ${task.parent_task_id}\n**Sub-task:** This is a sub-task of a larger task. Complete your portion and update status.` : ""}
${commentsBlock ? `\n${commentsBlock}` : ""}
${contextBlock}---
## 🚫 BLOCKED: When you CANNOT fully complete a task

If a task requires steps you cannot perform autonomously (DNS changes, external API config, secrets rotation, manual approval), you MUST:
1. Set the task status to \`blocked\` with a clear \`blocked_reason\` — do NOT mark it done/deployed
2. NEVER write "apply manually" or "run this in the dashboard" as part of a completed task
3. First try to do it yourself — you have: exec, curl, psql (if DB access is configured), GitHub API, browser automation, K8s kubectl
4. Only block if you genuinely cannot perform the action with your available tools

**On blocked (requires manual intervention):**

You MUST detect and self-block with structured metadata when you encounter these blocker types:
- **missing_credential** — API key, token, or secret not configured or empty
- **missing_config** — environment variable, URL, or setting needed but absent
- **ambiguous_requirement** — task description unclear, needs human clarification
- **permission_denied** — you lack access to a resource (403, RBAC, etc.)
- **external_dependency** — waiting on a third-party service, approval, or another team
- **task_dependency** — this task depends on another task that isn't completed yet
- **infrastructure** — resource not available (DB down, service unreachable, pod crash)
- **human_decision** — multiple valid approaches, need human to choose

**IMPORTANT: When blocking for task_dependency, you MUST also create the dependency relationship:**
\\\`\\\`\\\`bash
curl -s -X POST "https://lessxkxujvcmublgwdaa.supabase.co/rest/v1/task_relationships" \\\\
  -H "apikey: \${SUPABASE_KEY}" \\\\
  -H "Authorization: Bearer \${SUPABASE_KEY}" \\\\
  -H "Content-Type: application/json" \\\\
  -d '{"source_task_id":"THIS_TASK_ID","target_task_id":"DEPENDENCY_TASK_ID","relationship_type":"depends_on","created_by":"AGENT_NAME"}'
\\\`\\\`\\\`
This ensures the dispatcher will NOT re-dispatch this task until the dependency is completed/deployed.

\`\`\`bash
curl -s -X PATCH "https://lessxkxujvcmublgwdaa.supabase.co/rest/v1/agent_tasks?id=eq.${task.id}" \\
  -H "apikey: ${SUPABASE_KEY}" \\
  -H "Authorization: Bearer ${SUPABASE_KEY}" \\
  -H "Content-Type: application/json" \\
  -d '{"status":"blocked","blocked_reason":"DESCRIBE WHAT NEEDS MANUAL INTERVENTION","metadata":{"blocker":{"type":"BLOCKER_TYPE","title":"Short blocker title","description":"Detailed explanation","required_inputs":[{"key":"ENV_VAR","label":"Human label","type":"text","placeholder":"example"}],"suggested_action":"What the human should do"}},"error":"Blocked: requires manual intervention"}'
\`\`\`

**Example — missing credential:**
\`\`\`json
{"metadata":{"blocker":{"type":"missing_credential","title":"GitHub token expired","description":"GH_TOKEN returns 401","required_inputs":[{"key":"GH_TOKEN","label":"GitHub PAT","type":"password","placeholder":"ghp_..."}],"suggested_action":"Generate new PAT at github.com/settings/tokens"}}}
\`\`\`

---
## ⚠️ MANDATORY: Update task status when done

When you finish this task, you MUST update its status. Run this command:

**On success (coding tasks with a PR):**
\`\`\`bash
curl -s -X PATCH "https://lessxkxujvcmublgwdaa.supabase.co/rest/v1/agent_tasks?id=eq.${task.id}" \\
  -H "apikey: ${SUPABASE_KEY}" \\
  -H "Authorization: Bearer ${SUPABASE_KEY}" \\
  -H "Content-Type: application/json" \\
  -d '{"status":"qa_testing","assigned_agent":null,"completed_at":"'$(date -u +%Y-%m-%dT%H:%M:%SZ)'","pull_request_url":["https://github.com/OWNER/REPO/pull/NUMBER"],"result":{"summary":"DESCRIBE WHAT YOU DID","artifacts":[],"test_results":null}}'
\`\`\`

**On success (ops/review tasks WITHOUT a PR — skip QA):**
\`\`\`bash
curl -s -X PATCH "https://lessxkxujvcmublgwdaa.supabase.co/rest/v1/agent_tasks?id=eq.${task.id}" \\
  -H "apikey: ${SUPABASE_KEY}" \\
  -H "Authorization: Bearer ${SUPABASE_KEY}" \\
  -H "Content-Type: application/json" \\
  -d '{"status":"completed","completed_at":"'$(date -u +%Y-%m-%dT%H:%M:%SZ)'","result":{"summary":"DESCRIBE WHAT YOU DID","artifacts":[],"test_results":null}}'
\`\`\`

**On failure:**
\`\`\`bash
curl -s -X PATCH "https://lessxkxujvcmublgwdaa.supabase.co/rest/v1/agent_tasks?id=eq.${task.id}" \\
  -H "apikey: ${SUPABASE_KEY}" \\
  -H "Authorization: Bearer ${SUPABASE_KEY}" \\
  -H "Content-Type: application/json" \\
  -d '{"status":"failed","completed_at":"'$(date -u +%Y-%m-%dT%H:%M:%SZ)'","error":"DESCRIBE WHAT WENT WRONG"}'
\`\`\`


**On blocked (you CANNOT complete the task):**

  curl -s -X PATCH "https://lessxkxujvcmublgwdaa.supabase.co/rest/v1/agent_tasks?id=eq.${task.id}" \
    -H "apikey: ${SUPABASE_KEY}" \
    -H "Authorization: Bearer ${SUPABASE_KEY}" \
    -H "Content-Type: application/json" \
    -d '{"status":"blocked","blocked_reason":"EXPLAIN what you cannot do and why","metadata":{"blocker":{"type":"BLOCKER_TYPE","title":"Short title","description":"Why you are blocked","required_inputs":[],"suggested_action":"What human should do"}}}'

BLOCKED DETECTION RULES:
- NEVER mark done if manual steps remain (SQL migrations, external config, DNS changes)
- NEVER write "apply this manually" — if you cannot do it, set status to blocked
- NEVER ship incomplete work as complete
- Use curl, kubectl, gh CLI FIRST before deciding to block
- Only block if you genuinely CANNOT do it after trying.

DATABASE MIGRATION RULES (MANDATORY):
- If your code references NEW database columns, tables, or indexes: you MUST create a migration file
- Migration files go in migrations/NNNN_description.sql (sequential numbering)
- ALL SQL must be idempotent: use IF NOT EXISTS, IF EXISTS, CREATE OR REPLACE
- Include the migration file in the SAME PR as your code changes
- NEVER assume a column exists — if you add code that uses a new column, add the migration
- Example: migrations/0005_add_my_column.sql containing: ALTER TABLE agent_tasks ADD COLUMN IF NOT EXISTS my_column text;
- If you skip this, QA will auto-fail your PR and the scheduler may crash at runtime

EXECUTING MIGRATIONS (MANDATORY — DO NOT SKIP):
- After creating migration files, you MUST EXECUTE them against the target Supabase project
- Use the Supabase Management API to run SQL:
  curl -s -X POST "https://api.supabase.com/v1/projects/PROJECT_REF/database/query" \\
    -H "Authorization: Bearer $SUPABASE_MGMT_TOKEN" \\
    -H "Content-Type: application/json" \\
    -d '{"query": "YOUR SQL HERE"}'
- Default project ref (tasks.dante.id): lessxkxujvcmublgwdaa
- If the task has a different supabase_project_ref in its app config, use THAT ref instead
- ALWAYS verify the migration ran successfully by querying the table/column afterwards
- Example verification: SELECT column_name FROM information_schema.columns WHERE table_name = 'my_table' AND column_name = 'my_column';
- A migration file that was NOT executed is the same as no migration at all — the app WILL break
- NEVER mark a task as complete if migrations exist but were not executed

Do NOT skip this step. The task board at tasks.dante.id must reflect your work.`;

  try {
    const resp = await fetch(agent.url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${agent.token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        message,
        name: "Task Dispatcher",
        sessionKey: `hook:task:${task.id}`,
        wakeMode: "now",
      }),
    });

    if (resp.ok) {
      console.log(`// Track dispatch in Langfuse
    traceTaskPhase(task, "dispatch", agentName);
    console.log("[OK] Dispatched task ${task.id} ("${task.title}") → ${agentName}`);

      // Track the dispatched task
      activeTasks.set(task.id, {
        agentName,
        dispatchedAt: Date.now(),
        sessionKey: `hook:task:${task.id}`,
      });

      // Update status to in_progress
      await supabase
        .from("agent_tasks")
        .update({ status: "in_progress", started_at: new Date().toISOString(), error: null, result: null, idle_retries: 0 })
        .eq("id", task.id);

      // Archive blocker metadata after successful dispatch
      await archiveBlockerMetadata(task);
    } else {
      const err = await resp.text();
      const errMsg = `Dispatch to ${agentName} failed: HTTP ${resp.status} — ${err.slice(0, 200)}`;
      console.error(`[ERR] ${errMsg}`);
      await supabase.from("agent_tasks").update({ assigned_agent: null }).eq("id", task.id);
      await logTransientError(task.id, errMsg);
    }
  } catch (e) {
    const errMsg = `Dispatch to ${agentName} failed: ${e.message}`;
    console.error(`[ERR] ${errMsg}`);
    await supabase.from("agent_tasks").update({ assigned_agent: null }).eq("id", task.id);
    await logTransientError(task.id, errMsg);
  }
}


// --- QA Auto-Scaler ---
// QA Stale Detection: re-dispatch QA tasks that have been sitting > 20 min with no progress
// Helper: clean up a session on an agent
async function cleanupAgentSession(agentName, sessionKey) {
  const agentConfig = agentName ? AGENTS[agentName.toLowerCase()] : null;
  if (!agentConfig?.url || !agentConfig?.token) return;
  try {
    const deleteUrl = agentConfig.url.replace('/hooks/agent', '/sessions/') + encodeURIComponent(sessionKey);
    const resp = await fetch(deleteUrl, {
      method: 'DELETE',
      headers: { Authorization: `Bearer ${agentConfig.token}` },
    });
    console.log(`[SESSION-CLEANUP] Deleted ${sessionKey} on ${agentName}: ${resp.status}`);
  } catch (e) {
    console.warn(`[SESSION-CLEANUP] Failed to delete ${sessionKey} on ${agentName}: ${e.message}`);
  }
}

async function qaStaleDetector() {
  try {
    const { data: staleTasks, error } = await supabase
      .from("agent_tasks")
      .select("id, title, qa_agent, updated_at, qa_retries")
      .eq("status", "qa_testing")
      .not("qa_agent", "is", null);

    if (error || !staleTasks?.length) return;

    const QA_STALE_THRESHOLD = 20 * 60 * 1000; // 20 minutes
    const MAX_QA_RETRIES = 3;
    for (const task of staleTasks) {
      const age = Date.now() - new Date(task.updated_at).getTime();
      if (age > QA_STALE_THRESHOLD) {
        // Track QA retries to prevent infinite loops
        const retryCount = (task.qa_retries || 0) + 1;
        if (retryCount > MAX_QA_RETRIES) {
          // Move to blocked instead of failed — work is done, just QA couldn't complete
          console.log(`[QA-STALE] Task ${task.id} ("${task.title.slice(0, 40)}") exceeded ${MAX_QA_RETRIES} QA retries → marking BLOCKED (preserving completed work)`);
          await supabase
            .from("agent_tasks")
            .update({ status: "blocked", qa_agent: null, blocked_reason: `QA failed: timed out ${retryCount} times — work is complete but QA could not verify. Needs manual review or re-dispatch.` })
            .eq("id", task.id);
          await logTaskActivity(task.id, 'qa_error', null, `QA timed out ${retryCount} times — moved to blocked for manual review`, 'dispatcher');
          await cleanupAgentSession(task.qa_agent, `hook:qa:${task.id}`);
        } else {
          console.log(`[QA-STALE] Task ${task.id} ("${task.title.slice(0, 40)}") stuck in qa_testing for ${Math.floor(age / 60000)}min → retry ${retryCount}/${MAX_QA_RETRIES}`);
          await supabase
            .from("agent_tasks")
            .update({ qa_agent: null, qa_retries: retryCount })
            .eq("id", task.id);
          await logTransientError(task.id, `QA stale: stuck in qa_testing for ${Math.floor(age / 60000)}min — re-queued (retry ${retryCount}/${MAX_QA_RETRIES})`);
          await cleanupAgentSession(task.qa_agent, `hook:qa:${task.id}`);
        }
      }
    }
  } catch (e) {
    console.error("[QA-STALE] Error:", e.message);
  }
}

async function qaAutoScaler() {
  try {
    // Run stale detection first
    await qaStaleDetector();

    const { data: qaTasks, error } = await supabase
      .from("agent_tasks")
      .select("id")
      .eq("status", "qa_testing")
      .is("qa_agent", null);

    if (error) {
      console.error("[QA-SCALER] Error querying queue:", error.message);
      return;
    }

    const qaQueue = qaTasks?.length || 0;

    const jobListResp = await batchApi.listNamespacedJob({
      namespace: "agents",
      labelSelector: "role=beta-worker",
    });
    const jobList = jobListResp?.body || jobListResp || {};
    // Count truly active jobs (have active pods, not just 'not yet marked failed')
    const activeJobs = (jobList.items || []).filter(
      (j) => !j.status?.succeeded && !j.status?.failed && (j.status?.active || 0) > 0
    );
    const activeWorkers = activeJobs.length;

    // Immediately clean up jobs with no active pods (errored/crashed)
    for (const job of (jobList.items || [])) {
      if (!job.status?.succeeded && !job.status?.failed && (job.status?.active || 0) === 0) {
        const age = Date.now() - new Date(job.metadata.creationTimestamp).getTime();
        if (age > 60000) { // Give 60s grace for init containers
          try {
            await batchApi.deleteNamespacedJob({
              name: job.metadata.name,
              namespace: 'agents',
              propagationPolicy: 'Background',
            });
            console.log(`[QA-SCALER] Cleaned up dead job ${job.metadata.name} (no active pods)`);
          } catch (e) { /* ignore */ }
        }
      }
    }

    console.log(`[QA-SCALER] Queue: ${qaQueue}, Active workers: ${activeWorkers}`);

    if (qaQueue > 1) {
      const desired = Math.min(qaQueue - 1, MAX_QA_WORKERS);
      const toSpawn = desired - activeWorkers;
      for (let i = 0; i < toSpawn; i++) {
        await spawnBetaWorker();
      }
    }

    // Clean up completed/failed jobs older than 5 min
    for (const job of jobList.items || []) {
      if (job.status?.succeeded || job.status?.failed) {
        const finishTime = job.status.completionTime || job.status.conditions?.[0]?.lastTransitionTime;
        if (finishTime && Date.now() - new Date(finishTime).getTime() > 5 * 60 * 1000) {
          try {
            await batchApi.deleteNamespacedJob({
              name: job.metadata.name,
              namespace: "agents",
              propagationPolicy: "Background",
            });
            console.log(`[QA-SCALER] Cleaned up job ${job.metadata.name}`);
          } catch (e) {
            console.error(`[QA-SCALER] Failed to clean up job ${job.metadata.name}:`, e.message);
          }
        }
      }
    }

    await assignQueuedQATasks();
  } catch (e) {
    console.error("[QA-SCALER] Error:", e.message);
  }
}

async function spawnBetaWorker() {
  const workerName = `qa-worker-${Date.now().toString(36)}`;
  const hooksToken = "ephemeral-qa-worker-tok-2026";
  console.log(`[QA-SCALER] Spawning ephemeral QA worker: ${workerName}`);

  const initConfigScript = `
const fs = require('fs');
const config = {
  gateway: {
    port: 18789,
    mode: "local",
    bind: "lan",
    auth: { mode: "token", token: "ephemeral-qa-gw-tok-2026" },
    controlUi: { dangerouslyAllowHostHeaderOriginFallback: true },
    tools: { allow: ["Read", "Write", "Edit", "exec", "web_fetch"] },
  },
  models: {
    providers: {
      openrouter: {
        baseUrl: "https://openrouter.ai/api/v1",
        apiKey: process.env.OPENROUTER_API_KEY || "",
        models: [
          { id: "moonshotai/kimi-k2.5", name: "kimi-k2.5", api: "openai-completions", reasoning: true, input: ["text","image"], cost: { input: 0.45, output: 2.25 }, contextWindow: 262144, maxTokens: 16384 },
        ]
      }
    }
  },
  agents: {
    defaults: {
      model: { primary: "openrouter/moonshotai/kimi-k2.5" },
      workspace: "/root/.openclaw/workspace",
      compaction: { mode: "safeguard" },
      maxConcurrent: 1,
    }
  },
  hooks: { enabled: true, token: "${hooksToken}", allowRequestSessionKey: true, defaultSessionKey: "hook:default", allowedSessionKeyPrefixes: ["hook:"] },
};
fs.mkdirSync('/root/.openclaw/workspace', { recursive: true });
fs.writeFileSync('/root/.openclaw/openclaw.json', JSON.stringify(config, null, 2));
fs.writeFileSync('/root/.openclaw/workspace/AGENTS.md', '# Ephemeral QA Worker\\nYou are a temporary QA agent. Review the task, verify the work, update status, then exit.\\nDo NOT create memory files. You are ephemeral.\\n');
console.log("Config written for ephemeral QA worker");
`;

  const job = {
    apiVersion: "batch/v1",
    kind: "Job",
    metadata: {
      name: workerName,
      namespace: "agents",
      labels: {
        app: "qa-worker",
        role: "beta-worker",
        "managed-by": "task-dispatcher",
      },
    },
    spec: {
      backoffLimit: 0,
      ttlSecondsAfterFinished: 300,
      activeDeadlineSeconds: 1200, // 20 min max
      template: {
        metadata: {
          labels: {
            app: "qa-worker",
            role: "beta-worker",
            "managed-by": "task-dispatcher",
          },
        },
        spec: {
          restartPolicy: "Never",
          initContainers: [
            {
              name: "init-config",
              image: "node:22-bookworm-slim",
              command: ["node", "-e"],
              args: [initConfigScript],
              envFrom: [{ secretRef: { name: "beta-worker-env" } }],
              volumeMounts: [{ name: "workspace", mountPath: "/root/.openclaw" }],
            },
          ],
          containers: [
            {
              name: "openclaw",
              image: process.env.QA_WORKER_IMAGE || "ghcr.io/dante-alpha-assistant/openclaw-agent:latest",
              envFrom: [{ secretRef: { name: "beta-worker-env" } }],
              env: [
                { name: "WORKER_NAME", value: workerName },
                { name: "CHOKIDAR_USEPOLLING", value: "false" },
                { name: "OPENCLAW_NO_WATCH", value: "1" },
                { name: "NODE_OPTIONS", value: "--max-old-space-size=1536" },
              ],
              resources: {
                requests: { cpu: "100m", memory: "1Gi" },
                limits: { cpu: "500m", memory: "2Gi" },
              },
              volumeMounts: [{ name: "workspace", mountPath: "/root/.openclaw" }],
            },
          ],
          volumes: [
            { name: "workspace", emptyDir: {} },
          ],
        },
      },
    },
  };

  try {
    await batchApi.createNamespacedJob({ namespace: "agents", body: job });
    console.log(`[QA-SCALER] Spawned ephemeral QA worker: ${workerName}`);
  } catch (e) {
    console.error(`[QA-SCALER] Failed to spawn QA worker: ${e.message}`);
  }
}


async function assignQueuedQATasks() {
  try {
    const { data: unassigned, error: taskErr } = await supabase
      .from("agent_tasks")
      .select("id, title")
      .eq("status", "qa_testing")
      .is("qa_agent", null)
      .order("created_at", { ascending: true });

    if (taskErr || !unassigned?.length) return;

    const podListResp = await coreApi.listNamespacedPod({
      namespace: "agents",
      labelSelector: "role=beta-worker",
      fieldSelector: "status.phase=Running",
    });
    const podList = podListResp?.body || podListResp || {};

    // Only consider pods running for > 45s (gateway needs time to start)
    const readyPods = (podList.items || []).filter(p => {
      const startTime = p.status?.startTime ? new Date(p.status.startTime).getTime() : Date.now();
      return (Date.now() - startTime) > 150000;
    });
    const runningWorkers = readyPods.map((p) => p.metadata.name);
    const podIpMap = {};
    for (const p of readyPods) {
      if (p.status?.podIP) podIpMap[p.metadata.name] = p.status.podIP;
    }
    if (!runningWorkers.length) {
      console.log('[QA-SCALER] No ready workers (all < 150s old) — will retry next cycle');
      return;
    }

    const { data: assignedTasks } = await supabase
      .from("agent_tasks")
      .select("qa_agent")
      .eq("status", "qa_testing")
      .not("qa_agent", "is", null);

    const busyWorkers = new Set((assignedTasks || []).map((t) => t.qa_agent));
    const freeWorkers = runningWorkers.filter((w) => !busyWorkers.has(w));

    // Pre-fetch QA agent credentials for credential checking (beta-worker is the QA agent)
    const qaAgentCreds = await getAgentCredentials('beta-worker');

    for (let i = 0; i < Math.min(unassigned.length, freeWorkers.length); i++) {
      const task = unassigned[i];
      const worker = freeWorkers[i];

      // Pre-flight credential check for QA on app-scoped tasks
      if (task.app_id) {
        const qaAppCtx = await fetchAppContext(task.id, task.app_id);
        if (qaAppCtx) {
          const credCheck = checkAgentCredentials(qaAppCtx, qaAgentCreds, 'qa');
          if (!credCheck.ok) {
            console.warn(`[DISPATCH] No agent has required credentials for app "${qaAppCtx.name}": missing ${credCheck.missing.join(', ')}`);
            continue;
          }
        }
      }

      const { error: assignErr } = await supabase
        .from("agent_tasks")
        .update({ qa_agent: worker, assigned_agent: worker })
        .eq("id", task.id)
        .is("qa_agent", null);

      if (assignErr) {
        console.error(`[QA-SCALER] Failed to assign task ${task.id} to ${worker}:`, assignErr.message);
        continue;
      }

      console.log(`[QA-SCALER] Assigned task ${task.id} ("${task.title}") to ${worker}`);

      const podIp = podIpMap[worker];
      if (!podIp) { console.warn(`[QA-SCALER] No pod IP for ${worker}, skipping dispatch`); continue; }
      const workerUrl = `http://${podIp}:18789/hooks/agent`;
      try {
        const qaContextBlock = await buildContextBlockWithTimeout(task);
        const qaCommentsBlock = await fetchTaskComments(task.id);
        // Fetch app context for repo validation in QA
        const scalerAppContext = await fetchAppContext(task.id, task.app_id);
        const scalerRepoValidation = scalerAppContext ? buildQaRepoValidation(scalerAppContext) : '';
        // Tiered QA prompt based on task type
        const taskType = task.type || 'general';
        let qaInstructions = '';
        if (taskType === 'coding') {
          const resultStr = typeof task.result === 'string' ? task.result : JSON.stringify(task.result || {});
          const prMatch = resultStr.match(/PR\s*#(\d+)/i);
          const repoMatch = resultStr.match(/(dante-alpha-assistant\/[\w-]+)/);
          qaInstructions = `## QA Review (Coding): ${task.title}

**Task ID:** ${task.id}
**Type:** coding — do a focused code review, NOT a full test suite.

### Checklist (complete in under 3 minutes):
1. **PR exists?** ${prMatch ? `Check PR #${prMatch[1]}${repoMatch ? ` on ${repoMatch[1]}` : ''}` : 'Check task result for PR reference'}
2. **Code scan:** Read the diff. Look for: obvious bugs, missing error handling, hardcoded secrets, broken imports
3. **Does it match the task description?** Compare what was asked vs what was built
4. **No regressions:** Check if the change breaks existing patterns in the codebase

### VERIFY (not just code review):
- For DB changes (triggers, functions, migrations): verify the change EXISTS in the database by querying Supabase REST API
- For API changes: verify the endpoint responds correctly with a test curl
- For UI changes: check the PR diff matches what the task asked for
- If the agent wrote "apply manually" or deferred work → REJECT immediately
- **MIGRATION CHECK (2 parts — BOTH required):**
  1. FILE CHECK: If the PR adds code referencing NEW database columns/tables, check if a migration file exists in the migrations/ folder. If code uses a column that doesn't have a migration file → REJECT with "Missing database migration file for new column(s)"
  2. EXECUTION CHECK: If migration files exist, verify they were ACTUALLY EXECUTED against Supabase. Query the database to confirm tables/columns exist:
     curl -s -X POST "https://api.supabase.com/v1/projects/lessxkxujvcmublgwdaa/database/query" -H "Authorization: Bearer $SUPABASE_MGMT_TOKEN" -H "Content-Type: application/json" -d '{"query": "SELECT column_name FROM information_schema.columns WHERE table_name = '\''TABLE_NAME'\'' AND column_name = '\''COLUMN_NAME'\'';"}'
     If the table/column does NOT exist in the database → REJECT with "Migration file exists but was NOT executed against Supabase"

### DO NOT:
- Clone the repo and try to build it
- Run tests locally
- Spend more than 5 minutes total

### When done:
// MERGE QUEUE: QA agents must NEVER merge PRs directly
- If acceptable: update task status to \`completed\`. DO NOT merge the PR — the merge queue handles merging automatically.
- If issues found: update task status to \`failed\` with specific issues listed`;
        } else if (taskType === 'ops' || taskType === 'review') {
          qaInstructions = `## QA Review (Ops/Config): ${task.title}

**Task ID:** ${task.id}
**Type:** ${taskType} — lightweight verification only.

### Checklist (complete in under 1 minute):
1. **Was the change applied?** Check the task result for confirmation
2. **Any errors in the result?** Look for error messages or warnings
3. **Makes sense?** Does the result match what was requested?

### DO NOT:
- SSH into servers to verify
- Deep-dive into infrastructure
- Spend more than 1 minute

### When done:
- If result looks good: update task status to \`completed\`
- If result shows errors: update task status to \`failed\` with the error`;
        } else {
          qaInstructions = `## QA Review: ${task.title}

**Task ID:** ${task.id}
**Type:** ${taskType}

### Quick review (under 2 minutes):
1. Check the task result — does it match what was requested?
2. Any obvious errors or issues?
3. Update status to \`completed\` if acceptable, \`failed\` if not.`;
        }
        // Add app repo validation to QA instructions if task is app-scoped
        if (scalerRepoValidation) {
          qaInstructions += '\n' + scalerRepoValidation;
        }
        // Add progress comments + Gherkin acceptance criteria instructions
        qaInstructions += `

### ⚠️ MANDATORY: Post Progress Comments During QA

As you complete each check, post a comment to the task so the dashboard shows real-time progress. Do NOT wait until the end.

After each step, post a comment:
\\\`\\\`\\\`bash
curl -s -X POST "https://lessxkxujvcmublgwdaa.supabase.co/rest/v1/task_comments" \\\\
  -H "apikey: \${SUPABASE_SERVICE_ROLE_KEY:-eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imxlc3N4a3h1anZjbXVibGd3ZGFhIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MTM2MTQ2NSwiZXhwIjoyMDg2OTM3NDY1fQ.Wo2WczTauYjpaqtAzfADTSa5htFF6_cKU4UHaJ1EARI}" \\\\
  -H "Authorization: Bearer \${SUPABASE_SERVICE_ROLE_KEY:-eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imxlc3N4a3h1anZjbXVibGd3ZGFhIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MTM2MTQ2NSwiZXhwIjoyMDg2OTM3NDY1fQ.Wo2WczTauYjpaqtAzfADTSa5htFF6_cKU4UHaJ1EARI}" \\\\
  -H "Content-Type: application/json" \\\\
  -d '{"task_id":"${task.id}","author":"beta-worker","author_type":"agent","body":"✅ Step N: DESCRIPTION OF WHAT YOU CHECKED AND RESULT"}'
\\\`\\\`\\\`

**Example comments:**
- "✅ PR #1 exists — 47 files, feat/project-setup branch"
- "✅ Code scan — no hardcoded secrets, proper error handling"
- "⚠️ Missing migration for new contacts.phone column"
- "❌ Task asked for Docker setup but no Dockerfile found"

Post at LEAST 2-3 comments during QA. This is how the team knows you're working, not stuck.

### ⚠️ MANDATORY: Write Gherkin Acceptance Criteria

Before updating task status, you MUST generate Gherkin acceptance criteria (Given/When/Then scenarios) based on the task description and result. Write them to the \`acceptance_criteria\` field using a PATCH request.

**DO NOT write Gherkin scenarios to the \`description\` field. The original description must NEVER be overwritten.**

Generate realistic Gherkin scenarios, then PATCH the task with acceptance_criteria BEFORE updating the final task status.`;
        const qaMessage = `${qaContextBlock}\n${qaInstructions}\n${qaCommentsBlock ? qaCommentsBlock : ""}`;
        await fetch(workerUrl, {
          method: "POST",
          headers: { "Content-Type": "application/json", "Authorization": "Bearer ephemeral-qa-gw-tok-2026" },
          body: JSON.stringify({
            message: qaMessage,
            name: "Task Dispatcher (QA Auto-Scaler)",
            sessionKey: `hook:qa:${task.id}`,
            wakeMode: "now",
          }),
        });
        console.log(`[QA-SCALER] Dispatched QA review to ${worker} for task ${task.id}`);
      } catch (e) {
        console.error(`[QA-SCALER] Failed to dispatch to ${worker}: ${e.message}`);
      }
    }
  } catch (e) {
    console.error("[QA-SCALER] Assignment error:", e.message);
  }
}

// --- Subscribe to Realtime changes ---
function subscribe() {
  console.log("[BOOT] Task Dispatcher starting...");

initLangfuse();
  console.log(`[BOOT] Agents: ${Object.keys(AGENTS).join(", ")}`);

  const channel = supabase
    .channel("agent-tasks-changes")
    .on(
      "postgres_changes",
      {
        event: "*",
        schema: "public",
        table: "agent_tasks",
      },
      async (payload) => {
        const { eventType, new: task, old: prev } = payload;
        console.log(`[EVENT] ${eventType} on task ${task?.id || prev?.id}: status=${task?.status}`);

        // Guard: never process further if old status was deprecated (terminal state)
        if (prev?.status === 'deprecated') {
          console.log(`[GUARD] Task ${task?.id} was deprecated — ignoring transition to ${task?.status}`);
          return;
        }

        // === LANGFUSE: Track phase transitions ===
        if (eventType === 'UPDATE' && task && prev && task.status !== prev.status) {
          const phaseMap = {
            'in_progress': 'coding',
            'qa_testing': 'qa_review', 
            'completed': 'completed',
            'deployed': 'deployed',
            'failed': 'failed',
            'blocked': 'blocked',
          };
          const phase = phaseMap[task.status];
          if (phase) {
            
            // Auto-deploy: when an app-factory task completes, check if ALL sibling tasks are done
            // If so, create a batch deploy task (same as "Deploy All") to merge PRs properly
            if (task.status === 'completed' && task.dispatched_by === 'app-factory' && task.app_id) {
              console.log(`[AUTO-DEPLOY] Task ${task.id} completed by app-factory — checking siblings`);
              setTimeout(async () => {
                try {
                  const { data: siblings, error: sibErr } = await supabase
                    .from('agent_tasks')
                    .select('id, title, status, pull_request_url, repository_url, deploy_target, type')
                    .eq('app_id', task.app_id)
                    .eq('dispatched_by', 'app-factory')
                    .neq('type', 'deploy');

                  if (sibErr) { console.error(`[AUTO-DEPLOY] Sibling fetch error: ${sibErr.message}`); return; }
                  if (!siblings || siblings.length === 0) return;

                  const completedTasks = siblings.filter(t => t.status === 'completed');
                  const pendingTasks = siblings.filter(t => !['completed','deployed','deploying','failed'].includes(t.status));

                  console.log(`[AUTO-DEPLOY] App ${task.app_id}: ${completedTasks.length} completed, ${pendingTasks.length} pending, ${siblings.length} total`);

                  if (pendingTasks.length > 0 || completedTasks.length === 0) {
                    console.log(`[AUTO-DEPLOY] Not all tasks done yet — waiting (${pendingTasks.length} pending)`);
                    return;
                  }

                  // Guard: deploy task already exists for this app?
                  const { data: existingDeploy } = await supabase
                    .from('agent_tasks')
                    .select('id, status')
                    .eq('app_id', task.app_id)
                    .eq('type', 'deploy')
                    .in('status', ['todo', 'in_progress', 'deploying']);
                  
                  if (existingDeploy && existingDeploy.length > 0) {
                    console.log(`[AUTO-DEPLOY] Deploy task already exists for app ${task.app_id} — skipping`);
                    return;
                  }

                  const getPrUrl = (t) => Array.isArray(t.pull_request_url) ? t.pull_request_url[0] : t.pull_request_url;
                  const deployable = completedTasks.filter(t => getPrUrl(t));

                  if (deployable.length === 0) {
                    console.warn(`[AUTO-DEPLOY] All tasks completed but none have PRs`);
                    return;
                  }

                  const byRepo = {};
                  for (const t of deployable) {
                    const prUrl = getPrUrl(t);
                    const match = prUrl ? prUrl.match(/github\.com\/([^/]+\/[^/]+)\/pull/) : null;
                    const repo = match ? match[1] : t.repository_url || 'unknown';
                    if (!byRepo[repo]) byRepo[repo] = [];
                    byRepo[repo].push({
                      id: t.id, title: t.title, pr_url: prUrl,
                      pr_number: prUrl ? (prUrl.match(/\/pull\/(\d+)/) || [])[1] : null,
                      deploy_target: t.deploy_target || 'vercel',
                    });
                  }

                  const { data: deployTask, error: createErr } = await supabase
                    .from('agent_tasks')
                    .insert({
                      title: `Deploy — ${deployable.length} PRs for app ${task.app_id.slice(0,8)} [${new Date().toISOString().slice(0,16)}]`,
                      type: 'deploy',
                      priority: 'urgent',
                      status: 'todo',
                      app_id: task.app_id,
                      deploy_target: deployable[0].deploy_target || 'vercel',
                      dispatched_by: 'app-factory',
                      repository_url: deployable[0].repository_url,
                      description: 'Merge and deploy ' + deployable.length + ' PRs sequentially:\n\n' + deployable.map(t => '- ' + t.title + ' (' + getPrUrl(t) + ')').join('\n'),
                      metadata: {
                        batch_tasks: deployable.map(t => ({ id: t.id, title: t.title, pr_url: getPrUrl(t) })),
                        repos: byRepo,
                        strategy: 'sequential_rebase',
                      },
                    })
                    .select()
                    .single();

                  if (createErr) { console.error(`[AUTO-DEPLOY] Deploy task create error: ${createErr.message}`); return; }

                  const relationships = deployable.map(t => ({
                    source_task_id: t.id,
                    target_task_id: deployTask.id,
                    relationship_type: 'deployed_by',
                    created_by: 'auto-deploy',
                  }));
                  await supabase.from('task_relationships').insert(relationships);

                  await supabase.from('agent_tasks')
                    .update({ status: 'deploying', updated_at: new Date().toISOString() })
                    .in('id', deployable.map(t => t.id));

                  for (const t of deployable) {
                    await logTaskActivity(t.id, 'status', 'completed', 'deploying', 'auto-deploy');
                  }

                  console.log(`[AUTO-DEPLOY] Created deploy task ${deployTask.id} for ${deployable.length} PRs`);
                  await logTaskActivity(deployTask.id, 'status', null, 'todo', 'auto-deploy');
                } catch (e) {
                  console.error(`[AUTO-DEPLOY] Error: ${e.message}`);
                }
              }, 5000);
            }

            // Auto-detect merge conflict failures and set rebase metadata
            if (task.status === "failed") {
              detectAndSetRebaseMetadata(task).catch(() => {});
              // Auto-detect blocker patterns from error text → convert failed to blocked
              autoDetectBlocker(task).catch((e) => console.error(`[AUTO-BLOCKER] Error for task ${task.id}:`, e.message));
              // Auto-retry: QA failed coding tasks go back to todo for another coding agent
              // Limited to 2 auto-retries to prevent infinite loops
              if (prev?.status === 'qa_testing' && task.type === 'coding' && task.qa_result && task.qa_result.passed === false) {
                const qaRetries = task.qa_retries || 0;
                if (qaRetries < 2) {
                  console.log(`[QA-RETRY] Task ${task.id} QA failed (attempt ${qaRetries + 1}/2) — sending back to todo for coding fix`);
                  const qaFeedback = task.qa_result.failures ? task.qa_result.failures.join('; ') : 'QA review failed';
                  supabase.from('agent_tasks').update({
                    status: 'todo',
                    assigned_agent: null,
                    qa_agent: null,
                    started_at: null,
                    completed_at: null,
                    result: null,
                    qa_result: null,
                    
                    qa_retries: qaRetries + 1,
                    error: null,
                  }).eq('id', task.id).then(() => {
                    logTransientError(task.id, `QA failed (attempt ${qaRetries + 1}): ${qaFeedback.slice(0, 500)}`);
                    logTaskActivity(task.id, 'qa_retry', null, `QA failed — auto-retrying (attempt ${qaRetries + 1}/2): ${qaFeedback.slice(0, 200)}`, 'dispatcher');
                  }).catch(e => console.error(`[QA-RETRY] Failed to retry task ${task.id}:`, e.message));
                } else {
                  console.log(`[QA-RETRY] Task ${task.id} QA failed ${qaRetries + 1} times — staying failed for manual review`);
                }
              }

              // Auto-triage: classify the error and auto-retry retriable categories (timeout, session_lost)
              const errorText = task.error || '';
              const errorClassification = classifyError(errorText);
              if (errorClassification.retriable && errorClassification.action === 'auto_retry') {
                const retryCount = (task.metadata?.auto_retries || 0);
                if (retryCount < 3) {
                  console.log(`[AUTO-TRIAGE] Task ${task.id} error classified as ${errorClassification.category} (retry ${retryCount + 1}/3) — auto-retrying`);
                  supabase.from('agent_tasks').update({
                    status: 'todo',
                    assigned_agent: null,
                    started_at: null,
                    completed_at: null,
                    error: null,
                    last_failed_agent: task.assigned_agent || task.last_failed_agent,
                    metadata: { ...(task.metadata || {}), auto_retries: retryCount + 1, last_error_category: errorClassification.category },
                  }).eq('id', task.id).then(() => {
                    logTaskActivity(task.id, 'auto_triage', null, `Error classified as ${errorClassification.category} — auto-retrying (attempt ${retryCount + 1}/3)`, 'dispatcher');
                  }).catch(e => console.error(`[AUTO-TRIAGE] Failed to retry task ${task.id}:`, e.message));
                } else {
                  console.log(`[AUTO-TRIAGE] Task ${task.id} ${errorClassification.category} — max retries (3) reached, staying failed`);
                  logTaskActivity(task.id, 'auto_triage', null, `Error classified as ${errorClassification.category} — max retries (3) exhausted`, 'dispatcher').catch(() => {});
                }
              }
            }
            const phaseAgent = task.assigned_agent || prev?.assigned_agent || task.qa_agent || 'system';
            traceTaskPhase(task, phase, phaseAgent);
            // Record phase timing with actual usage from agent gateway
            const phaseStarted = task.started_at || task.updated_at;
            const durationMs = phaseStarted ? Date.now() - new Date(phaseStarted).getTime() : 0;
            // Delay cost enrichment by 4s: the final Claude API call (which triggered the status change)
            // may not be tallied in sessions_list yet — wait for the gateway to flush token counts.
            // NOTE: closeAgentSession is intentionally moved INSIDE the setTimeout so we capture
            // token counts BEFORE closing the session (otherwise the session may archive before query).
            {
              const costTaskId = task.id;
              const costPrevStatus = prev?.status;
              const costAgent = prev?.assigned_agent || task.assigned_agent;
              const costDurationMs = durationMs;
              const costIsSessionPhase = ['in_progress', 'qa_testing'].includes(prev?.status);
              setTimeout(async () => {
                let phaseModel = 'unknown';
                let phaseInputTokens = 0;
                let phaseOutputTokens = 0;
                let phaseCost = 0;
                try {
                  const costConfig = costAgent ? AGENTS[costAgent] : null;
                  if (costConfig?.gatewayToken) {
                    const isQa = costPrevStatus === 'qa_testing';
                    const sessKey = isQa ? `agent:main:hook:qa:${costTaskId}` : `agent:main:hook:task:${costTaskId}`;
                    const gwUrl = costConfig.url.replace(/\/hooks\/agent$/, "");
                    console.log(`[COST] Querying sessions_list for task ${costTaskId} phase ${costPrevStatus} (agent: ${costAgent})`);
                    const sessResp = await fetch(`${gwUrl}/tools/invoke`, {
                      method: 'POST',
                      headers: { Authorization: `Bearer ${costConfig.gatewayToken}`, 'Content-Type': 'application/json' },
                      body: JSON.stringify({ tool: 'sessions_list', params: { limit: 50, messageLimit: 0 } }),
                      signal: AbortSignal.timeout(8000),
                    });
                    if (sessResp.ok) {
                      const sessData = await sessResp.json();
                      // sessions_list returns result.details.sessions (NOT result.content[0].text)
                      const sessions = sessData?.result?.details?.sessions || [];
                      console.log(`[COST] sessions_list returned ${sessions.length} sessions for task ${costTaskId}`);
                      const sess = sessions.find(s => s.key === sessKey);
                      if (sess) {
                        phaseModel = sess.model || 'unknown';
                        // Try separate inputTokens/outputTokens fields first (OpenRouter / kimi-k2.5 reports these separately)
                        // Fall back to totalTokens with 80/20 split approximation (Anthropic sessions)
                        const rawInput = sess.inputTokens || (sess.usage && sess.usage.inputTokens) || 0;
                        const rawOutput = sess.outputTokens || (sess.usage && sess.usage.outputTokens) || 0;
                        if (rawInput > 0 || rawOutput > 0) {
                          phaseInputTokens = rawInput;
                          phaseOutputTokens = rawOutput;
                        } else {
                          const total = sess.totalTokens || 0;
                          phaseInputTokens = Math.round(total * 0.8);
                          phaseOutputTokens = total - phaseInputTokens;
                        }
                        if (phaseInputTokens === 0 && phaseOutputTokens === 0) {
                          console.warn(`[COST] Task ${costTaskId}: session found but 0 tokens. Full session data: ${JSON.stringify(sess)}`);
                        }
                        const pricing = {
                          'claude-sonnet-4-6': { input: 3.0, output: 15.0 },
                          'claude-opus-4-6': { input: 15.0, output: 75.0 },
                          'kimi-k2.5': { input: 0.45, output: 2.25 },
                          'gemini-2.5-pro': { input: 1.25, output: 10.0 },
                          'gemini-2.0-flash': { input: 0.1, output: 0.4 },
                        };
                        const modelKey = phaseModel.replace(/^.*\//, '');
                        const price = pricing[modelKey] || { input: 1.0, output: 5.0 };
                        phaseCost = (phaseInputTokens / 1_000_000 * price.input) + (phaseOutputTokens / 1_000_000 * price.output);
                        console.log(`[COST] Task ${costTaskId} phase ${costPrevStatus}: model=${phaseModel} in=${phaseInputTokens} out=${phaseOutputTokens} cost=$${phaseCost.toFixed(4)}`);
                      } else {
                        console.log(`[COST] Session not found: ${sessKey} (task ${costTaskId}, ${sessions.length} sessions available)`);
                      }
                    } else {
                      console.warn(`[COST] sessions_list HTTP ${sessResp.status} for task ${costTaskId}`);
                    }
                  } else {
                    console.log(`[COST] No gateway config for agent ${costAgent} — skipping cost enrichment for task ${costTaskId}`);
                  }
                } catch (e) {
                  console.error(`[COST] Cost enrichment error for task ${costTaskId}: ${e.message}`);
                }
                recordPhaseCost(supabase, costTaskId, costPrevStatus || 'unknown', {
                  model: phaseModel,
                  inputTokens: phaseInputTokens,
                  outputTokens: phaseOutputTokens,
                  durationMs: costDurationMs,
                  cost: phaseCost,
                }).catch((e) => console.error(`[COST] recordPhaseCost failed for task ${costTaskId}: ${e.message}`));
                // Close session AFTER cost is captured to prevent session archiving before token query
                if (costIsSessionPhase && costAgent) {
                  closeAgentSession(costAgent, costTaskId, costPrevStatus === 'qa_testing').catch(() => {});
                }
              }, 4000);
            }
          }
        }

        // Broadcast status change to SSE clients
        if (task) {
          broadcast("task:status", {
            taskId: task.id,
            status: task.status,
            previousStatus: prev?.status || null,
            agent: task.assigned_agent,
            title: task.title,
            timestamp: new Date().toISOString(),
          }, task.id);
        }

        // Dispatch when:
        // 1. Task has assigned_agent set and status is todo (scheduler assigned it)
        // Dispatch condition: assigned_agent just set (was null → agent name)
        const agentJustAssigned = task?.assigned_agent && eventType === "UPDATE" && !prev?.assigned_agent;
        // Skip qa_testing tasks here — they're handled by the QA dispatch block below
        if (agentJustAssigned && task?.assigned_agent && task?.status !== "qa_testing") {
          // Concurrency guard: check if this agent already has an in-progress task
          const agentName = task.assigned_agent.toLowerCase();
          const { data: inFlight } = await supabase
            .from('agent_tasks')
            .select('id')
            .eq('assigned_agent', task.assigned_agent)
            .in('status', ['in_progress'])
            .neq('id', task.id);
          // Check capacity from agent_cards (default 1 for backward compat)
          const { data: agentCard } = await supabase
            .from('agent_cards')
            .select('max_capacity')
            .eq('name', task.assigned_agent)
            .single();
          const maxCapacity = agentCard?.max_capacity || 1;
          if (inFlight && inFlight.length >= maxCapacity) {
            const reason = `Agent ${agentName} at capacity (${inFlight.length}/${maxCapacity} in-progress tasks), unassigning for re-dispatch`;
            console.log(`[DISPATCH] ${reason}`);
            await supabase.from('agent_tasks').update({
              assigned_agent: null,
              
            }).eq('id', task.id);
            await logTransientError(task.id, reason);
          } else {
            // Check dependencies before dispatching
            const depsResult = await areDependenciesMet(task.id, { detailed: true });
            if (!depsResult.met) {
              const unmetNames = depsResult.unmet.map(d => `"${d.title}" (${d.status})`).join(', ');
              const reason = `Unmet dependencies: ${unmetNames} — unassigning task`;
              console.log(`[DISPATCH] ${reason}`);
              await supabase.from('agent_tasks').update({
                assigned_agent: null,
                error: `Blocked by unmet dependencies: ${unmetNames}`,
              }).eq('id', task.id);
              await logTransientError(task.id, reason);
            } else {
              console.log(`[DISPATCH] Task ${task.id} → ${task.assigned_agent}`);
              dispatchToAgent(task);
            }
          }
        }

        // Dependency check on qa_testing transition
        if (task && task.status === 'qa_testing' && eventType === 'UPDATE' && prev?.status && prev.status !== task.status) {
          const depsResult = await areDependenciesMet(task.id, { detailed: true });
          if (!depsResult.met) {
            const unmetNames = depsResult.unmet.map(d => `"${d.title}" (${d.status})`).join(', ');
            console.log(`[DEPS] Task ${task.id} moved to qa_testing but has unmet dependencies: ${unmetNames} — reverting to blocked`);
            await supabase.from('agent_tasks').update({
              status: 'blocked',
              blocked_reason: `Unmet dependencies: ${unmetNames}`,
              error: `Cannot proceed — dependencies not met: ${unmetNames}`,
            }).eq('id', task.id);
            await logTransientError(task.id, `qa_testing blocked by unmet dependencies: ${unmetNames}`);
            return; // Skip further processing for this event
          }
        }

        // When task transitions to qa_testing: clear assigned_agent
        // The coding agent is done — assigned_agent should reflect current owner (nobody until scheduler assigns QA agent)
        if (task && task.status === 'qa_testing' && eventType === 'UPDATE' && prev?.status && prev.status !== task.status) {
          // App-factory tasks skip QA — auto-advance to completed (auto-deploy will pick it up from there)
          if (task.assigned_agent) {
            console.log(`[UNASSIGN] Task ${task.id} → qa_testing, clearing assigned_agent (was: ${task.assigned_agent}), resetting started_at for QA timeout`);
            await supabase
              .from('agent_tasks')
              .update({ assigned_agent: null, started_at: null, error: null })
              .eq('id', task.id);
          }
        }

        // === COMPLETED LOCK: When task reaches completed, clear QA agent and error to prevent post-completion changes ===
        if (task && task.status === 'completed' && eventType === 'UPDATE' && prev?.status !== 'completed') {
          const lockUpdates = { error: null }; // Clear transient errors — task is done
          if (task.qa_agent) lockUpdates.qa_agent = null;
          // Don't clear assigned_agent here — deploy needs to know who worked on it
          console.log(`[COMPLETED-LOCK] Task ${task.id} reached completed — clearing qa_agent + error`);
          await supabase.from('agent_tasks').update(lockUpdates).eq('id', task.id);
        }


        // ===== BATCH DEPLOY COMPLETION =====
        // When a deploy task completes, mark all its batch subtasks as deployed
        if (task && task.type === 'deploy' && (task.status === 'completed' || task.status === 'deployed') && eventType === 'UPDATE' && prev?.status !== 'completed' && prev?.status !== 'deployed') {
          const batchTasks = task.metadata?.batch_tasks;
          if (batchTasks && batchTasks.length > 0) {
            const taskIds = batchTasks.map(t => t.id);
            console.log(`[BATCH_DEPLOY] Deploy task ${task.id} completed — marking ${taskIds.length} subtasks as deployed`);
            
            // For Vercel deploy targets, resolve the actual deployment URL
            const deployTarget = task.deploy_target || 'kubernetes';
            let vercelUrlsByRepo = {};
            if (deployTarget === 'vercel') {
              const repos = task.metadata?.repos || {};
              for (const repoName of Object.keys(repos)) {
                try {
                  const url = await getVercelDeploymentUrl(repoName, 'main');
                  if (url) {
                    vercelUrlsByRepo[repoName] = url;
                    console.log(`[BATCH_DEPLOY] Vercel URL for ${repoName}: ${url}`);
                  }
                } catch (err) {
                  console.error(`[BATCH_DEPLOY] Failed to get Vercel URL for ${repoName}:`, err.message);
                }
              }
            }

            // Also mark the deploy task itself as deployed (not just completed)
            const deployTaskUpdate = { status: 'deployed', updated_at: new Date().toISOString() };
            // Set deployment_url on the deploy task if we found a Vercel URL
            const allVercelUrls = Object.values(vercelUrlsByRepo);
            if (allVercelUrls.length > 0) {
              deployTaskUpdate.deployment_url = allVercelUrls[0]; // Primary URL
            }
            await supabase.from('agent_tasks')
              .update(deployTaskUpdate)
              .eq('id', task.id);
              
            // Bypass trigger to set deployed status + deployment URL on subtasks
            for (const tid of taskIds) {
              const subtaskUpdate = { status: 'deployed', updated_at: new Date().toISOString() };
              
              // Find the repo for this subtask and set its Vercel URL
              if (deployTarget === 'vercel') {
                const repos = task.metadata?.repos || {};
                for (const [repoName, repoTasks] of Object.entries(repos)) {
                  if (repoTasks.some(rt => rt.id === tid) && vercelUrlsByRepo[repoName]) {
                    subtaskUpdate.deployment_url = vercelUrlsByRepo[repoName];
                    break;
                  }
                }
              }
              
              await supabase.from('agent_tasks')
                .update(subtaskUpdate)
                .eq('id', tid)
                .in('status', ['deploying', 'completed']); // Update deploying or completed (if deploying transition was blocked)
            }
          }
        }

        // ===== VERCEL DEPLOYMENT URL RESOLVER =====
        // When any task transitions to "deployed" with deploy_target "vercel" and no deployment_url,
        // resolve the Vercel URL from the PR/repo info. Falls back to the predictable
        // https://{repo-name}.vercel.app pattern when the Vercel API is unavailable.
        if (task?.status === 'deployed' && eventType === 'UPDATE' && prev?.status !== 'deployed' 
            && task.deploy_target === 'vercel' && !task.deployment_url) {
          try {
            // 1. Try to extract repo from PR URL
            const prUrl = Array.isArray(task.pull_request_url) ? task.pull_request_url[0] : task.pull_request_url;
            let repoFullName = prUrl?.match(/github\.com\/([^/]+\/[^/]+)/)?.[1];

            // 2. Fall back to repository_url field
            if (!repoFullName && task.repository_url) {
              const m = task.repository_url.match(/github\.com\/([^/]+\/[^/]+?)(?:\.git)?$/);
              repoFullName = m?.[1];
            }

            // 3. Fall back to task result artifacts (coding agent stores PR URLs there)
            if (!repoFullName && task.result?.artifacts?.length) {
              for (const artifact of task.result.artifacts) {
                const artifactUrl = artifact?.url || artifact;
                const m = String(artifactUrl).match(/github\.com\/([^/]+\/[^/]+)/);
                if (m) { repoFullName = m[1]; break; }
              }
            }

            // 4. Fall back to task result summary string
            if (!repoFullName && task.result?.summary) {
              const m = task.result.summary.match(/github\.com\/([^/]+\/[^/]+)/);
              if (m) repoFullName = m[1];
            }

            if (repoFullName) {
              // Remove trailing path segments (e.g. /pull/123)
              repoFullName = repoFullName.replace(/\/pull\/.*/, '').replace(/\/tree\/.*/, '');
              const vercelUrl = await getVercelDeploymentUrl(repoFullName, 'main');
              if (vercelUrl) {
                console.log(`[VERCEL_RESOLVER] Setting deployment_url for task ${task.id}: ${vercelUrl}`);
                await supabase.from('agent_tasks')
                  .update({ deployment_url: vercelUrl, updated_at: new Date().toISOString() })
                  .eq('id', task.id);
              } else {
                console.warn(`[VERCEL_RESOLVER] Could not resolve URL for task ${task.id} (repo: ${repoFullName})`);
              }
            } else {
              console.warn(`[VERCEL_RESOLVER] Cannot determine repo for task ${task.id} — no PR URL, repository_url, or result artifacts`);
            }
          } catch (err) {
            console.error(`[VERCEL_RESOLVER] Error resolving URL for task ${task.id}:`, err.message);
          }
        }

        // ===== QA COMPLETION VALIDATOR =====
        // When a coding task moves to 'completed', verify the QA actually reviewed code.
        // Revert to qa_testing if the QA agent rubber-stamped without PR review.
        if (task?.status === 'completed' && eventType === 'UPDATE' && prev?.status === 'qa_testing' && task.type === 'coding') {
          const qaResult = task.qa_result;
          const hasPR = task.pull_request_url && task.pull_request_url.length > 0;
          const qaNotesLower = (qaResult?.notes || '').toLowerCase();
          const rubberStamped = !hasPR && (
            qaNotesLower.includes('no pr') || 
            qaNotesLower.includes('unable to perform code') ||
            qaNotesLower.includes('cannot perform code') ||
            qaNotesLower.includes('no pull request')
          );
          
          if (rubberStamped) {
            console.log(`[QA-GUARD] Task ${task.id} ("${task.title.slice(0,40)}") was rubber-stamped without PR review — reverting to qa_testing`);
            await supabase.from('agent_tasks').update({ 
              status: 'qa_testing', 
              assigned_agent: null, 
              qa_agent: null, 
              qa_result: null,
              qa_retries: (task.qa_retries || 0) + 1,
              error: null,
              started_at: null
            }).eq('id', task.id);
            await logTransientError(task.id, 'QA rejected: coding task completed without PR code review. QA must verify actual code changes.');
            return; // Don't process further
          }
        }


        // Blocked task alert: notify Dante via Discord when a task moves to blocked
        if (task?.status === 'blocked' && eventType === 'UPDATE' && prev?.status !== 'blocked') {
          console.log(`[BLOCKED] Task ${task.id} ("${task.title}") moved to blocked by ${task.assigned_agent || 'unknown'}`);
          await notifyBlockedTask(task);
          // Also remove from active tracking
          if (activeTasks.has(task.id)) {
            console.log(`[TRACKER] Task ${task.id} blocked, removing from active tracking`);
            activeTasks.delete(task.id);
          }
        }


        // Remove completed/failed tasks from active tracking
        if (task?.status === 'qa_testing' || task?.status === 'failed' || task?.status === 'completed' || task?.status === 'deployed' || task?.status === 'deprecated') {
          if (activeTasks.has(task.id)) {
            console.log(`[TRACKER] Task ${task.id} completed (${task.status}), removing from active tracking`);
            activeTasks.delete(task.id);
          }
          // A2A: check if parent task should be completed
          await checkParentCompletion(task);
        }

        // Handle task deprecated: treat as terminal, close session, remove from tracking
        if (task?.status === 'deprecated' && eventType === 'UPDATE' && prev?.status !== 'deprecated') {
          console.log(`[DEPRECATED] Task ${task.id} ("${task.title}") deprecated from ${prev?.status} — treating as terminal`);
          // Remove from active tracking
          if (activeTasks.has(task.id)) {
            activeTasks.delete(task.id);
            console.log(`[TRACKER] Task ${task.id} deprecated, removed from active tracking`);
          }
          // Close agent session to free the lane
          const deprecatedAgent = prev?.assigned_agent || task.assigned_agent;
          if (deprecatedAgent) {
            closeAgentSession(deprecatedAgent, task.id, prev?.status === 'qa_testing').catch(() => {});
            console.log(`[DEPRECATED] Closing session for agent ${deprecatedAgent}, task ${task.id}`);
          }
        }

        // Auto-cleanup QA sessions: when task leaves qa_testing, delete the QA session on the agent
        if (prev?.status === 'qa_testing' && task?.status !== 'qa_testing') {
          const qaAgentName = prev.assigned_agent || task.assigned_agent;
          const qaAgentConfig = qaAgentName ? AGENTS[qaAgentName.toLowerCase()] : null;
          if (qaAgentConfig?.url && qaAgentConfig?.token) {
            const sessionKey = `hook:qa:${task.id}`;
            try {
              const deleteUrl = qaAgentConfig.url.replace('/hooks/agent', '/sessions/') + encodeURIComponent(sessionKey);
              const delResp = await fetch(deleteUrl, {
                method: 'DELETE',
                headers: { Authorization: `Bearer ${qaAgentConfig.token}` },
              });
              console.log(`[QA-CLEANUP] Deleted QA session ${sessionKey} on ${qaAgentName}: ${delResp.status}`);
            } catch (e) {
              console.warn(`[QA-CLEANUP] Failed to delete QA session on ${qaAgentName}: ${e.message}`);
            }
          }
        }


        // Auto-cleanup coding sessions: when task leaves in_progress, delete the task session
        if (prev?.status === 'in_progress' && task?.status !== 'in_progress') {
          const codingAgent = prev.assigned_agent || task.assigned_agent;
          const codingConfig = codingAgent ? AGENTS[codingAgent.toLowerCase()] : null;
          if (codingConfig?.url && codingConfig?.token) {
            const sessionKey = `hook:task:${task.id}`;
            try {
              const deleteUrl = codingConfig.url.replace('/hooks/agent', '/sessions/') + encodeURIComponent(sessionKey);
              const delResp = await fetch(deleteUrl, {
                method: 'DELETE',
                headers: { Authorization: `Bearer ${codingConfig.token}` },
              });
              console.log(`[TASK-CLEANUP] Deleted task session ${sessionKey} on ${codingAgent}: ${delResp.status}`);
            } catch (e) {
              console.warn(`[TASK-CLEANUP] Failed to delete task session on ${codingAgent}: ${e.message}`);
            }
          }
        }
        // Auto-comment: when an agent writes a result, post it as a comment in the task thread
        if (eventType === 'UPDATE' && task?.result && !prev?.result) {
          try {
            // Determine the agent who produced this result.
            // Note: the completing agent often clears assigned_agent in the same PATCH that sets result,
            // and Supabase Realtime 'old' record may not include assigned_agent (depends on replica identity).
            // Fallback: query the task_activity log for the last agent assignment.
            let agent = task.assigned_agent || prev?.assigned_agent || task.qa_agent || null;
            if (!agent) {
              try {
                const { data: activity } = await supabase
                  .from('task_activity_log')
                  .select('changed_by')
                  .eq('task_id', task.id)
                  .eq('field', 'status')
                  .neq('changed_by', 'dispatcher')
                  .order('changed_at', { ascending: false })
                  .limit(1)
                  .single();
                agent = activity?.changed_by || 'system';
              } catch {
                agent = 'system';
              }
            }
            const result = typeof task.result === 'object' ? task.result : (() => { try { return JSON.parse(task.result); } catch { return null; } })();
            let commentBody = '📋 **Task Result**\n\n';
            if (result?.summary) {
              commentBody += result.summary;
            } else {
              commentBody += typeof task.result === 'string' ? task.result : JSON.stringify(task.result, null, 2);
            }
            if (result?.artifacts?.length) {
              commentBody += '\n\n📎 **Artifacts:** ' + result.artifacts.map(a => typeof a === 'string' ? a : (a.url || JSON.stringify(a))).join(', ');
            }
            if (result?.test_results) {
              commentBody += '\n\n🧪 **Test Results:** ' + (typeof result.test_results === 'string' ? result.test_results : JSON.stringify(result.test_results));
            }
            if (task.pull_request_url?.length) {
              commentBody += '\n\n🔗 **PR:** ' + task.pull_request_url.join(', ');
            }
            // Show artifact links (repos, URLs) if available
            if (result?.artifacts?.length) {
              const repoArtifacts = result.artifacts.filter(a => a?.type === 'github_repo' || a?.type === 'repo');
              const otherArtifacts = result.artifacts.filter(a => a?.type !== 'github_repo' && a?.type !== 'repo');
              if (repoArtifacts.length) {
                commentBody += '\n📦 **Repository:** ' + repoArtifacts.map(a => a.url || a).join(', ');
              }
              if (otherArtifacts.length && !task.pull_request_url?.length) {
                commentBody += '\n🔗 **Links:** ' + otherArtifacts.map(a => a.url || a).join(', ');
              }
            } else if (task.repository_url) {
              commentBody += '\n\n📦 **Repository:** ' + task.repository_url;
            }
            const { error: commentErr } = await supabase
              .from('task_comments')
              .insert({ task_id: task.id, author: agent, author_type: 'agent', body: commentBody });
            if (commentErr) {
              console.error(`[AUTO-COMMENT] Failed to post comment for task ${task.id}: ${commentErr.message}`);
            } else {
              console.log(`[AUTO-COMMENT] Posted result as comment for task ${task.id} by ${agent}`);
            }
          } catch (e) {
            console.error(`[AUTO-COMMENT] Error: ${e.message}`);
          }
        }

        // Factory pipeline stage transitions: auto-advance to next stage
        // Note: factory tasks still use "done" internally for stage transitions before final QA
        if (task?.status === "qa_testing" && eventType === "UPDATE" && prev?.status && prev.status !== task.status && task.stage) {
          const nextStage = getNextStage(task.stage);
          if (nextStage) {
            // Guard: only advance forward (check current stage is valid and not final)
            const currentIdx = STAGE_PIPELINE.indexOf(task.stage);
            const nextIdx = STAGE_PIPELINE.indexOf(nextStage);
            if (nextIdx > currentIdx) {
              console.log(`[STAGE] Task ${task.id} ("${task.title}"): ${task.stage} → ${nextStage}`);
              await supabase.from("agent_tasks").update({
                stage: nextStage,
                status: "todo",
                assigned_agent: null,
                started_at: null,
                completed_at: null,
              }).eq("id", task.id);
              return; // Don't trigger QA routing for intermediate stages
            }
          }
          // Final stage (deployer) completed — fall through to QA routing
          if (isFinalStage(task.stage)) {
            console.log(`[STAGE] Task ${task.id} ("${task.title}"): final stage (${task.stage}) completed, proceeding to QA`);
          }
        }

        // QA dispatch: when scheduler assigns a QA agent to a qa_testing task, dispatch the review
        if (task?.status === "qa_testing" && eventType === "UPDATE" && task.assigned_agent && !prev?.assigned_agent) {
          // Don't QA tasks that were already QA'd (prevent loops)
          if (task.type === "qa") return;

          const qaAgentName = task.assigned_agent.toLowerCase();
          const qaAgent = AGENTS[qaAgentName];
          if (!qaAgent?.token) {
            console.error(`[QA] No token for QA agent ${qaAgentName}, task ${task.id}`);
            return;
          }

          // Pre-flight credential check for QA dispatch
          if (task.app_id) {
            const qaDispatchAppCtx = await fetchAppContext(task.id, task.app_id);
            if (qaDispatchAppCtx) {
              const qaDispatchCreds = await getAgentCredentials(qaAgentName);
              const qaCredCheck = checkAgentCredentials(qaDispatchAppCtx, qaDispatchCreds, 'qa');
              if (!qaCredCheck.ok) {
                console.warn(`[DISPATCH] No agent has required credentials for app "${qaDispatchAppCtx.name}": missing ${qaCredCheck.missing.join(', ')}`);
                await supabase.from('agent_tasks').update({ assigned_agent: null, qa_agent: null }).eq('id', task.id);
                await logTransientError(task.id, `QA agent ${qaAgentName} missing credentials for app "${qaDispatchAppCtx.name}": ${qaCredCheck.missing.join(', ')}`);
                return;
              }
            }
          }

          console.log(`[QA] Dispatching QA review for task ${task.id} ("${task.title}") → ${qaAgentName}`);

          // Set started_at to QA start time so absolute timeout counts from here
          await supabase.from('agent_tasks').update({ started_at: new Date().toISOString() }).eq('id', task.id);

          try {
            const qaContextBlock = await buildContextBlockWithTimeout(task);
            const qaCommentsBlock = await fetchTaskComments(task.id);
            const taskType = task.type || 'general';
            const resultStr = typeof task.result === 'string' ? task.result : JSON.stringify(task.result || {});
            const prMatch = resultStr.match(/PR\s*#(\d+)/i);
            const repoMatch = resultStr.match(/(dante-alpha-assistant\/[\w-]+)/);

            // Fetch app context for repo validation in QA
            const qaAppContext = await fetchAppContext(task.id, task.app_id);
            const qaRepoValidation = qaAppContext ? buildQaRepoValidation(qaAppContext) : '';

            let qaScope = '';
            let timeLimit = '3 minutes';
            if (taskType === 'ops' || taskType === 'review') {
              qaScope = `### Lightweight QA (ops/config task — complete in under 1 minute)
1. Check the result — was the change applied successfully?
2. Any errors in the output?
3. Does the result match the description?
4. **Verify the ACTUAL outcome** — if the task claims a DB change was made, verify it exists (query Supabase API). If a deployment was made, verify the pod/service is running. Do NOT just trust the agent's summary.
5. If the agent's result says "apply manually" or "run this in the dashboard", REJECT and set status back to \`blocked\` — agents must either do it themselves or properly block the task.
DO NOT: SSH into servers or deep-dive into infrastructure.`;
              timeLimit = '1 minute';
            } else if (taskType === 'coding') {
              qaScope = `### Code Review QA (complete in under 3 minutes)
1. ${prMatch ? `Check PR #${prMatch[1]}${repoMatch ? ` on ${repoMatch[1]}` : ''} — read the diff` : 'Check the task result for a PR reference'}
2. Scan for: obvious bugs, missing error handling, broken imports, hardcoded secrets
3. Does the code match what was requested in the description?
4. Check for regressions — does the change break existing patterns?
5. **Verify actual outcomes** — for DB changes, verify the trigger/function/table exists via Supabase API. For deployments, verify the feature works end-to-end (not just that a pod restarted). Do NOT just trust that merged code = working feature.
6. If the result contains "apply manually" or "run this in the dashboard", REJECT — set task back to \ with reason explaining what the agent failed to do autonomously.
DO NOT: Clone the repo, run builds, run tests, or spend more than 3 minutes.

### ⛔ ATOMIC RULE: ONE status update only
You MUST make exactly ONE status update call (pass OR fail). Your first update is FINAL.
Do NOT update status to completed and then change qa_result or status in a second call.
The database will REJECT any changes to a completed task. Decide pass/fail BEFORE updating.

### ⛔ HARD RULE: Coding tasks MUST have a PR
If you cannot find a Pull Request URL (in the result, pull_request_url field, or task comments), you MUST **FAIL** the task immediately with:
- status: "failed"
- qa_result: { passed: false, failures: ["No PR URL found — cannot perform code review"] }
Do NOT pass a coding task without reviewing actual code changes in a PR.`;
              timeLimit = '3 minutes';
            } else {
              qaScope = `### Quick QA (complete in under 2 minutes)
1. Does the result match the task description?
2. Any obvious errors?
DO NOT spend more than 2 minutes on this review.`;
              timeLimit = '2 minutes';
            }

            // Add app repo validation to QA scope if task is app-scoped
            if (qaRepoValidation) {
              qaScope += '\n' + qaRepoValidation;
            }

            // Add Gherkin acceptance criteria instructions to ALL QA reviews
            qaScope += `

### ⚠️ MANDATORY: Write Gherkin Acceptance Criteria

Before updating task status, you MUST generate Gherkin acceptance criteria (Given/When/Then scenarios) based on the task description and result. Write them to the \`acceptance_criteria\` field using a PATCH request.

**DO NOT write Gherkin scenarios to the \`description\` field. The original description must NEVER be overwritten.**

\`\`\`bash
curl -s -X PATCH "https://lessxkxujvcmublgwdaa.supabase.co/rest/v1/agent_tasks?id=eq.${task.id}" \\
  -H "apikey: ${SUPABASE_KEY}" \\
  -H "Authorization: Bearer ${SUPABASE_KEY}" \\
  -H "Content-Type: application/json" \\
  -d '{"acceptance_criteria": "YOUR_GHERKIN_SCENARIOS_HERE"}'
\`\`\`

Generate realistic Gherkin scenarios that cover the task requirements, then PATCH them BEFORE updating the final task status.`;

            const qaPayload = JSON.stringify({
              task_id: task.id,
              title: task.title,
              description: task.description,
              acceptance_criteria: task.acceptance_criteria,
              type: task.type,
              priority: task.priority,
              stage: task.stage,
              parent_task_id: task.parent_task_id,
              dispatched_by: task.dispatched_by,
              pull_request_url: task.pull_request_url,
            }, null, 2);

            const qaMessage = `\`\`\`json
${qaPayload}
\`\`\`

${qaContextBlock}## QA Review: ${task.title}

**Task ID:** ${task.id}
**Type:** ${taskType} | **Time limit:** ${timeLimit}

### Description
${task.description || "(none)"}

### Pull Request URL
${task.pull_request_url && task.pull_request_url.length ? task.pull_request_url.join("\n") : "(no PR URL set)"}

### Result
${task.result ? JSON.stringify(task.result, null, 2).slice(0, 1000) : "(no result reported)"}

${qaScope}
${qaCommentsBlock ? qaCommentsBlock : ""}

### Update task status when done:

**If QA passes (coding tasks — DO NOT MERGE, queue handles it):**
\`\`\`bash
# DO NOT merge — the merge queue handles this automatically
curl -s -X PATCH "https://lessxkxujvcmublgwdaa.supabase.co/rest/v1/agent_tasks?id=eq.${task.id}" \\
  -H "apikey: ${SUPABASE_KEY}" \\
  -H "Authorization: Bearer ${SUPABASE_KEY}" \\
  -H "Content-Type: application/json" \\
  -d '{"status":"completed","completed_at":"'$(date -u +%Y-%m-%dT%H:%M:%SZ)'","qa_result":{"passed":true,"notes":"WHAT YOU VERIFIED"}}'
\`\`\`

**If QA fails (do NOT merge):**
\`\`\`bash
curl -s -X PATCH "https://lessxkxujvcmublgwdaa.supabase.co/rest/v1/agent_tasks?id=eq.${task.id}" \\
  -H "apikey: ${SUPABASE_KEY}" \\
  -H "Authorization: Bearer ${SUPABASE_KEY}" \\
  -H "Content-Type: application/json" \\
  -d '{"status":"failed","completed_at":"'$(date -u +%Y-%m-%dT%H:%M:%SZ)'","qa_result":{"passed":false,"failures":["SPECIFIC ISSUE"]}}'
\`\`\``;

            const resp = await fetch(qaAgent.url, {
              method: "POST",
              headers: {
                Authorization: `Bearer ${qaAgent.token}`,
                "Content-Type": "application/json",
              },
              body: JSON.stringify({
                message: qaMessage,
                name: "Task Dispatcher (QA)",
                sessionKey: `hook:qa:${task.id}`,
                wakeMode: "now",
              }),
            });

            if (resp.ok) {
              console.log(`[QA] Dispatched task ${task.id} to ${qaAgentName} for QA review`);
            } else {
              console.error(`[QA] Failed to dispatch to ${qaAgentName}: ${resp.status}`);
            }
          } catch (e) {
            console.error(`[QA] Error dispatching task ${task.id} to QA: ${e.message}`);
          }
        }
      }
    )
    .subscribe((status) => {
      console.log(`[REALTIME] Subscription status: ${status}`);
    });

  // --- Subscribe to task_comments for @mention routing ---
  // When a user posts a comment mentioning an agent, the dispatcher sends a hook
  // to that specific agent with full task context so it can reply.
  const commentsChannel = supabase
    .channel("task-comments-mentions")
    .on(
      "postgres_changes",
      {
        event: "INSERT",
        schema: "public",
        table: "task_comments",
      },
      async (payload) => {
        const comment = payload.new;

        // Ignore agent/system comments — prevents infinite reply loops
        if (comment.author_type === "agent" || comment.author_type === "system") return;

        const body = comment.body || "";
        if (!body.includes("@")) return;

        // Parse @mentions — supports both @agent-name and @[agent-name] formats (case-insensitive)
        const rawMatches = [
          ...(body.match(/@\[([a-zA-Z0-9_-]+)\]/g) || []).map(m => m.slice(2, -1)),  // @[name]
          ...(body.match(/@([a-zA-Z0-9_-]+)(?!\])/g) || []).map(m => m.slice(1)),     // @name
        ];
        if (rawMatches.length === 0) return;

        const mentionedNames = [...new Set(rawMatches.map(m => m.toLowerCase()))];
        console.log(`[COMMENT-MENTION] Comment ${comment.id} on task ${comment.task_id} @mentions: ${mentionedNames.join(", ")}`);

        // Fetch full task context
        const { data: task } = await supabase
          .from("agent_tasks")
          .select("id, title, description, status, type, priority, result, pull_request_url, assigned_agent, blocked_reason, error, acceptance_criteria")
          .eq("id", comment.task_id)
          .single();
        if (!task) {
          console.warn(`[COMMENT-MENTION] Task ${comment.task_id} not found`);
          return;
        }

        // Fetch full comment thread for context
        const { data: allComments } = await supabase
          .from("task_comments")
          .select("id, author, author_type, body, created_at, reply_to")
          .eq("task_id", comment.task_id)
          .order("created_at", { ascending: true })
          .limit(50);

        // Build comment thread text
        const commentThread = (allComments || []).map(c => {
          const time = new Date(c.created_at).toISOString().replace("T", " ").slice(0, 16);
          return `**${c.author}** (${c.author_type}) — ${time}:\n> ${c.body.replace(/\n/g, "\n> ")}`;
        }).join("\n\n");

        const DASHBOARD_URL = process.env.DASHBOARD_URL || "https://tasks.dante.id";
        const prUrls = Array.isArray(task.pull_request_url) ? task.pull_request_url.join(", ") : (task.pull_request_url || "none");

        // Dispatch to each mentioned agent independently
        for (const agentName of mentionedNames) {
          const agentConfig = AGENTS[agentName];
          if (!agentConfig) {
            console.log(`[COMMENT-MENTION] Unknown agent: @${agentName} — skipping`);
            continue;
          }

          const callbackUrl = `${DASHBOARD_URL}/api/tasks/${comment.task_id}/comments/reply`;

          const message = `## 💬 Task Comment — You were @mentioned by ${comment.author}

You have been mentioned in a task comment. Read the full context below, understand what is being asked, and reply with a helpful response.

---

### Task Details
**Task:** ${task.title}
**Task ID:** ${task.id}
**Status:** ${task.status} | **Type:** ${task.type} | **Priority:** ${task.priority}
**PR:** ${prUrls}
${task.assigned_agent ? `**Assigned Agent:** ${task.assigned_agent}` : ""}
${task.blocked_reason ? `**⚠️ Blocked Reason:** ${task.blocked_reason}` : ""}
**Task URL:** ${DASHBOARD_URL}/task/${task.id}

---

### 💬 New Comment from ${comment.author}:
${comment.body}

---

### Full Comment Thread:
${commentThread || "(no previous comments)"}

---

### Task Description:
${task.description || "(no description)"}

${task.acceptance_criteria ? `### Acceptance Criteria:\n${task.acceptance_criteria}\n` : ""}
${task.result ? `### Previous Result:\n${typeof task.result === "object" ? JSON.stringify(task.result, null, 2) : task.result}\n` : ""}
${task.error ? `### Error:\n${task.error}\n` : ""}
---

## ⚡ MANDATORY: Reply to this comment

You MUST reply to this comment. Use the following curl command to post your reply:

\`\`\`bash
curl -s -X POST "${callbackUrl}" \\
  -H "Content-Type: application/json" \\
  -d '{"body": "YOUR_REPLY_HERE", "author": "${agentName}", "comment_id": "${comment.id}"}'
\`\`\`

Replace YOUR_REPLY_HERE with your actual response.
Do NOT skip this step — the user is waiting for your reply in the task comment thread.`;

          const sessionKey = `hook:comment:${comment.task_id}:${comment.id}:${agentName}`;

          try {
            const headers = { "Content-Type": "application/json" };
            if (agentConfig.token) headers["Authorization"] = `Bearer ${agentConfig.token}`;

            const resp = await fetch(agentConfig.url, {
              method: "POST",
              headers,
              body: JSON.stringify({
                message,
                name: "Task Comment Mention",
                sessionKey,
                wakeMode: "now",
              }),
            });
            console.log(`[COMMENT-MENTION] Notified @${agentName} for task ${comment.task_id} (comment ${comment.id}): HTTP ${resp.status}`);
          } catch (e) {
            console.error(`[COMMENT-MENTION] Failed to notify @${agentName}: ${e.message}`);
          }
        }
      }
    )
    .subscribe((status) => {
      console.log(`[REALTIME-COMMENTS] Comment mention subscription status: ${status}`);
    });

  return channel;
}

// --- Health check server ---
import { createServer } from "http";
const PORT = process.env.PORT || 8080;
createServer(async (req, res) => {
  const url = new URL(req.url, "http://localhost");
  const pathname = url.pathname;

  if (pathname === "/health" || pathname === "/") {
    let capacity = {};
    try {
      const [{ data }, cards] = await Promise.all([
        supabase.from("agent_tasks").select("assigned_agent, status").or("status.eq.in_progress,assigned_agent.not.is.null"),
        getAgentCards(),
      ]);
      for (const card of cards) {
        const load = (data || []).filter(t => t.assigned_agent?.toLowerCase() === card.name).length;
        capacity[card.name] = { load, max: card.max_concurrent, available: card.max_concurrent - load, capabilities: card.capabilities };
      }
    } catch {}
    let qaWorkers = 0;
    try {
      const jobListResp = await batchApi.listNamespacedJob({
        namespace: "agents",
        labelSelector: "role=beta-worker",
      });
      const jobList = jobListResp?.body || jobListResp || {};
      qaWorkers = (jobList.items || []).filter(j => !j.status?.succeeded && !j.status?.failed).length;
    } catch {}
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, service: "task-dispatcher", activeTasks: activeTasks.size, capacity, qaWorkers }));
  } else if (pathname === "/events" && req.method === "GET") {
    // SSE endpoint
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      "Connection": "keep-alive",
    });
    const tasksParam = url.searchParams.get("tasks");
    const taskFilter = tasksParam ? new Set(tasksParam.split(",")) : null;
    const client = { res, taskFilter };
    sseClients.add(client);
    req.on("close", () => sseClients.delete(client));
    // Send initial snapshot
    for (const [taskId, info] of activeTasks) {
      const snapshot = { taskId, ...info, timestamp: new Date().toISOString() };
      res.write(`event: task:status\ndata: ${JSON.stringify(snapshot)}\n\n`);
    }
    for (const [taskId, progress] of latestProgress) {
      if (!taskFilter || taskFilter.has(taskId)) {
        res.write(`event: task:progress\ndata: ${JSON.stringify({ taskId, ...progress, timestamp: new Date().toISOString() })}\n\n`);
      }
    }
  } else if (pathname === "/progress" && req.method === "POST") {
    // Progress reporting endpoint
    let body = "";
    req.on("data", chunk => body += chunk);
    req.on("end", () => {
      try {
        const auth = req.headers.authorization || "";
        const token = auth.replace("Bearer ", "");
        const knownTokens = Object.values(AGENTS).map(a => a.token).filter(Boolean);
        if (!token || !knownTokens.includes(token)) {
          res.writeHead(401, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "Unauthorized" }));
          return;
        }
        const { taskId, percent, step, log } = JSON.parse(body);
        const progressData = { percent, step, log, timestamp: new Date().toISOString() };
        latestProgress.set(taskId, progressData);
        broadcast("task:progress", { taskId, ...progressData }, taskId);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true }));
      } catch (e) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: e.message }));
      }
    });
  } else if (pathname === "/api/error-stats" && req.method === "GET") {
    // Error category breakdown stats
    try {
      const period = url.searchParams.get("period") || "7d";
      const periodMs = period === "24h" ? 86400000 : period === "7d" ? 604800000 : 604800000;
      const since = new Date(Date.now() - periodMs).toISOString();

      const { data, error } = await supabase
        .from("task_activity_log")
        .select("error_category, changed_at")
        .in("field", ["error", "dispatch_error"])
        .gte("changed_at", since)
        .not("error_category", "is", null);

      if (error) throw error;

      // Count by category
      const counts = {};
      for (const entry of (data || [])) {
        const cat = entry.error_category || "unknown";
        counts[cat] = (counts[cat] || 0) + 1;
      }

      // Enrich with metadata
      const categories = getErrorCategories();
      const breakdown = categories.map(c => ({
        ...c,
        count: counts[c.category] || 0,
      })).filter(c => c.count > 0).sort((a, b) => b.count - a.count);

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true, period, since, breakdown, total: (data || []).length }));
    } catch (e) {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: false, error: e.message }));
    }
  } else if (pathname === "/api/error-categories" && req.method === "GET") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(getErrorCategories()));
  } else if (pathname === "/api/agents/register-credentials" && req.method === "POST") {
    // Agent credential self-registration endpoint
    // Agents call this on startup to report which known env vars they have
    let body = "";
    req.on("data", chunk => body += chunk);
    req.on("end", async () => {
      try {
        // Authenticate: agent must provide a known hooks token
        const auth = req.headers.authorization || "";
        const token = auth.replace("Bearer ", "");
        const knownTokens = Object.values(AGENTS).map(a => a.token).filter(Boolean);
        if (!token || !knownTokens.includes(token)) {
          res.writeHead(401, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "Unauthorized" }));
          return;
        }

        const { agent_name, available_credentials } = JSON.parse(body);
        if (!agent_name || typeof agent_name !== "string") {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "agent_name is required" }));
          return;
        }
        if (!Array.isArray(available_credentials)) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "available_credentials must be an array" }));
          return;
        }

        // Sanitize: only allow known credential names (no values)
        const ALLOWED_CREDENTIAL_NAMES = [
          "GH_TOKEN", "SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_MGMT_TOKEN",
          "VERCEL_TOKEN", "KUBECONFIG", "ANTHROPIC_API_KEY", "OPENROUTER_API_KEY",
          "DOCKER_TOKEN", "NPM_TOKEN", "AWS_ACCESS_KEY_ID",
        ];
        const sanitized = available_credentials.filter(c =>
          typeof c === "string" && ALLOWED_CREDENTIAL_NAMES.includes(c)
        );

        const { error } = await supabase
          .from("agent_cards")
          .update({ available_credentials: sanitized })
          .eq("id", agent_name);

        if (error) {
          console.error(`[CRED-REG] DB error for ${agent_name}:`, error.message);
          res.writeHead(500, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: false, error: error.message }));
          return;
        }

        console.log(`[CRED-REG] ${agent_name} registered ${sanitized.length} credential(s): ${sanitized.join(", ") || "none"}`);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true, agent: agent_name, credentials: sanitized }));
      } catch (e) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: e.message }));
      }
    });
  } else {
    res.writeHead(404);
    res.end("Not found");
  }
}).listen(PORT, () => {
  console.log(`[HEALTH] Listening on :${PORT}`);
});

// SSE heartbeat every 30s
setInterval(() => {
  broadcast("heartbeat", { timestamp: new Date().toISOString(), activeTasks: activeTasks.size });
}, 30_000);

// --- Unified Task Monitor ---
// Replaces both watchdog and session checker
// Queries ALL in_progress tasks from Supabase (not just locally tracked ones)
// Checks if agent session still exists, auto-closes if not
const MONITOR_INTERVAL = 30_000; // 30 seconds
const TASK_HARD_TIMEOUT = 15 * 60 * 1000; // 15 minutes for normal tasks
const QA_HARD_TIMEOUT = 20 * 60 * 1000; // 20 minutes for QA tasks
const SESSION_GONE_GRACE = 90_000; // 1.5 minute grace after session disappears
const CODING_HARD_TIMEOUT = 45 * 60 * 1000; // 45 minutes for coding tasks

// Track when we first noticed a session was gone: taskId → timestamp
const sessionGoneAt = new Map();

async function taskMonitor() {
  try {
    // Get ALL in_progress and assigned tasks from Supabase
    const { data: activeTasks_db, error } = await supabase
      .from("agent_tasks")
      .select("id, title, status, assigned_agent, qa_agent, started_at, created_at, result, pull_request_url, idle_retries, type")
      .in("status", ["in_progress", "qa_testing"]);

    if (error) {
      console.error("[MONITOR] Query error:", error.message);
      return;
    }

    if (!activeTasks_db?.length) {
      sessionGoneAt.clear(); // No active tasks, clear tracking
      return;
    }

    for (const task of activeTasks_db) {
      // Race condition protection: re-check task status before processing
      const { data: freshTask } = await supabase
        .from("agent_tasks")
        .select("status")
        .eq("id", task.id)
        .single();
      if (freshTask && ["done", "failed", "completed", "deprecated"].includes(freshTask.status)) {
        continue;
      }

      // Skip unassigned qa_testing tasks — they are waiting for the scheduler to assign them
      if (task.status === "qa_testing" && !task.assigned_agent && !task.qa_agent) {
        continue;
      }

      // Skip tasks without started_at — they have not been dispatched yet
      if (!task.started_at) {
        continue;
      }

      // For qa_testing tasks, check the QA agent (Beta); for others, check assigned_agent
      const isQaTesting = task.status === "qa_testing";
      const agentName = isQaTesting
        ? (task.qa_agent?.toLowerCase() || "beta-worker")
        : task.assigned_agent?.toLowerCase();
      const agent = AGENTS[agentName];

      if (!agent) continue;

      // Use gateway token for session polling (NOT hooks token)
      const authToken = agent.gatewayToken || agent.token;
      const invokeUrl = agent.url.replace('/hooks/agent', '/tools/invoke');

      // Only check sessions for in_progress and qa_testing tasks (assigned tasks haven't started yet)
      if (task.status !== "in_progress" && task.status !== "qa_testing") continue;

      // Check if session still exists FIRST (before any timeout logic)
      let sessions = [];
      let sessionAlive = false;
      try {
        const resp = await fetch(invokeUrl, {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${authToken}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            tool: "sessions_list",
            params: { limit: 100, messageLimit: 0 },
          }),
        });

        if (resp.ok) {
          const data = await resp.json();
          sessions = data?.result?.details?.sessions || [];
          const sessionKey = isQaTesting
            ? `agent:main:hook:qa:${task.id}`
            : `agent:main:hook:task:${task.id}`;
          const hookSession = sessions.find(s => s.key === sessionKey);

          if (hookSession) {
            sessionAlive = true;
            sessionGoneAt.delete(task.id);
            const idleSec = Math.floor((Date.now() - hookSession.updatedAt) / 1000);
            if (idleSec > 120) {
              console.log(`[MONITOR] Task ${task.id} → ${agentName}: session alive, idle ${idleSec}s`);
            }
          }
        } else {
          console.log(`[MONITOR] Cannot reach ${agentName} gateway (${resp.status}) for task ${task.id}`);
          continue; // Can't determine session state, skip
        }
      } catch (e) {
        console.error(`[MONITOR] Error checking ${agentName} for task ${task.id}: ${e.message}`);
        continue; // Can't determine session state, skip
      }

      // Broadcast monitor status to SSE clients
      {
        const startTime = task.started_at || task.created_at;
        const monitorElapsed = startTime ? Date.now() - new Date(startTime).getTime() : 0;
        broadcast("task:monitor", {
          taskId: task.id,
          sessionAlive,
          idleSeconds: 0,
          elapsed: monitorElapsed,
          timestamp: new Date().toISOString(),
        }, task.id);
      }

      if (sessionAlive) {
        // Idle timeout: QA tasks get longer timeout since they need to clone, build, and test
        // Per-task-type idle timeouts (Fix: coding was 5min, now 15min)
        const IDLE_TIMEOUTS = {
          coding: 30 * 60 * 1000,   // 30 min - codex/claude code needs time - clones, codex subprocess, builds, PRs
          deploy: 30 * 60 * 1000,   // 30 min - merges, conflict resolution, CI waits, ArgoCD
          ops: 20 * 60 * 1000,      // 20 min - kubectl, sealing, infra
          research: 20 * 60 * 1000, // 20 min - deep research rounds
          qa: 15 * 60 * 1000,       // 15 min - clone, build, test
          review: 10 * 60 * 1000,   // 10 min
          general: 10 * 60 * 1000,  // 10 min
          manual: 10 * 60 * 1000,   // 10 min
        };
        const taskType = isQaTesting ? "qa" : (task.type || "general");
        const IDLE_TIMEOUT = IDLE_TIMEOUTS[taskType] || 10 * 60 * 1000; // default 10 min
        const hookSession = sessions?.find(s => s.key === (isQaTesting ? `agent:main:hook:qa:${task.id}` : `agent:main:hook:task:${task.id}`));
        // Subprocess-aware idle detection: check if any exec sessions are still running
        // (e.g. Codex 5.3, git clone, npm run build) — these mean the agent is NOT idle
        const hasActiveSubprocesses = sessions.some(s => {
          if (!s.key || !s.key.startsWith("exec:")) return false;
          // Only count subprocesses that updated recently (within 2x the idle timeout)
          const subAge = Date.now() - (s.updatedAt || 0);
          return subAge < IDLE_TIMEOUT * 2;
        });
        const idleMs = hookSession ? Date.now() - (hookSession.updatedAt || 0) : 0;
        if (hasActiveSubprocesses && idleMs > IDLE_TIMEOUT) {
          console.log(`[MONITOR] Task ${task.id} idle ${Math.floor(idleMs/60000)}min but has active subprocesses — extending timeout`);
        }
        if (idleMs > IDLE_TIMEOUT && !hasActiveSubprocesses) {
          const idleRetries = (task.idle_retries || 0) + 1;
          const MAX_IDLE_RETRIES = 3;
          const hasCompletedWork = !!(task.result || (task.pull_request_url && task.pull_request_url.length > 0));

          if (idleRetries >= MAX_IDLE_RETRIES) {
            // Always go to blocked after max retries — preserve completed work context
            const reason = hasCompletedWork
              ? `QA timed out ${idleRetries} times but work is done (has result/PR) — needs manual QA or re-dispatch`
              : `Timed out ${idleRetries} times — agent may be down or task needs manual intervention`;
            console.log(`[MONITOR] Idle timeout: task ${task.id} ("${task.title.slice(0,40)}") → ${agentName} idle ${idleRetries} times → moving to BLOCKED (hasWork: ${hasCompletedWork})`);
            await supabase.from("agent_tasks").update({
              status: "blocked",
              assigned_agent: null,
              started_at: null,
              idle_retries: idleRetries,
              blocked_reason: reason,
              error: `Idle timeout: ${agentName} unresponsive after ${idleRetries} retries — moved to blocked`,
            }).eq("id", task.id);
            await logTaskActivity(task.id, 'dispatch_error', null, `Idle timeout: ${agentName} unresponsive after ${idleRetries} retries. HasCompletedWork: ${hasCompletedWork}. Moved to blocked.`, 'dispatcher');
            // Close the stale hook session
            closeAgentSession(agentName, task.id, isQaTesting).catch(() => {});
          } else if (hasCompletedWork && isQaTesting) {
            // QA idle but work is done — stay in qa_testing, just clear QA agent for re-dispatch
            // Don't reset to todo — that loses the completed coding work
            console.log(`[MONITOR] QA idle timeout: task ${task.id} ("${task.title.slice(0,40)}") → QA agent idle ${Math.floor(idleMs/60000)}min but work is done — clearing QA agent for re-dispatch (retry ${idleRetries}/${MAX_IDLE_RETRIES})`);
            await supabase.from("agent_tasks").update({
              qa_agent: null,
              idle_retries: idleRetries,
            }).eq("id", task.id);
            const qaIdleMsg = `QA idle timeout: ${agentName} session idle >${Math.floor(idleMs/60000)}min but work is done — clearing QA agent for re-dispatch (retry ${idleRetries}/${MAX_IDLE_RETRIES})`;
            await logTransientError(task.id, qaIdleMsg);
            try {
              await supabase.from('task_comments').insert({
                task_id: task.id, author: 'system', author_type: 'system',
                content: `⏰ ${qaIdleMsg}`,
              });
            } catch(e) {}
          } else {
            // Re-check task state to avoid race conditions
            const { data: idleCurrentTask } = await supabase
              .from('agent_tasks')
              .select('status, result, pull_request_url, assigned_agent')
              .eq('id', task.id)
              .single();
            if (idleCurrentTask && ['completed', 'deployed', 'done', 'failed', 'deprecated'].includes(idleCurrentTask.status)) {
              console.log(`[MONITOR] Task ${task.id} already moved to ${idleCurrentTask.status} — skipping idle timeout`);
              continue;
            }
            const hasCompletedWork = !!(idleCurrentTask?.result || (idleCurrentTask?.pull_request_url && idleCurrentTask.pull_request_url.length > 0));
            // If work is done, go directly to qa_testing instead of todo/blocked (prevents regression)
            const targetStatus = hasCompletedWork ? 'qa_testing' : 'todo';
            console.log(`[MONITOR] Idle timeout: task ${task.id} ("${task.title.slice(0,40)}") → ${agentName} idle ${Math.floor(idleMs/60000)}min (retry ${idleRetries}/${MAX_IDLE_RETRIES}) → ${targetStatus} (hasWork: ${hasCompletedWork})`);
            recordTransition(task.id);
            const agentWhoWorkedIdle = idleCurrentTask?.assigned_agent || task.assigned_agent || agentName;
            await supabase.from("agent_tasks").update({
              status: targetStatus,
              assigned_agent: null,
              started_at: null,
              idle_retries: idleRetries,
              ...(hasCompletedWork ? { completed_at: new Date().toISOString() } : {}),
            }).eq("id", task.id);
            if (hasCompletedWork) {
              await logTaskActivity(task.id, 'status', 'in_progress', 'qa_testing', agentWhoWorkedIdle);
            }
            // Close the stale hook session on the agent gateway
            closeAgentSession(agentName, task.id, isQaTesting).catch(() => {});
            const idleMsg = `Idle timeout: ${agentName} session idle >${Math.floor(idleMs/60000)}min — ${hasCompletedWork ? 'qa_testing (has work)' : 're-queued'} (retry ${idleRetries}/${MAX_IDLE_RETRIES})`;
            await logTransientError(task.id, idleMsg);
            // Post visible comment so timeline shows WHY the task bounced back
            try {
              await supabase.from('task_comments').insert({
                task_id: task.id,
                author: 'system',
                author_type: 'system',
                content: `⏰ ${idleMsg}`,
              });
              console.log(`[AUTO-COMMENT] Posted idle timeout comment for task ${task.id}`);
            } catch(e) { console.error(`[AUTO-COMMENT] idle comment error: ${e.message}`); }
          }
          activeTasks.delete(task.id);
          continue;
        }

        // Session is active — only hard-timeout if REALLY old (safety net)
        const startTime = task.started_at || task.created_at;
        // Use status-based timeout: QA tasks get QA timeout even if task.type is 'coding'
        const isInQa = task.status === 'qa_testing';
        const timeout = isInQa ? QA_HARD_TIMEOUT : ((task.type) === "qa" ? QA_HARD_TIMEOUT : (task.type) === "coding" ? CODING_HARD_TIMEOUT : TASK_HARD_TIMEOUT);
        const elapsed = startTime ? Date.now() - new Date(startTime).getTime() : 0;
        // Even with active session, kill after 2x the timeout (absolute safety)
        if (elapsed > timeout * 2) {
          console.log(`[MONITOR] Absolute timeout: task ${task.id} ("${task.title}") → ${agentName} (${Math.round(elapsed / 60000)}min, session still alive but too old)`);
          // If task has completed work (result/PR) or is in QA, block instead of fail
          const hasWork = !!(task.result || (task.pull_request_url && task.pull_request_url.length > 0));
          if (hasWork || task.status === 'qa_testing') {
            await supabase.from("agent_tasks").update({
              status: "blocked",
              assigned_agent: null,
              started_at: null,
              blocked_reason: `Absolute timeout after ${Math.round(elapsed / 60000)}min — work is complete but ${task.status === 'qa_testing' ? 'QA' : 'processing'} timed out`,
              error: `Absolute timeout: task ran for ${Math.round(elapsed / 60000)} minutes — moved to blocked (has work)`,
            }).eq("id", task.id);
          } else {
            await supabase.from("agent_tasks").update({
              status: "failed",
              error: `Absolute timeout: task ran for ${Math.round(elapsed / 60000)} minutes`,
              completed_at: new Date().toISOString(),
            }).eq("id", task.id);
          }
          sessionGoneAt.delete(task.id);
          activeTasks.delete(task.id);
        }
      } else {
        // Session is gone — start grace period then auto-close
        if (!sessionGoneAt.has(task.id)) {
          sessionGoneAt.set(task.id, Date.now());
          console.log(`[MONITOR] Task ${task.id} ("${task.title}") → ${agentName}: session gone, grace period started`);
        } else if (Date.now() - sessionGoneAt.get(task.id) > SESSION_GONE_GRACE) {
          // === RACE GUARD: Re-check task state before any status change ===
          const { data: currentTask } = await supabase
            .from('agent_tasks')
            .select('status, result, pull_request_url, assigned_agent')
            .eq('id', task.id)
            .single();
          
          if (currentTask && ['completed', 'deployed', 'done', 'failed', 'deprecated'].includes(currentTask.status)) {
            console.log(`[MONITOR] Task ${task.id} already moved to ${currentTask.status} — skipping session-gone handler`);
            sessionGoneAt.delete(task.id);
            activeTasks.delete(task.id);
            continue;
          }

          // Session gone but task still in qa_testing → clear qa_agent for re-dispatch
          if (currentTask && currentTask.status === 'qa_testing') {
            const hasWork = !!(currentTask.result || (currentTask.pull_request_url && currentTask.pull_request_url.length > 0));
            console.log(`[MONITOR] Session gone for QA task ${task.id} ("${task.title.slice(0,40)}") → clearing qa_agent for re-dispatch (hasWork: ${hasWork})`);
            await supabase.from("agent_tasks").update({
              qa_agent: null,
              assigned_agent: null,
              started_at: null,
            }).eq("id", task.id);
            await logTransientError(task.id, `QA session gone (pod restart?) — re-queued for QA dispatch`);
            sessionGoneAt.delete(task.id);
            activeTasks.delete(task.id);
            continue;
          }
          
          const sessionGoneHasWork = !!(currentTask?.result || (currentTask?.pull_request_url && currentTask.pull_request_url.length > 0));

          const startTime = task.started_at || task.created_at;
          const timeout = (task.type) === "qa" ? QA_HARD_TIMEOUT : (task.type) === "coding" ? CODING_HARD_TIMEOUT : TASK_HARD_TIMEOUT;
          const elapsed = startTime ? Date.now() - new Date(startTime).getTime() : 0;

          if (isQaTesting) {
            // QA session crashed — track retries, block after 3
            const qaRetryCount = (task.qa_retries || 0) + 1;
            if (qaRetryCount > 3) {
              const blockReason = `QA session died ${qaRetryCount} times — moved to blocked. Work is complete but QA cannot verify. Manual review needed.`;
              console.log(`[MONITOR] QA session died ${qaRetryCount}x for task ${task.id} → BLOCKED`);
              await supabase.from("agent_tasks").update({
                status: "blocked",
                qa_agent: null,
                blocked_reason: blockReason,
                error: blockReason,
              }).eq("id", task.id);
              await logTaskActivity(task.id, 'qa_error', null, blockReason, 'dispatcher');
            } else {
              const qaGoneReason = `QA agent session lost (attempt ${qaRetryCount}/3) — re-queued for re-dispatch`;
              console.log(`[MONITOR] QA session gone for task ${task.id} → retry ${qaRetryCount}/3`);
              await supabase.from("agent_tasks").update({
                qa_agent: null,
                qa_retries: qaRetryCount,
              }).eq("id", task.id);
              await logTransientError(task.id, qaGoneReason);
            }
            recordTransition(task.id);
          } else if (sessionGoneHasWork) {
            // Agent completed work but session ended — route directly to qa_testing
            // This prevents the in_progress → todo → qa_testing regression
            const agentWhoWorked = currentTask?.assigned_agent || task.assigned_agent || agentName;
            console.log(`[MONITOR] Task ${task.id} ("${task.title}") — session gone but has completed work → qa_testing (agent: ${agentWhoWorked})`);
            await supabase.from("agent_tasks").update({
              status: "qa_testing",
              assigned_agent: null,
              started_at: null,
              completed_at: new Date().toISOString(),
            }).eq("id", task.id);
            await logTaskActivity(task.id, 'status', 'in_progress', 'qa_testing', agentWhoWorked);
          } else if (elapsed > timeout) {
            console.log(`[MONITOR] Timeout + session gone: task ${task.id} ("${task.title}") → failed (${Math.round(elapsed / 60000)}min)`);
            await supabase.from("agent_tasks").update({
              status: "failed",
              error: `Timeout: session ended after ${Math.round(elapsed / 60000)} minutes without completion`,
              completed_at: new Date().toISOString(),
            }).eq("id", task.id);
          } else {
            console.log(`[MONITOR] Task ${task.id} ("${task.title}") — session ended without reporting completion → failed`);
            await supabase.from("agent_tasks").update({
              status: "failed",
              error: "Session ended without reporting completion",
              completed_at: new Date().toISOString(),
            }).eq("id", task.id);
          }
          sessionGoneAt.delete(task.id);
          activeTasks.delete(task.id);
        }
      }
    }

    // Clean up sessionGoneAt for tasks no longer in the active list
    const activeIds = new Set(activeTasks_db.map(t => t.id));
    for (const taskId of sessionGoneAt.keys()) {
      if (!activeIds.has(taskId)) sessionGoneAt.delete(taskId);
    }
  } catch (e) {
    console.error("[MONITOR] Error:", e.message);
  }
}


// Check if a task's dependencies are all completed/deployed
// Returns true if all met, or an object { met: false, unmet: [...] } with details
async function areDependenciesMet(taskId, { detailed = false } = {}) {
  try {
    const { data: deps } = await supabase
      .from('task_relationships')
      .select('target_task_id')
      .eq('source_task_id', taskId)
      .eq('relationship_type', 'depends_on');
    
    if (!deps || deps.length === 0) return detailed ? { met: true, unmet: [] } : true;
    
    const depIds = deps.map(d => d.target_task_id);
    const { data: depTasks } = await supabase
      .from('agent_tasks')
      .select('id, title, status')
      .in('id', depIds);
    
    const completedStatuses = new Set(['completed', 'deployed', 'deploying']);
    const unmet = (depTasks || []).filter(t => !completedStatuses.has(t.status));
    
    if (!detailed) return unmet.length === 0;
    return { met: unmet.length === 0, unmet };
  } catch (e) {
    console.error('[SCHEDULER] Error checking dependencies:', e.message);
    return detailed ? { met: true, unmet: [] } : true;
  }
}

// --- Auto-scheduler: assign todo tasks to available agents ---
async function scheduler() {
  try {
    console.log("[SCHEDULER] Cycle starting...");
    const cards = await getAgentCards();
    console.log(`[SCHEDULER] Got ${cards.length} agent cards`);
    if (!cards.length) {
      console.log("[SCHEDULER] No online agents found — skipping cycle");
      return;
    }

    // Only count tasks in active statuses as load (not deployed/failed/completed/deprecated)
    const { data: activeTasks_db, error: activeErr } = await supabase
      .from("agent_tasks")
      .select("assigned_agent")
      .not("assigned_agent", "is", null)
      .in("status", ["in_progress", "qa_testing"]);

    if (activeErr) {
      console.error("[SCHEDULER] Error fetching active tasks:", activeErr.message);
      return;
    }

    // Build load map from active tasks (in_progress OR assigned but not yet started)
    const agentLoad = {};
    for (const card of cards) agentLoad[card.name] = 0;
    for (const t of activeTasks_db || []) {
      const agent = t.assigned_agent?.toLowerCase();
      if (agent && agentLoad[agent] !== undefined) agentLoad[agent]++;
    }

    // Build available agents with remaining capacity
    const available = [];
    for (const card of cards) {
      const remaining = card.max_concurrent - (agentLoad[card.name] || 0);
      if (remaining > 0) {
        available.push({ name: card.name, remaining, max_concurrent: card.max_concurrent, capabilities: card.capabilities, priority_affinity: card.priority_affinity || {} });
      }
    }

    if (available.length === 0) { console.log("[SCHEDULER] No agents with capacity"); return; }

    // Belt-and-suspenders: fetch disabled/degraded agents and exclude them
    const { data: disabledAgents } = await supabase
      .from('agent_cards')
      .select('name')
      .in('status', ['disabled', 'degraded']);
    const disabledNames = new Set((disabledAgents || []).map(a => a.name.toLowerCase()));
    const availableFiltered = available.filter(a => !disabledNames.has(a.name));
    if (availableFiltered.length === 0) { console.log("[SCHEDULER] No available agents after filtering disabled"); return; }

    // DB is the source of truth for agent availability — no gateway session checks.
    // The agent_load count + max_capacity + inflightCheck guard are sufficient.
    const freeAgents = availableFiltered;

    // Fetch todo tasks that don't have an agent assigned yet
    // Cooldown: skip tasks updated in last 30s to prevent assign/clear loops
    const cooldownTime = new Date(Date.now() - 30000).toISOString();
    const { data: todoTasks, error: todoErr } = await supabase
      .from("agent_tasks")
      .select("*")
      .eq("status", "todo")
      .is("assigned_agent", null)
      .neq("paused", true)
      .lt("updated_at", cooldownTime)
      .order("created_at", { ascending: true });

    if (todoErr) {
      console.error("[SCHEDULER] Error fetching todo tasks:", todoErr.message);
      return;
    }

    // === GUARD: Skip todo tasks that already have completed work (result/PR) ===
    const trulyTodoTasks = [];
    for (const t of (todoTasks || [])) {
      if (t.type === "manual") continue;
      // Check task dependencies
      const depsMet = await areDependenciesMet(t.id);
      if (!depsMet) {
        console.log('[SCHEDULER] Task ' + t.id.slice(0,8) + ' has unmet dependencies — skipping');
        continue;
      }
      const hasWork = !!(t.result || (t.pull_request_url && t.pull_request_url.length > 0));
      const isQaRetry = (t.qa_retries || 0) > 0;  // QA failed → needs coding fix, not re-QA
      const isRebaseNeeded = !!(t.metadata && t.metadata.rebase_requested);  // Merge queue conflict → needs rebase, not re-QA
      if (hasWork && !isQaRetry && !isRebaseNeeded) {  // Skip QA routing for tasks needing coding fixes or rebases
        // depsMet already checked above — if we got here, dependencies are met
        const originalAgent = t.last_failed_agent || t.assigned_agent || 'unknown-agent';
        console.log('[SCHEDULER] Task ' + t.id + ' has completed work but is todo — routing to qa_testing (original agent: ' + originalAgent + ')');
        await supabase.from('agent_tasks').update({ status: 'qa_testing', assigned_agent: null }).eq('id', t.id);
        await logTaskActivity(t.id, 'status', 'todo', 'qa_testing', originalAgent);
        recordTransition(t.id);
        continue;
      }
      if (!canTransition(t.id)) {
        console.log("[SCHEDULER] Cooldown active for task " + t.id + " — skipping this cycle");
        continue;
      }
      console.log(`[SCHEDULER] Task ${t.id.slice(0,8)} ("${t.title.slice(0,30)}") passed all filters — schedulable`);
      trulyTodoTasks.push(t);
    }

    // Fetch unassigned QA tasks for scheduling
    const { data: qaTasks } = await supabase
      .from('agent_tasks')
      .select('*')
      .eq('status', 'qa_testing')
      .is('assigned_agent', null)
      .lt('updated_at', cooldownTime)
      .order('created_at', { ascending: true });
    const qaTaskList = (qaTasks || []).filter(t => canTransition(t.id));
    console.log(`[SCHEDULER] Found ${(todoTasks||[]).length} todo + ${qaTaskList.length} QA tasks, ${freeAgents.length} free agents`);
    const allSchedulable = [...qaTaskList, ...trulyTodoTasks];
    if (!allSchedulable.length) { console.log("[SCHEDULER] No schedulable tasks found"); return; }

    allSchedulable.sort((a, b) => {
      const aBoost = a.status === 'qa_testing' ? -0.5 : 0;
      const bBoost = b.status === 'qa_testing' ? -0.5 : 0;
      return ((PRIORITY_ORDER[a.priority] ?? 2) + aBoost) - ((PRIORITY_ORDER[b.priority] ?? 2) + bBoost);
    });

    let assigned = 0;

    for (const task of allSchedulable) {
      const isQaTask = task.status === 'qa_testing';

      // Respect assigned_agent hint — but remap disabled/degraded agents (todo tasks only)
      if (!isQaTask && task.assigned_agent) {
        let hintAgent = task.assigned_agent.toLowerCase();
        // If hint points to a disabled/degraded agent, try the -worker variant
        if (disabledNames.has(hintAgent)) {
          const workerVariant = hintAgent + '-worker';
          if (!disabledNames.has(workerVariant)) {
            console.log(`[SCHEDULER] Remapping disabled/degraded hint ${hintAgent} → ${workerVariant} for task ${task.id}`);
            hintAgent = workerVariant;
          } else {
            // Both unavailable — clear hint and let capability-based routing handle it
            console.log(`[SCHEDULER] Hint ${hintAgent} is disabled/degraded, clearing for task ${task.id}`);
            await supabase.from("agent_tasks").update({ assigned_agent: null }).eq("id", task.id);
            await logTransientError(task.id, `Hint agent ${hintAgent} is disabled/degraded — cleared for re-routing`);
            // Fall through to capability-based assignment below
          }
        }
        const agentSlot = freeAgents.find(a => a.name === hintAgent && a.remaining > 0);
        if (agentSlot) {
          console.log(`[SCHEDULER] Assigning task ${task.id} ("${task.title}") → ${hintAgent} (hint)`);
          await supabase
            .from("agent_tasks")
            .update({ assigned_agent: hintAgent, error: null })
            .eq("id", task.id);
          agentSlot.remaining--;
          assigned++;
          continue;
        }
        continue;
      }

      // For qa_testing tasks, require "qa" capability; for regular tasks, use task type
      const requiredCapability = isQaTask ? "qa" : (task.type || "general");

      // Capability aliasing: some task types map to multiple valid capabilities
      const CAPABILITY_ALIASES = {
        research: ["research", "web_search"],
        ops: ["ops", "kubernetes"],
        deploy: ["deploy", "ops", "kubernetes", "gitops"],
        general: ["coding", "general", "ops"],
      };
      const validCaps = CAPABILITY_ALIASES[requiredCapability] || [requiredCapability];

      // Pre-flight credential check: fetch app context and filter agents by credentials
      let schedulerAppContext = null;
      if (task.app_id) {
        schedulerAppContext = await fetchAppContext(task.id, task.app_id);
      }
      const credType = isQaTask ? 'qa' : 'coding';

      // Score agents: must have required capability, then rank by capacity + priority affinity
      const candidates = freeAgents
        .filter(a => a.remaining > 0 && a.capabilities.some(c => validCaps.includes(c)) )
        .filter(a => {
          // Credential check: if task is app-scoped, agent must have required credentials
          if (!schedulerAppContext) return true;
          const credCheck = checkAgentCredentials(schedulerAppContext, a.available_credentials, credType);
          if (!credCheck.ok) {
            console.log(`[SCHEDULER] Skipping ${a.name} for task ${task.id} — missing credentials for app "${schedulerAppContext.name}": ${credCheck.missing.join(', ')}`);
            return false;
          }
          return true;
        })
        .map(a => {
          let score = a.remaining;
          const affinityMultiplier = a.priority_affinity[task.priority];
          if (affinityMultiplier) score *= affinityMultiplier;
          return { ...a, score };
        })
        .sort((a, b) => b.score - a.score);

      const bestCandidate = candidates[0];
      if (!bestCandidate && !isQaTask) {
        // Check if failure is due to credentials specifically
        const capableButMissingCreds = schedulerAppContext ? freeAgents
          .filter(a => a.remaining > 0 && a.capabilities.some(c => validCaps.includes(c)))
          .filter(a => !checkAgentCredentials(schedulerAppContext, a.available_credentials, credType).ok)
          : [];
        if (capableButMissingCreds.length > 0) {
          const required = credType === 'qa'
            ? (schedulerAppContext.required_qa_credentials || [])
            : (schedulerAppContext.required_credentials || []);
          console.warn(`[DISPATCH] No agent has required credentials for app "${schedulerAppContext.name}": missing ${required.join(', ')}. Capable agents without creds: ${capableButMissingCreds.map(a => a.name).join(', ')}`);
        } else {
          console.log(`[SCHEDULER] No candidate for task ${task.id} ("${task.title.substring(0,30)}") type=${task.type} required=${requiredCapability} last_failed=${task.last_failed_agent} agents=${freeAgents.map(a=>a.name+":"+a.capabilities.join(",")).join("|")}`);
        }
      }

      if (bestCandidate) {
        // Decrement the ORIGINAL agent in freeAgents (not the spread copy)
        const originalAgent = freeAgents.find(a => a.name === bestCandidate.name);
        if (!originalAgent || originalAgent.remaining <= 0) continue;
        
        // Double-check: re-query DB for in-flight tasks this agent has RIGHT NOW
        // Only count tasks in active statuses (not completed/deployed/failed/deprecated)
        const { data: inflightCheck } = await supabase
          .from("agent_tasks")
          .select("id")
          .eq("assigned_agent", bestCandidate.name)
          .in("status", ["in_progress", "qa_testing"])
          .limit(5);
        const inflightCount = inflightCheck?.length || 0;
        if (inflightCount >= originalAgent.max_concurrent) {
          console.log(`[SCHEDULER] ${bestCandidate.name} already has ${inflightCount} in-flight tasks (max ${originalAgent.max_concurrent}), skipping`);
          originalAgent.remaining = 0;
          continue;
        }

        if (isQaTask) {
          // QA assignment: assign agent, keep status as qa_testing
          console.log(`[SCHEDULER] Assigning QA task ${task.id} ("${task.title}") \u2192 ${bestCandidate.name} (qa)`);
          await supabase
            .from("agent_tasks")
            .update({ assigned_agent: bestCandidate.name, qa_agent: bestCandidate.name, error: null })
            .eq("id", task.id);
        } else {
          console.log(`[SCHEDULER] Auto-assigning task ${task.id} ("${task.title}") \u2192 ${bestCandidate.name} (type: ${requiredCapability}, remaining: ${originalAgent.remaining})`);
          await supabase
            .from("agent_tasks")
            .update({ assigned_agent: bestCandidate.name, error: null })
            .eq("id", task.id);
        }
        originalAgent.remaining--;
        assigned++;
      } else {
        const availableTypes = freeAgents.filter(a => a.remaining > 0).map(a => `${a.name}[${a.capabilities.join(',')}]`).join(', ') || 'none';
        const errorMsg = isQaTask
          ? `No QA-capable agent available. Need "qa" capability. Available: ${availableTypes}`
          : `No capable agent for task type "${requiredCapability}". Available agents: ${availableTypes}`;
        console.log(`[SCHEDULER] ${errorMsg} — task ${task.id}`);
        // Write a visible note to the task (not just log)
        const now = new Date().toISOString();
        const existingNotes = task.notes || '';
        const noteEntry = `[${now}] SCHEDULER: ${errorMsg}`;
        const updatedNotes = existingNotes ? `${existingNotes}\n${noteEntry}` : noteEntry;
        await supabase
          .from("agent_tasks")
          .update({ error: errorMsg, notes: updatedNotes })
          .eq("id", task.id);
      }
    }

    if (assigned > 0) {
      console.log(`[SCHEDULER] Assigned ${assigned} tasks this cycle`);
    }
  } catch (e) {
    console.error("[SCHEDULER] Error:", e.message);
  }
}

// Stale agent detection — mark agents degraded if last_heartbeat > 10min, offline if > 30min
// On degrade: requeue any in_progress tasks so they aren't stranded
const STALE_AGENT_INTERVAL = 60_000;
const DEGRADE_THRESHOLD_MS = 10 * 60 * 1000;  // 10 minutes → degraded
const OFFLINE_THRESHOLD_MS = 30 * 60 * 1000;  // 30 minutes → offline

async function requeueAgentTasks(agentName) {
  // Find in_progress tasks assigned to this agent and reset to todo
  const { data: tasks, error } = await supabase
    .from('agent_tasks')
    .select('id, title')
    .eq('assigned_agent', agentName)
    .eq('status', 'in_progress');
  if (error) {
    console.error(`[REQUEUE] Failed to query tasks for ${agentName}:`, error.message);
    return 0;
  }
  let requeued = 0;
  for (const task of (tasks || [])) {
    const { error: updateErr } = await supabase
      .from('agent_tasks')
      .update({ status: 'todo', assigned_agent: null })
      .eq('id', task.id);
    await logTransientError(task.id, `Agent ${agentName} degraded (stale heartbeat) — re-queued for re-dispatch`);
    if (updateErr) {
      console.error(`[REQUEUE] Failed to requeue task ${task.id}:`, updateErr.message);
    } else {
      console.log(`[REQUEUE] Requeued task ${task.id} (${task.title}) from degraded agent ${agentName}`);
      requeued++;
    }
  }
  return requeued;
}

async function staleAgentDetector() {
  try {
    const now = Date.now();
    const degradeThreshold = new Date(now - DEGRADE_THRESHOLD_MS).toISOString();
    const offlineThreshold = new Date(now - OFFLINE_THRESHOLD_MS).toISOString();

    // Phase 1: Mark degraded agents as offline if heartbeat > 30min stale
    const { data: degradedStale } = await supabase
      .from('agent_cards')
      .select('id, name, status, last_heartbeat')
      .eq('status', 'degraded')
      .lt('last_heartbeat', offlineThreshold);
    for (const agent of (degradedStale || [])) {
      if (agent.name.endsWith('-worker')) continue;
      console.log(`[STALE] Marking ${agent.name} as offline (degraded for >30min, last_heartbeat: ${agent.last_heartbeat})`);
      await supabase
        .from('agent_cards')
        .update({ status: 'offline', metadata: { model_health: 'offline', degraded_at: agent.metadata?.degraded_at } })
        .eq('id', agent.id);
    }

    // Phase 2: Mark online agents as degraded if heartbeat > 10min stale
    const { data: onlineStale, error } = await supabase
      .from('agent_cards')
      .select('id, name, status, last_heartbeat')
      .eq('status', 'online')
      .lt('last_heartbeat', degradeThreshold);
    if (error) {
      console.error('[STALE] Failed to check stale agents:', error.message);
      return;
    }
    for (const agent of (onlineStale || [])) {
      // Skip headless workers — they don't have heartbeat crons
      if (agent.name.endsWith('-worker')) continue;
      const reason = `Heartbeat stale since ${agent.last_heartbeat} — possible model API failure (403/429/timeout)`;
      console.log(`[STALE] Marking ${agent.name} as degraded (last_heartbeat: ${agent.last_heartbeat})`);
      await supabase
        .from('agent_cards')
        .update({
          status: 'degraded',
          metadata: { model_health: 'degraded', degraded_at: new Date().toISOString(), reason }
        })
        .eq('id', agent.id)
        .neq('status', 'disabled');

      // CRITICAL: Requeue in_progress tasks from this agent
      const requeued = await requeueAgentTasks(agent.name);
      if (requeued > 0) {
        console.log(`[STALE] Requeued ${requeued} tasks from degraded agent ${agent.name}`);
      }
    }
  } catch (err) {
    console.error('[STALE] Error:', err.message);
  }
}

// --- Auto-Deploy Detection ---
// Polls ArgoCD app sync status via K8s CRD API. When apps are Synced+Healthy,
// moves "completed" tasks to "deployed" if they were completed after the last sync.
const DEPLOY_DETECT_INTERVAL = 60_000; // 60 seconds
// Grace period: wait this long after task completion before checking deploy status
// Gives ArgoCD Image Updater time to detect new image and trigger sync
const DEPLOY_GRACE_PERIOD = 90_000; // 90 seconds

const ARGOCD_URL = process.env.ARGOCD_URL || "http://argocd-server.argocd.svc.cluster.local";
const ARGOCD_USERNAME = process.env.ARGOCD_USERNAME;
const ARGOCD_PASSWORD = process.env.ARGOCD_PASSWORD;
const ARGOCD_TOKEN = process.env.ARGOCD_TOKEN;

async function getArgoAppsViaHTTP() {
  try {
    let token = ARGOCD_TOKEN;
    if (!token) {
      if (!ARGOCD_USERNAME || !ARGOCD_PASSWORD) return [];
      const sessionResp = await fetch(`${ARGOCD_URL}/api/v1/session`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username: ARGOCD_USERNAME, password: ARGOCD_PASSWORD }),
      });
      if (!sessionResp.ok) throw new Error(`Session: ${sessionResp.status}`);
      ({ token } = await sessionResp.json());
    }
    const appsResp = await fetch(`${ARGOCD_URL}/api/v1/applications`, {
      headers: { "Authorization": `Bearer ${token}` },
    });
    if (!appsResp.ok) throw new Error(`Apps: ${appsResp.status}`);
    const data = await appsResp.json();
    return data.items || [];
  } catch (e) {
    console.error("[DEPLOY-DETECT] ArgoCD HTTP API failed:", e.message);
    return [];
  }
}

async function getArgoApps() {
  // Try K8s CRD first (in-cluster RBAC), fallback to ArgoCD HTTP API
  try {
    const resp = await customApi.listNamespacedCustomObject({
      group: "argoproj.io",
      version: "v1alpha1",
      namespace: "argocd",
      plural: "applications",
    });
    const body = resp?.body || resp || {};
    const items = body.items || [];
    if (items.length > 0) return items;
  } catch (e) {
    console.warn("[DEPLOY-DETECT] K8s CRD query failed, trying HTTP API:", e.message);
  }
  return getArgoAppsViaHTTP();
}

function isAppSyncedHealthy(app) {
  const status = app?.status || {};
  const syncStatus = status.sync?.status; // "Synced" | "OutOfSync"
  const healthStatus = status.health?.status; // "Healthy" | "Degraded" | "Progressing"
  return syncStatus === "Synced" && (healthStatus === "Healthy" || healthStatus === "Progressing");
}

function getAppSyncFinishedAt(app) {
  // Get the timestamp when the last sync operation finished
  const opState = app?.status?.operationState;
  if (opState?.finishedAt) return new Date(opState.finishedAt).getTime();
  // Fallback: reconciledAt
  if (app?.status?.reconciledAt) return new Date(app.status.reconciledAt).getTime();
  return null;
}

async function autoDeployDetector() {
  try {
    // 1. Get ArgoCD apps and check if they're synced+healthy
    const apps = await getArgoApps();
    console.log(`[DEPLOY-DETECT] Found ${apps.length} ArgoCD apps`);
    if (!apps.length) {
      return;
    }

    // Check dev app specifically (primary deploy target)
    const devApp = apps.find(a => a.metadata?.name === "dev");
    const prodApp = apps.find(a => a.metadata?.name === "prod");

    const devSynced = devApp && isAppSyncedHealthy(devApp);
    const prodSynced = prodApp && isAppSyncedHealthy(prodApp);
    console.log(`[DEPLOY-DETECT] dev: ${devSynced ? 'Synced+Healthy' : 'not ready'}, prod: ${prodSynced ? 'Synced+Healthy' : 'not ready'}`);

    // Even if neither env is synced, continue — fallback logic handles Unknown state

    const devSyncTime = devApp ? getAppSyncFinishedAt(devApp) : null;
    const prodSyncTime = prodApp ? getAppSyncFinishedAt(prodApp) : null;

    // 2. Query completed tasks (QA passed) that haven't been marked as deployed yet
    const { data: completedTasks, error } = await supabase
      .from("agent_tasks")
      .select("id, title, status, completed_at, result, type, repository_id, deploy_target, pull_request_url, repository_url")
      .eq("status", "completed")
      .order("completed_at", { ascending: true });

    if (error) {
      console.error("[DEPLOY-DETECT] Error querying completed tasks:", error.message);
      return;
    }

    if (!completedTasks?.length) return;

    let deployed = 0;

    for (const task of completedTasks) {
      const completedAt = task.completed_at ? new Date(task.completed_at).getTime() : null;
      if (!completedAt) continue;

      // Grace period: don't try to deploy-detect tasks that just completed
      // Give CI + ArgoCD Image Updater time to push and sync
      if (Date.now() - completedAt < DEPLOY_GRACE_PERIOD) continue;

      // Determine if this task's code changes are deployed:
      // Strategy depends on deploy_target:
      // - vercel: PR merged → Vercel auto-deploys — no ArgoCD involved
      // - kubernetes (default): ArgoCD sync finished AFTER task completed
      const isCodingTask = task.type === "coding";
      const deployTarget = task.deploy_target || "kubernetes";
      
      // For non-coding tasks (ops, research, etc.), they don't need deploy detection —
      // mark them as deployed immediately since there's nothing to deploy
      if (!isCodingTask) {
        console.log(`[DEPLOY-DETECT] Task ${task.id} ("${task.title.slice(0, 40)}") is non-coding (${task.type}) → deployed`);
        await supabase.from("agent_tasks").update({
          status: "deployed",
          error: null,
        }).eq("id", task.id);
        deployed++;
        continue;
      }

      // Extract PR number, commit, and repo from task result + fields
      let commitRef = null;
      let prNumber = null;
      let repoFullName = null;

      // Check pull_request_url field first (most reliable)
      const taskPrUrl = Array.isArray(task.pull_request_url) ? task.pull_request_url[0] : task.pull_request_url;
      if (taskPrUrl) {
        const m = taskPrUrl.match(/github\.com\/([^/]+\/[^/]+)\/pull\/(\d+)/);
        if (m) { repoFullName = m[1]; prNumber = parseInt(m[2]); commitRef = `PR #${prNumber}`; }
      }

      if (task.result) {
        const resultStr = typeof task.result === "string" ? task.result : JSON.stringify(task.result);
        // Match PR numbers like "PR #13" or "PR#13" or "#13 merged"
        if (!prNumber) {
          const prMatch = resultStr.match(/PR\s*#(\d+)/i) || resultStr.match(/#(\d+)\s*merged/i);
          if (prMatch) { prNumber = parseInt(prMatch[1]); commitRef = `PR #${prNumber}`; }
        }
        // Match commit SHAs
        const shaMatch = resultStr.match(/\b([0-9a-f]{7,40})\b/);
        if (!commitRef && shaMatch) commitRef = shaMatch[1].slice(0, 7);
        // Extract repo name from result (e.g. "dante-alpha-assistant/queue-dashboard")
        if (!repoFullName) {
          const repoMatch = resultStr.match(/(dante-alpha-assistant\/[\w-]+)/);
          if (repoMatch) repoFullName = repoMatch[1];
        }
      }

      // For Vercel-targeted coding tasks: Vercel auto-deploys when a PR is merged.
      // We don't need ArgoCD sync — just verify the PR is merged, then mark deployed
      // and resolve the deployment URL.
      if (deployTarget === 'vercel') {
        if (!prNumber) {
          console.log(`[DEPLOY-DETECT] Task ${task.id} (vercel, "${task.title.slice(0, 40)}") no PR ref — skipping`);
          continue;
        }

        // Resolve repo if not yet known
        if (!repoFullName && task.repository_id) {
          const { data: repo } = await supabase.from('agent_repositories').select('full_name').eq('id', task.repository_id).single();
          if (repo) repoFullName = repo.full_name;
        }
        if (!repoFullName && task.repository_url) {
          const m = task.repository_url.match(/github\.com\/([^/]+\/[^/]+?)(?:\.git)?$/);
          if (m) repoFullName = m[1];
        }

        const KNOWN_REPOS = ['dante-alpha-assistant/queue-dashboard', 'dante-alpha-assistant/task-dispatcher'];
        const reposToCheck = repoFullName ? [repoFullName] : KNOWN_REPOS;
        let prMerged = false;
        for (const repo of reposToCheck) {
          try {
            const ghResp = await fetch(`https://api.github.com/repos/${repo}/pulls/${prNumber}`, {
              headers: { 'Authorization': `token ${process.env.GH_TOKEN}`, 'Accept': 'application/vnd.github.v3+json' },
              signal: AbortSignal.timeout(5000),
            });
            if (ghResp.ok) {
              const pr = await ghResp.json();
              if (pr.merged && pr.merged_at) {
                repoFullName = repo;
                prMerged = true;
                break;
              } else if (pr.state === 'open') {
                console.log(`[DEPLOY-DETECT] Task ${task.id} (vercel) PR #${prNumber} on ${repo} is open — waiting for merge`);
                break;
              }
            }
          } catch (ghErr) {
            console.warn(`[DEPLOY-DETECT] GitHub API error for ${repo}#${prNumber}:`, ghErr.message);
          }
        }

        if (prMerged) {
          console.log(`[DEPLOY-DETECT] Task ${task.id} (vercel, "${task.title.slice(0, 40)}") PR merged → deployed`);
          // Resolve the Vercel deployment URL (API or fallback to https://{repo}.vercel.app)
          let deploymentUrl = null;
          if (repoFullName) {
            try {
              deploymentUrl = await getVercelDeploymentUrl(repoFullName, 'main');
            } catch (e) {
              console.warn(`[DEPLOY-DETECT] Could not resolve Vercel URL for ${repoFullName}:`, e.message);
            }
          }
          const updateData = { status: 'deployed', error: null };
          if (deploymentUrl) updateData.deployment_url = deploymentUrl;
          await supabase.from("agent_tasks").update(updateData).eq("id", task.id);
          deployed++;

          broadcast("task:status", {
            taskId: task.id, status: "deployed", previousStatus: "completed",
            title: task.title, environment: "vercel", commitRef,
            deploymentUrl, timestamp: new Date().toISOString(),
          }, task.id);
        }
        continue; // Done handling this Vercel task — skip ArgoCD logic below
      }

      // For coding tasks: MUST verify the specific PR was merged via GitHub API
      // Don't rely solely on ArgoCD sync time — that catches unrelated deploys
      let isDeployed = false;
      let deployEnv = null;

      if (prNumber) {
        // Look up repo from task's repository_id or fall back to extracted repo
        if (!repoFullName && task.repository_id) {
          const { data: repo } = await supabase.from('agent_repositories').select('full_name').eq('id', task.repository_id).single();
          if (repo) repoFullName = repo.full_name;
        }
        // If no repo found, try all known repos
        const KNOWN_REPOS = ['dante-alpha-assistant/queue-dashboard', 'dante-alpha-assistant/task-dispatcher'];
        const reposToCheck = repoFullName ? [repoFullName] : KNOWN_REPOS;

        for (const repo of reposToCheck) {
          try {
            const ghResp = await fetch(`https://api.github.com/repos/${repo}/pulls/${prNumber}`, {
              headers: { 'Authorization': `token ${process.env.GH_TOKEN}`, 'Accept': 'application/vnd.github.v3+json' },
              signal: AbortSignal.timeout(5000),
            });
            if (ghResp.ok) {
              const pr = await ghResp.json();
              if (pr.merged && pr.merged_at) {
                repoFullName = repo;
                const mergedAt = new Date(pr.merged_at).getTime();
                if (devSynced && devSyncTime && devSyncTime > mergedAt) {
                  isDeployed = true;
                  deployEnv = "dev";
                }
                if (prodSynced && prodSyncTime && prodSyncTime > mergedAt) {
                  isDeployed = true;
                  deployEnv = deployEnv ? "dev+prod" : "prod";
                }
                if (!isDeployed) {
                  console.log(`[DEPLOY-DETECT] Task ${task.id} PR #${prNumber} merged on ${repo} but ArgoCD hasn't synced since merge yet`);
                }
                break; // Found merged PR, stop checking other repos
              } else if (pr.state === 'open') {
                console.log(`[DEPLOY-DETECT] Task ${task.id} PR #${prNumber} on ${repo} is open — waiting for QA agent to merge`);
                break;
              }
            }
          } catch (ghErr) {
            console.warn(`[DEPLOY-DETECT] GitHub API error for ${repo}#${prNumber}:`, ghErr.message);
          }
        }
      } else {
        // No PR number in result — cannot verify deployment
        // Do NOT auto-promote based on ArgoCD sync alone (produces false positives)
        // Tasks without PR refs stay in completed until manually promoted
        console.log(`[DEPLOY-DETECT] Task ${task.id} ("${task.title.slice(0, 40)}") no PR ref — skipping (manual promotion required)`);
      }

      if (isDeployed) {
        console.log(`[DEPLOY-DETECT] Task ${task.id} ("${task.title.slice(0, 40)}") → deployed (${deployEnv}${commitRef ? `, ref: ${commitRef}` : ""})`);
        await supabase.from("agent_tasks").update({
          status: "deployed",
          error: null,
        }).eq("id", task.id);
        deployed++;

        // Broadcast to SSE clients
        broadcast("task:status", {
          taskId: task.id,
          status: "deployed",
          previousStatus: "completed",
          title: task.title,
          environment: deployEnv,
          commitRef,
          timestamp: new Date().toISOString(),
        }, task.id);
      }
    }

    if (deployed > 0) {
      console.log(`[DEPLOY-DETECT] Moved ${deployed} task(s) to deployed`);
    }
  } catch (e) {
    console.error("[DEPLOY-DETECT] Error:", e.message);
  }
}

// --- Start ---
subscribe();

// Auto-scheduler
setInterval(scheduler, SCHEDULER_INTERVAL);
// Delay initial scheduler cycle by 5s to ensure Realtime subscription is ready
setTimeout(scheduler, 5000);
console.log(`[BOOT] Auto-scheduler running every ${SCHEDULER_INTERVAL / 1000}s`);

// Unified task monitor (replaces watchdog + session checker)
setInterval(taskMonitor, MONITOR_INTERVAL);
setTimeout(taskMonitor, 5000);
setTimeout(detectOrphanedTasks, 3000); // Detect tasks orphaned by previous restart // Run 5s after boot (let realtime connect first)
console.log(`[BOOT] Task monitor running every ${MONITOR_INTERVAL / 1000}s (hard timeout: ${TASK_HARD_TIMEOUT / 60000}min, grace: ${SESSION_GONE_GRACE / 1000}s)`);

// QA Auto-Scaler — DISABLED: scheduler handles QA assignment via agent_cards
// The QA scaler used K8s pod names instead of agent names, causing dispatch failures.
// setInterval(qaAutoScaler, 30000); // Disabled — using persistent beta-worker replicas
console.log(`[BOOT] QA auto-scaler ENABLED — spawns ephemeral workers when QA queue > 1 (max ${MAX_QA_WORKERS})`);

// Stale agent detector
setInterval(staleAgentDetector, STALE_AGENT_INTERVAL);
setTimeout(staleAgentDetector, 10000);
console.log("[BOOT] Stale agent detector running every 60s (threshold: 10min)");

// ==========================================
// Credential Poller — poll agents on startup to sync available_credentials
// Queries each online agent's gateway for known env vars (existence only)
// Runs once on boot, then every 30 minutes
// ==========================================
const KNOWN_CREDENTIAL_VARS = [
  "GH_TOKEN", "SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_MGMT_TOKEN",
  "VERCEL_TOKEN", "KUBECONFIG", "ANTHROPIC_API_KEY", "OPENROUTER_API_KEY",
  "DOCKER_TOKEN", "NPM_TOKEN", "AWS_ACCESS_KEY_ID",
];

async function pollAgentCredentials() {
  try {
    const { data: agents, error } = await supabase
      .from("agent_cards")
      .select("id, name, status, webhook_url")
      .in("status", ["online", "degraded"]);

    if (error || !agents?.length) return;

    for (const agent of agents) {
      try {
        const agentConfig = AGENTS[agent.name] || AGENTS[agent.id];
        if (!agentConfig?.gatewayToken) continue;

        // Use the agent's gateway URL to query sessions — if the agent is reachable,
        // ask it to report credentials via a lightweight webhook
        const baseUrl = (agentConfig.url || "").replace(/\/hooks\/agent$/, "");
        if (!baseUrl) continue;

        // Send a credential check request via the agent's hooks endpoint
        // The agent's OpenClaw gateway will process this as a system event
        const checkPayload = {
          type: "credential_check",
          credentials_to_check: KNOWN_CREDENTIAL_VARS,
          callback_url: `http://task-dispatcher.infra.svc.cluster.local:${PORT}/api/agents/register-credentials`,
        };

        const resp = await fetch(`${baseUrl}/hooks/credential-check`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${agentConfig.token}`,
          },
          body: JSON.stringify(checkPayload),
          signal: AbortSignal.timeout(5000),
        }).catch(() => null);

        // If the agent doesn't support the credential-check hook (404), that's fine
        // Agents will self-register via their own startup scripts
        if (resp?.ok) {
          console.log(`[CRED-POLL] ${agent.name}: credential check request sent`);
        }
      } catch (e) {
        // Silently skip agents that can't be reached
      }
    }
  } catch (e) {
    console.warn("[CRED-POLL] Error polling agent credentials:", e.message);
  }
}

// Run credential poll 15s after boot, then every 30 minutes
setTimeout(pollAgentCredentials, 15000);
setInterval(pollAgentCredentials, 30 * 60 * 1000);
console.log("[BOOT] Credential poller — runs on boot + every 30min");

// DISABLED: deploy-detect produces false positives (marks tasks deployed before image is live)
// Deployment will be manual via Deploy button on dashboard. Re-enable when button is ready.
// setInterval(autoDeployDetector, DEPLOY_DETECT_INTERVAL);
// setTimeout(autoDeployDetector, 15000);
console.log(`[BOOT] Auto-deploy detector DISABLED — manual deployment only`);

// ==========================================
// Health Checker — detect tasks in suspicious/inconsistent states
// Runs every 5 minutes, flags tasks via metadata.health_flags
// ==========================================
const HEALTH_CHECK_INTERVAL = 5 * 60_000; // 5 minutes

async function healthChecker() {
  try {
    // Fetch all non-terminal active tasks + recently completed/deployed
    const { data: tasks, error } = await supabase
      .from("agent_tasks")
      .select("id, title, status, type, assigned_agent, qa_agent, error, started_at, completed_at, updated_at, metadata, pull_request_url, result")
      .in("status", ["todo", "in_progress", "running", "qa_testing", "completed", "deployed"]);

    if (error) {
      console.error("[HEALTH] Failed to fetch tasks:", error.message);
      return;
    }
    if (!tasks || tasks.length === 0) return;

    const now = Date.now();
    let flagged = 0;
    let critical = 0;

    for (const task of tasks) {
      const flags = [];
      const meta = task.metadata || {};
      const qaResult = meta.qa_result || (task.result?.qa_result) || null;
      const qaRetries = meta.qa_retries || 0;
      const hasActiveError = !!task.error;
      const startedMs = task.started_at ? new Date(task.started_at).getTime() : 0;
      const updatedMs = task.updated_at ? new Date(task.updated_at).getTime() : 0;

      // 1. completed + active error + no qa_result.passed
      if (task.status === "completed" && hasActiveError && (!qaResult || !qaResult.passed)) {
        flags.push({ code: "COMPLETED_WITH_ERROR", severity: "warning", message: "Completed with active error and no QA pass" });
      }

      // 2. deployed + active error (CRITICAL)
      if (task.status === "deployed" && hasActiveError) {
        flags.push({ code: "DEPLOYED_WITH_ERROR", severity: "critical", message: "Deployed with an unresolved error" });
      }

      // 3. in_progress + no assigned_agent
      if ((task.status === "in_progress" || task.status === "running") && !task.assigned_agent) {
        flags.push({ code: "IN_PROGRESS_UNASSIGNED", severity: "warning", message: "In progress with no assigned agent" });
      }

      // 4. qa_testing + no qa_agent for >10 min
      if (task.status === "qa_testing" && !task.qa_agent) {
        const qaWaitMs = now - updatedMs;
        if (qaWaitMs > 10 * 60_000) {
          flags.push({ code: "QA_STUCK_NO_AGENT", severity: "warning", message: `QA testing with no reviewer for ${Math.round(qaWaitMs / 60_000)}min` });
        }
      }

      // 5. in_progress + started > 60 min ago + no recent update (zombie)
      if ((task.status === "in_progress" || task.status === "running") && startedMs) {
        const elapsed = now - startedMs;
        const sinceUpdate = now - updatedMs;
        if (elapsed > 60 * 60_000 && sinceUpdate > 30 * 60_000) {
          flags.push({ code: "ZOMBIE_TASK", severity: "warning", message: `In progress for ${Math.round(elapsed / 60_000)}min, no update for ${Math.round(sinceUpdate / 60_000)}min` });
        }
      }

      // 6. completed + coding type + no pull_request_url
      if (task.status === "completed" && task.type === "coding") {
        const prUrl = task.pull_request_url;
        const hasPr = prUrl && (Array.isArray(prUrl) ? prUrl.length > 0 : !!prUrl);
        if (!hasPr) {
          flags.push({ code: "CODING_NO_PR", severity: "warning", message: "Coding task completed without a PR URL" });
        }
      }

      // 7. todo + assigned_agent not null
      if (task.status === "todo" && task.assigned_agent) {
        flags.push({ code: "TODO_ASSIGNED", severity: "info", message: "Assigned but still in todo status" });
      }

      // 8. completed + qa_retries >= 2 + qa_result.passed = false
      if (task.status === "completed" && qaRetries >= 2 && qaResult && qaResult.passed === false) {
        flags.push({ code: "COMPLETED_QA_FAILED", severity: "warning", message: `Completed despite ${qaRetries} QA retries with failed result` });
      }

      // Compare with existing flags — only update if changed
      const existingFlags = meta.health_flags || [];
      const existingCodes = new Set(existingFlags.map(f => f.code));
      const newCodes = new Set(flags.map(f => f.code));
      const changed = flags.length !== existingFlags.length ||
        flags.some(f => !existingCodes.has(f.code)) ||
        existingFlags.some(f => !newCodes.has(f.code));

      if (changed) {
        const updatedMeta = { ...meta, health_flags: flags, health_checked_at: new Date().toISOString() };
        if (flags.length === 0) delete updatedMeta.health_flags; // Clean up when resolved
        await supabase.from("agent_tasks").update({ metadata: updatedMeta }).eq("id", task.id);

        if (flags.length > 0) {
          flagged++;
          const flagSummary = flags.map(f => `${f.severity === "critical" ? "🔴" : "⚠️"} ${f.code}: ${f.message}`).join(", ");
          console.log(`[HEALTH] Task ${task.id} ("${task.title?.slice(0, 40)}"): ${flagSummary}`);

          // Broadcast health flag update via SSE
          broadcast("task:health", { taskId: task.id, flags, title: task.title }, task.id);

          // Discord alert for critical violations
          const criticalFlags = flags.filter(f => f.severity === "critical");
          if (criticalFlags.length > 0) {
            critical++;
            console.warn(`[HEALTH] 🔴 CRITICAL: Task ${task.id} — ${criticalFlags.map(f => f.message).join("; ")}`);
          }
        } else if (existingFlags.length > 0) {
          console.log(`[HEALTH] Task ${task.id} ("${task.title?.slice(0, 40)}"): flags cleared ✅`);
          broadcast("task:health", { taskId: task.id, flags: [], title: task.title }, task.id);
        }
      }
    }

    if (flagged > 0) {
      console.log(`[HEALTH] Scan complete — ${flagged} task(s) flagged, ${critical} critical`);
    }
  } catch (e) {
    console.error("[HEALTH] Health checker error:", e.message);
  }
}

// Health checker — every 5 minutes
setInterval(healthChecker, HEALTH_CHECK_INTERVAL);
setTimeout(healthChecker, 15000); // First run 15s after boot
console.log(`[BOOT] Health checker running every ${HEALTH_CHECK_INTERVAL / 1000}s`);

// Graceful shutdown
process.on("SIGTERM", () => {
  console.log("[SHUTDOWN] Received SIGTERM");
  process.exit(0);
});

// ==========================================
// Coding Worker Auto-Scaler
// Spawns ephemeral K8s Jobs when todo coding queue exceeds capacity
// ==========================================

const MAX_CODING_WORKERS = 5;
const CODING_SCALER_INTERVAL = 30000; // 30 seconds
const CODING_WORKER_HOOKS_TOKEN = "ephemeral-coding-worker-tok-2026";

async function codingAutoScaler() {
  try {
    // 1. Count unassigned todo coding tasks waiting > 2 min
    const twoMinAgo = new Date(Date.now() - 2 * 60 * 1000).toISOString();
    const { data: todoTasks, error } = await supabase
      .from("agent_tasks")
      .select("id, title, type, created_at")
      .eq("status", "todo")
      .in("type", ["coding", "ops", "general"])
      .is("assigned_agent", null)
      .neq("paused", true)
      .lt("created_at", twoMinAgo);

    if (error) {
      console.error("[CODING-SCALER] Error querying queue:", error.message);
      return;
    }

    const queueDepth = todoTasks?.length || 0;

    // 2. Count active ephemeral coding worker jobs
    const jobListResp = await batchApi.listNamespacedJob({
      namespace: "agents",
      labelSelector: "role=coding-worker,managed-by=task-dispatcher",
    });
    const jobList = jobListResp?.body || jobListResp || {};
    const activeJobs = (jobList.items || []).filter(
      (j) => !j.status?.succeeded && !j.status?.failed
    );
    const activeWorkers = activeJobs.length;

    if (queueDepth > 0 || activeWorkers > 0) {
      console.log(`[CODING-SCALER] Queue: ${queueDepth} todo tasks, Active ephemeral workers: ${activeWorkers}`);
    }

    // 3. Spawn workers if needed
    if (queueDepth > 0 && activeWorkers < MAX_CODING_WORKERS) {
      const toSpawn = Math.min(queueDepth, MAX_CODING_WORKERS - activeWorkers);
      for (let i = 0; i < toSpawn; i++) {
        const task = todoTasks[i];
        if (task) {
          await spawnCodingWorker(task);
        }
      }
    }

    // 4. Clean up completed/failed jobs older than 5 min
    for (const job of jobList.items || []) {
      if (job.status?.succeeded || job.status?.failed) {
        const finishTime = job.status.completionTime || job.status.conditions?.[0]?.lastTransitionTime;
        if (finishTime && Date.now() - new Date(finishTime).getTime() > 5 * 60 * 1000) {
          try {
            await batchApi.deleteNamespacedJob({
              name: job.metadata.name,
              namespace: "agents",
              propagationPolicy: "Background",
            });
            console.log(`[CODING-SCALER] Cleaned up job ${job.metadata.name}`);
          } catch (e) {
            console.error(`[CODING-SCALER] Failed to clean up job ${job.metadata.name}:`, e.message);
          }
        }
      }
    }
  } catch (e) {
    console.error("[CODING-SCALER] Error:", e.message);
  }
}

async function spawnCodingWorker(task) {
  const workerName = `coding-worker-${Date.now().toString(36)}`;
  const hooksToken = CODING_WORKER_HOOKS_TOKEN;
  console.log(`[CODING-SCALER] Spawning ${workerName} for task ${task.id.slice(0,8)} ("${task.title.slice(0,40)}")`);

  // The init-config script generates openclaw.json inline
  const initConfigScript = `
const fs = require('fs');
const e = process.env;
const config = {
  gateway: {
    port: 18789,
    mode: "local",
    bind: "lan",
    auth: { mode: "token", token: e.OPENCLAW_GATEWAY_TOKEN || "ephemeral-gw-tok" },
    tools: { allow: ["sessions_spawn","sessions_send","sessions_list","sessions_history","session_status"] },
    controlUi: { dangerouslyAllowHostHeaderOriginFallback: true },
  },
  models: {
    providers: {
      anthropic: {
        baseUrl: "https://api.anthropic.com",
        models: [{ id: "claude-opus-4-6", name: "claude-opus-4-6", api: "anthropic-messages", reasoning: true, input: ["text","image"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 200000, maxTokens: 16384 }]
      },
      openrouter: {
        baseUrl: "https://openrouter.ai/api/v1",
        apiKey: e.OPENROUTER_API_KEY || "",
        models: [
          { id: "moonshotai/kimi-k2.5", name: "kimi-k2.5", api: "openai-completions", reasoning: true, input: ["text","image"], cost: { input: 0.45, output: 2.25, cacheRead: 0.07, cacheWrite: 0 }, contextWindow: 262144, maxTokens: 8192 },
        ]
      }
    }
  },
  agents: {
    defaults: {
      model: { primary: "openrouter/moonshotai/kimi-k2.5" },
      workspace: "/root/.openclaw/workspace",
      compaction: { mode: "safeguard" },
      maxConcurrent: 2,
      subagents: { maxConcurrent: 4 },
    }
  },
  hooks: { enabled: true, token: "${hooksToken}", allowRequestSessionKey: true, defaultSessionKey: "hook:default", allowedSessionKeyPrefixes: ["hook:"] },
  tools: { sessions: { visibility: "all" } }
};
fs.mkdirSync('/root/.openclaw/workspace/skills/task-worker', { recursive: true });
fs.mkdirSync('/root/.openclaw/workspace/skills/coding-task', { recursive: true });
fs.mkdirSync('/root/.openclaw/workspace/skills/deploy-batch', { recursive: true });
fs.writeFileSync('/root/.openclaw/openclaw.json', JSON.stringify(config, null, 2));

// Write task-worker skill
fs.writeFileSync('/root/.openclaw/workspace/skills/task-worker/SKILL.md', \`# task-worker — Dispatched Task Execution Skill

## When to Use
This skill applies to EVERY message from "Task Dispatcher" that contains a JSON task payload.

## Git Workflow
1. Clone the repo to /tmp/<repo-name> (ephemeral worker — no persistent workspace)
2. git checkout main && git pull origin main
3. git checkout -b task/<task-id-first-8-chars>
4. Make your changes
5. git add -A && git commit -m "<type>: <description> [task:<id>]"
6. git push origin HEAD
7. Create PR: gh pr create --title "<title>" --body "Task: <id>" --base main

## GitHub Auth
Use GH_TOKEN env var: git clone https://x-access-token:\\\${GH_TOKEN}@github.com/<owner>/<repo>.git

## MANDATORY: Blocked Detection
- NEVER mark done if manual steps remain
- NEVER write "apply this manually" — block the task instead
- Use curl, kubectl, gh CLI FIRST before blocking
\`);

// Write deploy-batch skill (read from bundled file or write inline)
try {
  const skillPath = require('path').join(__dirname, 'skills', 'deploy-batch.md');
  if (fs.existsSync(skillPath)) {
    fs.writeFileSync('/root/.openclaw/workspace/skills/deploy-batch/SKILL.md', fs.readFileSync(skillPath, 'utf8'));
  }
} catch(e) { console.error('deploy-batch skill write failed:', e.message); }

// Write coding-task skill  
fs.writeFileSync('/root/.openclaw/workspace/skills/coding-task/SKILL.md', \`# coding-task Skill

## Steps
1. Parse the task (extract task_id, title, description, repo)
2. Setup git: git config user.email "neo-ephemeral@openclaw.ai" && git config user.name "Neo (ephemeral)"
3. Clone repo to /tmp: cd /tmp && git clone https://x-access-token:\\\${GH_TOKEN}@github.com/{owner}/{repo}.git
4. Create branch: git checkout -b task/{short-task-id}
5. Make changes — keep focused on what the task asks
6. Commit and push: git add -A && git commit -m "{type}: {description} [task:{id}]" && git push -u origin HEAD
7. Create PR: gh pr create --title "{task title}" --body "Task: {task_id}" --base main
8. Update task status with the curl command from the dispatch payload

## NEVER:
- Edit files without cloning the repo first
- Commit to main directly
- Mark done if manual steps remain — set status to blocked instead
\`);

// Write AGENTS.md
fs.writeFileSync('/root/.openclaw/workspace/AGENTS.md', \`# Ephemeral Coding Worker
You are a temporary coding agent. Complete the assigned task, push a PR, update status, then exit.
Do NOT create memory files or heartbeats. You are ephemeral.
\`);

console.log("Config + skills written for ephemeral coding worker");
`;

  const job = {
    apiVersion: "batch/v1",
    kind: "Job",
    metadata: {
      name: workerName,
      namespace: "agents",
      labels: {
        app: "coding-worker",
        role: "coding-worker",
        "managed-by": "task-dispatcher",
        "task-id": task.id.slice(0, 8),
      },
    },
    spec: {
      backoffLimit: 0,
      ttlSecondsAfterFinished: 300,
      activeDeadlineSeconds: 1800, // 30 min max
      template: {
        metadata: {
          labels: {
            app: "coding-worker",
            role: "coding-worker",
            "managed-by": "task-dispatcher",
          },
        },
        spec: {
          restartPolicy: "Never",
          initContainers: [
            {
              name: "init-tools",
              image: "debian:bookworm-slim",
              command: ["/bin/bash", "-c"],
              args: [
                "apt-get update -qq && apt-get install -y -qq curl ca-certificates > /dev/null 2>&1 && " +
                "KUBE_VER=v1.31.0 && curl -sL \"https://dl.k8s.io/release/${KUBE_VER}/bin/linux/amd64/kubectl\" -o /tools/kubectl && " +
                "chmod +x /tools/kubectl && echo 'kubectl ready'"
              ],
              volumeMounts: [{ name: "tools-bin", mountPath: "/tools" }],
            },
            {
              name: "init-config",
              image: "node:22-bookworm-slim",
              command: ["node", "-e"],
              args: [initConfigScript],
              envFrom: [{ secretRef: { name: "neo-worker-env" } }],
              volumeMounts: [{ name: "workspace", mountPath: "/root/.openclaw" }],
            },
          ],
          containers: [
            {
              name: "openclaw",
              image: process.env.QA_WORKER_IMAGE || "ghcr.io/dante-alpha-assistant/openclaw-agent:latest",
              env: [
                { name: "PATH", value: "/tools:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" },
              ],
              envFrom: [{ secretRef: { name: "neo-worker-env" } }],
              ports: [{ containerPort: 18789 }],
              resources: {
                requests: { cpu: "200m", memory: "512Mi" },
                limits: { cpu: "2", memory: "2Gi" },
              },
              volumeMounts: [
                { name: "workspace", mountPath: "/root/.openclaw" },
                { name: "tools-bin", mountPath: "/tools" },
              ],
            },
          ],
          volumes: [
            { name: "workspace", emptyDir: {} },
            { name: "tools-bin", emptyDir: {} },
          ],
        },
      },
    },
  };

  try {
    await batchApi.createNamespacedJob({ namespace: "agents", body: job });
    console.log(`[CODING-SCALER] Spawned job: ${workerName}`);

    // Pre-assign task to prevent other agents from picking it up
    await supabase.from("agent_tasks").update({
      assigned_agent: workerName,
      status: "assigned",
    }).eq("id", task.id);

    // Wait for pod to get an IP (poll for up to 60s)
    let podIp = null;
    for (let i = 0; i < 12; i++) {
      await new Promise(r => setTimeout(r, 5000));
      try {
        const podsResp = await coreApi.listNamespacedPod({
          namespace: "agents",
          labelSelector: `job-name=${workerName}`,
        });
        const pods = podsResp?.body?.items || podsResp?.items || [];
        const runningPod = pods.find(p => p.status?.phase === "Running" && p.status?.podIP);
        if (runningPod) {
          podIp = runningPod.status.podIP;
          break;
        }
      } catch (e) {
        console.warn(`[CODING-SCALER] Pod poll error: ${e.message}`);
      }
    }

    if (!podIp) {
      console.error(`[CODING-SCALER] ${workerName} pod did not get IP within 60s — task ${task.id.slice(0,8)} will be re-queued`);
      await supabase.from("agent_tasks").update({
        assigned_agent: null,
        status: "todo",
        error: `Ephemeral worker ${workerName} failed to start`,
      }).eq("id", task.id);
      return;
    }

    console.log(`[CODING-SCALER] ${workerName} running at ${podIp} — dispatching task ${task.id.slice(0,8)}`);

    // Register as temporary agent for dispatch
    AGENTS[workerName] = {
      url: `http://${podIp}:18789/hooks/agent`,
      token: hooksToken,
      gatewayToken: "ephemeral-gw-tok",
    };

    // Dispatch the task
    const taskToDispatch = { ...task, status: "assigned", assigned_agent: workerName };
    const fullTask = await supabase.from("agent_tasks").select("*").eq("id", task.id).single();
    if (fullTask.data) {
      await dispatchToAgent({ ...fullTask.data, assigned_agent: workerName });
    }

    // Clean up AGENTS entry after 35 min (job TTL is 30 min)
    setTimeout(() => {
      delete AGENTS[workerName];
      console.log(`[CODING-SCALER] Cleaned up AGENTS entry for ${workerName}`);
    }, 35 * 60 * 1000);

  } catch (e) {
    console.error(`[CODING-SCALER] Failed to spawn ${workerName}: ${e.message}`);
    // Reset task
    await supabase.from("agent_tasks").update({
      assigned_agent: null,
      status: "todo",
    }).eq("id", task.id);
  }
}

// Start coding auto-scaler
// DISABLED: persistent worker replicas replace ephemeral pods
// setInterval(codingAutoScaler, CODING_SCALER_INTERVAL);

// Start merge queue processor
// DISABLED: Merge queue auto-merges completed tasks — we use manual Deploy/Deploy All buttons instead
// startMergeQueue(supabase, logTaskActivity);

// Flush Langfuse events every 60s
setInterval(() => flushLangfuse().catch(() => {}), 60000);
console.log(`[BOOT] Coding auto-scaler running every ${CODING_SCALER_INTERVAL / 1000}s (max ${MAX_CODING_WORKERS} ephemeral workers)`);


// === AUTO-BLOCKER DETECTION ===
// When a task fails, check the error text for common blocker patterns.
// If a pattern matches, convert failed → blocked with structured metadata.
async function autoDetectBlocker(task) {
  if (!task || task.status !== 'failed') return;

  // Combine all error sources for pattern matching
  const errorText = [
    typeof task.error === 'string' ? task.error : (task.error ? JSON.stringify(task.error) : ''),
    task.blocked_reason || '',
    task.result?.summary || '',
    typeof task.result === 'string' ? task.result : '',
  ].filter(Boolean).join(' ');

  if (!errorText.trim()) return;

  const detection = detectBlockerPattern(errorText);
  if (!detection) return;

  const blockerMeta = buildBlockerMetadata(detection);
  const blockedReason = `Auto-detected: ${detection.pattern.title} — ${detection.details.slice(0, 200)}`;

  console.log(`[AUTO-BLOCKER] Task ${task.id} ("${(task.title || '').slice(0, 40)}") failed → blocked (${detection.pattern.type}): ${detection.details.slice(0, 100)}`);

  // Update task: failed → blocked with structured blocker metadata
  const metadata = task.metadata || {};
  metadata.blocker = blockerMeta;

  const { error: updateErr } = await supabase
    .from('agent_tasks')
    .update({
      status: 'blocked',
      blocked_reason: blockedReason,
      metadata,
    })
    .eq('id', task.id)
    .eq('status', 'failed'); // Only update if still failed (avoid race conditions)

  if (updateErr) {
    console.error(`[AUTO-BLOCKER] Failed to update task ${task.id}:`, updateErr.message);
    return;
  }

  // Log the auto-detection in activity log
  await logTaskActivity(
    task.id,
    'auto_blocker_detected',
    'failed',
    `blocked (${detection.pattern.type}): ${blockedReason.slice(0, 300)}`,
    'dispatcher'
  );
}

// === AUTO-REBASE DETECTION ===
// When a task fails QA due to merge conflicts, set rebase metadata
// so the next dispatch includes rebase instructions
async function detectAndSetRebaseMetadata(task) {
  if (!task || task.status !== 'failed') return;
  
  const qaResult = task.qa_result;
  if (!qaResult) return;
  
  const failureText = JSON.stringify(qaResult).toLowerCase();
  if (!failureText.includes('merge conflict') && !failureText.includes('mergestatestatus') && !failureText.includes('dirty')) return;
  
  // Extract PR info from pull_request_url
  const prUrls = task.pull_request_url || [];
  if (!prUrls.length) return;
  
  const prUrl = prUrls[0];
  const prMatch = prUrl.match(/github\.com\/([\w-]+\/[\w-]+)\/pull\/(\d+)/);
  if (!prMatch) return;
  
  const repo = prMatch[1];
  const prNumber = parseInt(prMatch[2]);
  
  // Fetch PR details from GitHub
  try {
    const ghRes = await fetch(`https://api.github.com/repos/${repo}/pulls/${prNumber}`, {
      headers: { Authorization: `token ${process.env.GH_TOKEN || process.env.GITHUB_TOKEN || ''}` }
    });
    if (!ghRes.ok) return;
    const pr = await ghRes.json();
    
    const metadata = task.metadata || {};
    metadata.rebase_requested = true;
    metadata.rebase_pr = {
      number: prNumber,
      repo: repo,
      branch: pr.head.ref,
      base: pr.base.ref,
      url: prUrl,
    };
    
    await supabase.from('agent_tasks').update({ metadata }).eq('id', task.id);
    console.log(`[REBASE] Set rebase metadata for task ${task.id} — PR #${prNumber} on ${repo} (branch: ${pr.head.ref})`);
  } catch (e) {
    console.error(`[REBASE] Failed to fetch PR info: ${e.message}`);
  }
}
