import { createClient } from "@supabase/supabase-js";
import * as k8s from "@kubernetes/client-node";
import { execSync } from "child_process";

// K8s client setup
const kc = new k8s.KubeConfig();
kc.loadFromCluster();
const batchApi = kc.makeApiClient(k8s.BatchV1Api);
const coreApi = kc.makeApiClient(k8s.CoreV1Api);
const customApi = kc.makeApiClient(k8s.CustomObjectsApi);
const MAX_QA_WORKERS = 2;


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

// Only fetch online agents — disabled agents are never auto-assigned or dispatched to
async function getAgentCards() {
  const { data, error } = await supabase
    .from('agent_cards')
    .select('name, capabilities, task_types, max_capacity, priority_affinity, status')
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
  }));
}

const DANTE_ID_API_URL = process.env.DANTE_ID_API_URL || "https://api.dante.id";
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
    await supabase.from('task_activity_log').insert({
      task_id: taskId,
      field,
      old_value: oldValue,
      new_value: newValue,
      changed_by: changedBy,
      changed_at: new Date().toISOString(),
    });
  } catch (e) {
    console.error('[ACTIVITY-LOG] Failed to log activity for task', taskId, ':', e.message);
  }
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
        error: 'Dispatch lost during restart — re-queued automatically',
        updated_at: new Date().toISOString(),
      }).eq('id', task.id);
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
          await supabase.from("agent_tasks").update({
            status: "qa_testing",
            result: { output: "Factory pipeline completed", deployment_url: deploymentUrl, project_id: projectId, stage_results: stageResults },
            completed_at: new Date().toISOString(),
          }).eq("id", task.id);
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
          .update({ assigned_agent: null, started_at: null, error: skipReason, last_failed_agent: task.assigned_agent })
          .eq('id', task.id);
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
    // If task was in_progress with a result, move to qa_testing (work was done)
    // If in_progress without result, move back to todo for re-dispatch
    const statusFix = task.status === 'in_progress'
      ? (task.result ? { status: 'qa_testing', completed_at: new Date().toISOString() }
         : { status: 'todo' })
      : {};
    await supabase
      .from('agent_tasks')
      .update({
        assigned_agent: null,
        started_at: null,
        error: authReason,
        last_failed_agent: agentName,
        ...statusFix,
      })
      .eq('id', task.id);
    if (statusFix.status) console.log(`[AUTH-PREFLIGHT] Task ${task.id} was ${task.status} → ${statusFix.status} (had result: ${!!task.result})`);
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
    repo: task.repo,
    branch: task.branch,
    context: task.context,
  }, null, 2);

  const contextBlock = await buildContextBlockWithTimeout(task);
  const commentsBlock = await fetchTaskComments(task.id);

  // Build coding task section if applicable
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

${task.description || ""}
${task.prompt ? `**Prompt:** ${task.prompt}` : ""}
${codingTaskSection}

**Task ID:** ${task.id}
**Type:** ${task.type}
**Priority:** ${task.priority}
**Dispatched by:** ${task.dispatched_by}${task.parent_task_id ? `\n\n**Parent Task:** ${task.parent_task_id}\n**Sub-task:** This is a sub-task of a larger task. Complete your portion and update status.` : ""}
${commentsBlock ? `\n${commentsBlock}` : ""}
${contextBlock}---
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
    -d '{"status":"blocked","blocked_reason":"EXPLAIN what you cannot do and why"}'

BLOCKED DETECTION RULES:
- NEVER mark done if manual steps remain (SQL migrations, external config, DNS changes)
- NEVER write "apply this manually" — if you cannot do it, set status to blocked
- NEVER ship incomplete work as complete
- Use curl, kubectl, gh CLI FIRST before deciding to block
- Only block if you genuinely CANNOT do it after trying.

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
      console.log(`[OK] Dispatched task ${task.id} ("${task.title}") → ${agentName}`);

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
    } else {
      const err = await resp.text();
      const errMsg = `Dispatch to ${agentName} failed: HTTP ${resp.status} — ${err.slice(0, 200)}`;
      console.error(`[ERR] ${errMsg}`);
      // Write error to task so it shows in Activity log
      await supabase.from("agent_tasks").update({ error: errMsg, assigned_agent: null }).eq("id", task.id);
    }
  } catch (e) {
    const errMsg = `Dispatch to ${agentName} failed: ${e.message}`;
    console.error(`[ERR] ${errMsg}`);
    await supabase.from("agent_tasks").update({ error: errMsg, assigned_agent: null }).eq("id", task.id);
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

    for (let i = 0; i < Math.min(unassigned.length, freeWorkers.length); i++) {
      const task = unassigned[i];
      const worker = freeWorkers[i];

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

### DO NOT:
- Clone the repo and try to build it
- Run tests locally
- Spend more than 5 minutes total

### When done:
- If acceptable: **MERGE the PR first** (squash merge via GitHub API), then update task status to \`completed\`
${prMatch && repoMatch ? `  Merge command: \`gh pr merge ${prMatch[1]} -R ${repoMatch[1]} --squash --delete-branch\`` : prMatch ? `  Merge command: \`gh pr merge ${prMatch[1]} --squash --delete-branch\`` : '  Find the PR number from the task result and merge it'}
- If issues found: do NOT merge. Update task status to \`failed\` with specific issues listed`;
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
        // Add Gherkin acceptance criteria instructions
        qaInstructions += `

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
          if (inFlight && inFlight.length > 0) {
            const reason = `Agent ${agentName} busy (${inFlight.length} in-progress task), unassigning for re-dispatch`;
            console.log(`[DISPATCH] ${reason}`);
            await supabase.from('agent_tasks').update({
              assigned_agent: null,
              last_failed_agent: agentName,
              error: reason,
            }).eq('id', task.id);
          } else {
            console.log(`[DISPATCH] Task ${task.id} → ${task.assigned_agent}`);
            dispatchToAgent(task);
          }
        }

        // When task transitions to qa_testing: clear assigned_agent
        // The coding agent is done — assigned_agent should reflect current owner (nobody until scheduler assigns QA agent)
        if (task && task.status === 'qa_testing' && eventType === 'UPDATE' && prev?.status && prev.status !== task.status) {
          if (task.assigned_agent) {
            const unassignReason = `Unassigned from ${task.assigned_agent}: task moved to qa_testing`;
            console.log(`[UNASSIGN] Task ${task.id} → qa_testing, clearing assigned_agent (was: ${task.assigned_agent}), resetting started_at for QA timeout`);
            await supabase
              .from('agent_tasks')
              .update({ assigned_agent: null, started_at: null, error: unassignReason })
              .eq('id', task.id);
          }
        }

        // Remove completed/failed tasks from active tracking
        if (task?.status === 'qa_testing' || task?.status === 'failed' || task?.status === 'completed' || task?.status === 'deployed') {
          if (activeTasks.has(task.id)) {
            console.log(`[TRACKER] Task ${task.id} completed (${task.status}), removing from active tracking`);
            activeTasks.delete(task.id);
          }
          // A2A: check if parent task should be completed
          await checkParentCompletion(task);
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
            const agent = task.assigned_agent || prev?.assigned_agent || 'system';
            const result = typeof task.result === 'object' ? task.result : (() => { try { return JSON.parse(task.result); } catch { return null; } })();
            let commentBody = '📋 **Task Result**\n\n';
            if (result?.summary) {
              commentBody += result.summary;
            } else {
              commentBody += typeof task.result === 'string' ? task.result : JSON.stringify(task.result, null, 2);
            }
            if (result?.artifacts?.length) {
              commentBody += '\n\n📎 **Artifacts:** ' + result.artifacts.join(', ');
            }
            if (result?.test_results) {
              commentBody += '\n\n🧪 **Test Results:** ' + (typeof result.test_results === 'string' ? result.test_results : JSON.stringify(result.test_results));
            }
            if (task.pull_request_url?.length) {
              commentBody += '\n\n🔗 **PR:** ' + task.pull_request_url.join(', ');
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

            let qaScope = '';
            let timeLimit = '3 minutes';
            if (taskType === 'ops' || taskType === 'review') {
              qaScope = `### Lightweight QA (ops/config task — complete in under 1 minute)
1. Check the result — was the change applied successfully?
2. Any errors in the output?
3. Does the result match the description?
DO NOT: SSH into servers, run commands, or deep-dive into infrastructure.`;
              timeLimit = '1 minute';
            } else if (taskType === 'coding') {
              qaScope = `### Code Review QA (complete in under 3 minutes)
1. ${prMatch ? `Check PR #${prMatch[1]}${repoMatch ? ` on ${repoMatch[1]}` : ''} — read the diff` : 'Check the task result for a PR reference'}
2. Scan for: obvious bugs, missing error handling, broken imports, hardcoded secrets
3. Does the code match what was requested in the description?
4. Check for regressions — does the change break existing patterns?
DO NOT: Clone the repo, run builds, run tests, or spend more than 3 minutes.`;
              timeLimit = '3 minutes';
            } else {
              qaScope = `### Quick QA (complete in under 2 minutes)
1. Does the result match the task description?
2. Any obvious errors?
DO NOT spend more than 2 minutes on this review.`;
              timeLimit = '2 minutes';
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
            }, null, 2);

            const qaMessage = `\`\`\`json
${qaPayload}
\`\`\`

${qaContextBlock}## QA Review: ${task.title}

**Task ID:** ${task.id}
**Type:** ${taskType} | **Time limit:** ${timeLimit}

### Description
${task.description || "(none)"}

### Result
${task.result ? JSON.stringify(task.result, null, 2).slice(0, 1000) : "(no result reported)"}

${qaScope}
${qaCommentsBlock ? qaCommentsBlock : ""}

### Update task status when done:

**If QA passes (coding tasks — MERGE FIRST):**
\`\`\`bash
gh pr merge <PR_NUMBER> -R <REPO> --squash --delete-branch
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
      .select("id, title, status, assigned_agent, qa_agent, started_at, created_at")
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
      if (freshTask && ["done", "failed", "completed"].includes(freshTask.status)) {
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
        const IDLE_TIMEOUT = isQaTesting ? 15 * 60 * 1000 : 5 * 60 * 1000; // 15min QA, 5min coding
        const hookSession = sessions?.find(s => s.key === (isQaTesting ? `agent:main:hook:qa:${task.id}` : `agent:main:hook:task:${task.id}`));
        const idleMs = hookSession ? Date.now() - (hookSession.updatedAt || 0) : 0;
        if (idleMs > IDLE_TIMEOUT) {
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
          } else if (hasCompletedWork && isQaTesting) {
            // QA idle but work is done — stay in qa_testing, just clear QA agent for re-dispatch
            // Don't reset to todo — that loses the completed coding work
            console.log(`[MONITOR] QA idle timeout: task ${task.id} ("${task.title.slice(0,40)}") → QA agent idle ${Math.floor(idleMs/60000)}min but work is done — clearing QA agent for re-dispatch (retry ${idleRetries}/${MAX_IDLE_RETRIES})`);
            await supabase.from("agent_tasks").update({
              qa_agent: null,
              idle_retries: idleRetries,
              error: `QA idle timeout: ${agentName} session idle >${Math.floor(idleMs/60000)}min — re-queued QA (retry ${idleRetries}/${MAX_IDLE_RETRIES})`,
            }).eq("id", task.id);
            await logTaskActivity(task.id, 'dispatch_error', null, `QA agent ${agentName} idle for ${Math.floor(idleMs/60000)}min — re-queued QA (retry ${idleRetries}/${MAX_IDLE_RETRIES})`, 'dispatcher');
          } else {
            console.log(`[MONITOR] Idle timeout: task ${task.id} ("${task.title.slice(0,40)}") → ${agentName} idle ${Math.floor(idleMs/60000)}min (retry ${idleRetries}/${MAX_IDLE_RETRIES}) → resetting to todo`);
            recordTransition(task.id);
            await supabase.from("agent_tasks").update({
              status: "todo",
              assigned_agent: null,
              started_at: null,
              idle_retries: idleRetries,
              error: `Idle timeout: ${agentName} session idle >${Math.floor(idleMs/60000)}min — re-queued (retry ${idleRetries}/${MAX_IDLE_RETRIES})`,
            }).eq("id", task.id);
            await logTaskActivity(task.id, 'dispatch_error', null, `Agent ${agentName} idle for ${Math.floor(idleMs/60000)}min — re-queued (retry ${idleRetries}/${MAX_IDLE_RETRIES})`, 'dispatcher');
          }
          activeTasks.delete(task.id);
          continue;
        }

        // Session is active — only hard-timeout if REALLY old (safety net)
        const startTime = task.started_at || task.created_at;
        // Use status-based timeout: QA tasks get QA timeout even if task.type is 'coding'
        const isInQa = task.status === 'qa_testing';
        const timeout = isInQa ? QA_HARD_TIMEOUT : (task.type === 'qa' ? QA_HARD_TIMEOUT : task.type === 'coding' ? CODING_HARD_TIMEOUT : TASK_HARD_TIMEOUT);
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
          // Check if past hard timeout → fail, otherwise → done
          const startTime = task.started_at || task.created_at;
          const timeout = task.type === "qa" ? QA_HARD_TIMEOUT : task.type === "coding" ? CODING_HARD_TIMEOUT : TASK_HARD_TIMEOUT;
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
                error: qaGoneReason,
              }).eq("id", task.id);
              await logTaskActivity(task.id, 'qa_error', null, qaGoneReason, 'dispatcher');
            }
            recordTransition(task.id);
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
      const hasWork = !!(t.result || (t.pull_request_url && t.pull_request_url.length > 0));
      if (hasWork) {
        console.log("[SCHEDULER] Task " + t.id + " has completed work but is todo — routing to qa_testing");
        await supabase.from("agent_tasks").update({ status: "qa_testing", assigned_agent: null }).eq("id", t.id);
        recordTransition(t.id);
        continue;
      }
      if (!canTransition(t.id)) {
        console.log("[SCHEDULER] Cooldown active for task " + t.id + " — skipping this cycle");
        continue;
      }
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
            await supabase.from("agent_tasks").update({ assigned_agent: null, error: `Hint agent ${hintAgent} is disabled/degraded — cleared for re-routing` }).eq("id", task.id);
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

      // Score agents: must have required capability, then rank by capacity + priority affinity
      const candidates = freeAgents
        .filter(a => a.remaining > 0 && a.capabilities.includes(requiredCapability) && a.name !== task.last_failed_agent)
        .map(a => {
          let score = a.remaining;
          const affinityMultiplier = a.priority_affinity[task.priority];
          if (affinityMultiplier) score *= affinityMultiplier;
          return { ...a, score };
        })
        .sort((a, b) => b.score - a.score);

      const bestCandidate = candidates[0];
      if (!bestCandidate && !isQaTask) {
        console.log(`[SCHEDULER] No candidate for task ${task.id} ("${task.title.substring(0,30)}") type=${task.type} required=${requiredCapability} last_failed=${task.last_failed_agent} agents=${freeAgents.map(a=>a.name+":"+a.capabilities.join(",")).join("|")}`);
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
      .update({ status: 'todo', assigned_agent: null, error: `Agent ${agentName} degraded (stale heartbeat) — re-queued for re-dispatch` })
      .eq('id', task.id);
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
      .select("id, title, status, completed_at, result, type, repository_id")
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
      // Strategy: If the ArgoCD sync finished AFTER the task was completed,
      // the new image (built from the task's merged PR) is live.
      // This works because: PR merge → CI builds image → GHCR push →
      // Image Updater detects → ArgoCD syncs → app is Synced+Healthy
      const isCodingTask = task.type === "coding";
      
      // For coding tasks, check if ArgoCD synced after task completion
      // For non-coding tasks (ops, research, etc.), they don't need deploy detection —
      // mark them as deployed immediately since there's nothing to deploy
      if (!isCodingTask) {
        console.log(`[DEPLOY-DETECT] Task ${task.id} ("${task.title.slice(0, 40)}") is non-coding (${task.type}) → deployed`);
        await supabase.from("agent_tasks").update({
          status: "deployed",
        }).eq("id", task.id);
        deployed++;
        continue;
      }

      // Extract PR number or commit from task result
      let commitRef = null;
      let prNumber = null;
      let repoFullName = null;
      if (task.result) {
        const resultStr = typeof task.result === "string" ? task.result : JSON.stringify(task.result);
        // Match PR numbers like "PR #13" or "PR#13" or "#13 merged"
        const prMatch = resultStr.match(/PR\s*#(\d+)/i) || resultStr.match(/#(\d+)\s*merged/i);
        if (prMatch) {
          prNumber = parseInt(prMatch[1]);
          commitRef = `PR #${prNumber}`;
        }
        // Match commit SHAs
        const shaMatch = resultStr.match(/\b([0-9a-f]{7,40})\b/);
        if (!commitRef && shaMatch) commitRef = shaMatch[1].slice(0, 7);
        // Extract repo name from result (e.g. "dante-alpha-assistant/queue-dashboard")
        const repoMatch = resultStr.match(/(dante-alpha-assistant\/[\w-]+)/);
        if (repoMatch) repoFullName = repoMatch[1];
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

// DISABLED: deploy-detect produces false positives (marks tasks deployed before image is live)
// Deployment will be manual via Deploy button on dashboard. Re-enable when button is ready.
// setInterval(autoDeployDetector, DEPLOY_DETECT_INTERVAL);
// setTimeout(autoDeployDetector, 15000);
console.log(`[BOOT] Auto-deploy detector DISABLED — manual deployment only`);

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
setInterval(codingAutoScaler, CODING_SCALER_INTERVAL);
console.log(`[BOOT] Coding auto-scaler running every ${CODING_SCALER_INTERVAL / 1000}s (max ${MAX_CODING_WORKERS} ephemeral workers)`);

