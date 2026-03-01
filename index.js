import { createClient } from "@supabase/supabase-js";
import * as k8s from "@kubernetes/client-node";

// K8s client setup
const kc = new k8s.KubeConfig();
kc.loadFromCluster();
const batchApi = kc.makeApiClient(k8s.BatchV1Api);
const coreApi = kc.makeApiClient(k8s.CoreV1Api);
const customApi = kc.makeApiClient(k8s.CustomObjectsApi);
const MAX_QA_WORKERS = 3;


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
        if (!['assigned', 'in_progress', 'qa_testing'].includes(t.status)) {
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
            status: "done",
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

  const terminal = ['done', 'completed', 'failed'];
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
      status: 'done',
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
          .in('status', ['assigned', 'in_progress'])
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
async function dispatchToAgent(task) {
  // Never dispatch to disabled agents
  if (task.assigned_agent) {
    const { data: agentCard } = await supabase
      .from('agent_cards')
      .select('status')
      .ilike('name', task.assigned_agent)
      .single();
    if (agentCard?.status === 'disabled') {
      console.log(`[SKIP] Agent ${task.assigned_agent} is disabled, skipping task ${task.id}`);
      return;
    }
  }

  // Auth preflight: verify gateway credentials before dispatching
  const agentName = task.assigned_agent?.toLowerCase();
  const authCheck = await preflightAuthCheck(agentName);
  if (!authCheck.ok) {
    console.log(`[AUTH-PREFLIGHT] Agent ${agentName} failed auth check (HTTP ${authCheck.status}), skipping task ${task.id}`);
    await supabase
      .from('agent_tasks')
      .update({
        status: 'todo',
        assigned_agent: null,
        started_at: null,
        error: `Auth preflight failed for ${agentName} (HTTP ${authCheck.status})`,
      })
      .eq('id', task.id);
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

  const message = `\`\`\`json
${taskPayload}
\`\`\`

${contextBlock}## Task Assigned: ${task.title}

${task.description || ""}
${task.prompt ? `**Prompt:** ${task.prompt}` : ""}

**Task ID:** ${task.id}
**Type:** ${task.type}
**Priority:** ${task.priority}
**Dispatched by:** ${task.dispatched_by}${task.parent_task_id ? `\n\n**Parent Task:** ${task.parent_task_id}\n**Sub-task:** This is a sub-task of a larger task. Complete your portion and update status.` : ""}

${contextBlock}---
## ⚠️ MANDATORY: Update task status when done

When you finish this task, you MUST update its status. Run this command:

**On success:**
\`\`\`bash
curl -s -X PATCH "https://lessxkxujvcmublgwdaa.supabase.co/rest/v1/agent_tasks?id=eq.${task.id}" \\
  -H "apikey: ${SUPABASE_KEY}" \\
  -H "Authorization: Bearer ${SUPABASE_KEY}" \\
  -H "Content-Type: application/json" \\
  -d '{"status":"done","completed_at":"'$(date -u +%Y-%m-%dT%H:%M:%SZ)'","result":{"summary":"DESCRIBE WHAT YOU DID","artifacts":[],"test_results":null}}'
\`\`\`

**On failure:**
\`\`\`bash
curl -s -X PATCH "https://lessxkxujvcmublgwdaa.supabase.co/rest/v1/agent_tasks?id=eq.${task.id}" \\
  -H "apikey: ${SUPABASE_KEY}" \\
  -H "Authorization: Bearer ${SUPABASE_KEY}" \\
  -H "Content-Type: application/json" \\
  -d '{"status":"failed","completed_at":"'$(date -u +%Y-%m-%dT%H:%M:%SZ)'","error":"DESCRIBE WHAT WENT WRONG"}'
\`\`\`

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
        .update({ status: "in_progress", started_at: new Date().toISOString() })
        .eq("id", task.id);
    } else {
      const err = await resp.text();
      console.error(`[ERR] ${agentName} returned ${resp.status}: ${err}`);
    }
  } catch (e) {
    console.error(`[ERR] Failed to dispatch to ${agentName}: ${e.message}`);
  }
}


// --- QA Auto-Scaler ---
// QA Stale Detection: re-dispatch QA tasks that have been sitting > 20 min with no progress
async function qaStaleDetector() {
  try {
    const { data: staleTasks, error } = await supabase
      .from("agent_tasks")
      .select("id, title, qa_agent, updated_at")
      .eq("status", "qa_testing")
      .not("qa_agent", "is", null);

    if (error || !staleTasks?.length) return;

    const QA_STALE_THRESHOLD = 20 * 60 * 1000; // 20 minutes
    for (const task of staleTasks) {
      const age = Date.now() - new Date(task.updated_at).getTime();
      if (age > QA_STALE_THRESHOLD) {
        console.log(`[QA-STALE] Task ${task.id} ("${task.title.slice(0, 40)}") stuck in qa_testing for ${Math.floor(age / 60000)}min → resetting for re-dispatch`);
        await supabase
          .from("agent_tasks")
          .update({ status: "done", qa_agent: null })
          .eq("id", task.id);
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
    const activeJobs = (jobList.items || []).filter(
      (j) => !j.status?.succeeded && !j.status?.failed
    );
    const activeWorkers = activeJobs.length;

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
  const workerName = `beta-worker-${Date.now().toString(36)}`;
  console.log(`[QA-SCALER] Spawning worker: ${workerName}`);

  const job = {
    apiVersion: "batch/v1",
    kind: "Job",
    metadata: {
      name: workerName,
      namespace: "agents",
      labels: {
        app: "beta-worker",
        role: "beta-worker",
        "managed-by": "task-dispatcher",
      },
    },
    spec: {
      backoffLimit: 0,
      ttlSecondsAfterFinished: 300,
      activeDeadlineSeconds: 1200,
      template: {
        metadata: {
          labels: {
            app: "beta-worker",
            role: "beta-worker",
            "managed-by": "task-dispatcher",
          },
        },
        spec: {
          restartPolicy: "Never",
          containers: [
            {
              name: "worker",
              image: process.env.QA_WORKER_IMAGE || "ghcr.io/dante-alpha-assistant/openclaw-agent:latest",
              env: [
                { name: "SUPABASE_URL", value: process.env.SUPABASE_URL },
                { name: "SUPABASE_SERVICE_ROLE_KEY", value: process.env.SUPABASE_SERVICE_ROLE_KEY },
                {
                  name: "OPENROUTER_API_KEY",
                  valueFrom: { secretKeyRef: { name: "beta-env", key: "OPENROUTER_API_KEY" } },
                },
                { name: "WORKER_NAME", value: workerName },
                { name: "WORKER_MODE", value: "qa" },
              ],
              resources: {
                requests: { cpu: "100m", memory: "256Mi" },
                limits: { cpu: "500m", memory: "512Mi" },
              },
            },
          ],
        },
      },
    },
  };

  try {
    await batchApi.createNamespacedJob({ namespace: "agents", body: job });
    console.log(`[QA-SCALER] Spawned worker job: ${workerName}`);
  } catch (e) {
    console.error(`[QA-SCALER] Failed to spawn worker: ${e.message}`);
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

    const runningWorkers = (podList.items || []).map((p) => p.metadata.name);
    if (!runningWorkers.length) return;

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
        .update({ qa_agent: worker })
        .eq("id", task.id)
        .is("qa_agent", null);

      if (assignErr) {
        console.error(`[QA-SCALER] Failed to assign task ${task.id} to ${worker}:`, assignErr.message);
        continue;
      }

      console.log(`[QA-SCALER] Assigned task ${task.id} ("${task.title}") to ${worker}`);

      const workerUrl = `http://${worker}.agents.svc.cluster.local:18789/hooks/agent`;
      try {
        const qaContextBlock = await buildContextBlockWithTimeout(task);
        const qaMessage = `${qaContextBlock}## QA Review: ${task.title}\n\n**Task ID:** ${task.id}\nReview this task and update status when done.`;
        await fetch(workerUrl, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
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
        // 1. New task inserted with status 'assigned' and an assigned_agent
        // 2. Task updated to 'assigned' status (e.g., dragged in kanban)
        if (task?.status === "assigned" && task?.assigned_agent) {
          // Only dispatch if status actually changed to 'assigned'
          if (eventType === "INSERT" || (eventType === "UPDATE" && prev?.status !== "assigned")) {
            console.log(`[DISPATCH] Task ${task.id} → ${task.assigned_agent}`);
            dispatchToAgent(task);
          }
        }

        // Remove completed/failed tasks from active tracking
        if (task?.status === 'done' || task?.status === 'failed' || task?.status === 'completed' || task?.status === 'deployed') {
          if (activeTasks.has(task.id)) {
            console.log(`[TRACKER] Task ${task.id} completed (${task.status}), removing from active tracking`);
            activeTasks.delete(task.id);
          }
          // A2A: check if parent task should be completed
          await checkParentCompletion(task);
        }

        // Factory pipeline stage transitions: auto-advance to next stage
        if (task?.status === "done" && eventType === "UPDATE" && prev?.status !== "done" && task.stage) {
          const nextStage = getNextStage(task.stage);
          if (nextStage) {
            // Guard: only advance forward (check current stage is valid and not final)
            const currentIdx = STAGE_PIPELINE.indexOf(task.stage);
            const nextIdx = STAGE_PIPELINE.indexOf(nextStage);
            if (nextIdx > currentIdx) {
              console.log(`[STAGE] Task ${task.id} ("${task.title}"): ${task.stage} → ${nextStage}`);
              await supabase.from("agent_tasks").update({
                stage: nextStage,
                status: "assigned",
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

        // QA routing: when task moves to "done", move it to "qa" and dispatch to Beta
        // No separate QA task — the original task goes through QA itself
        if (task?.status === "done" && eventType === "UPDATE" && prev?.status !== "done") {
          // Don't QA tasks that were already QA'd (prevent loops)
          if (task.type === "qa") return;

          console.log(`[QA] Task ${task.id} ("${task.title}") → moving to qa_testing, dispatching to beta`);

          try {

            // Dispatch the SAME task to Beta for review
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
              repo: task.repo,
              branch: task.branch,
              context: task.context,
            }, null, 2);

            const qaContextBlock = await buildContextBlockWithTimeout(task);
            const qaMessage = `\`\`\`json
${qaPayload}
\`\`\`

${qaContextBlock}## QA Review: ${task.title}

**Task ID:** ${task.id}
**Type:** ${task.type}
**Originally assigned to:** ${task.assigned_agent}
**Dispatched by:** ${task.dispatched_by}

### Description
${task.description || "(none)"}

### Result
${task.result ? JSON.stringify(task.result, null, 2) : "(no result reported)"}

### Acceptance Criteria
${task.acceptance_criteria || "Verify the task was completed correctly based on the description and result."}

### Your Job
1. Review the result against the acceptance criteria
2. If the task involved code: check the repo, run tests if possible
3. If the task involved a URL/service: test it
4. Update THIS task status directly:

**If QA passes:**
\`\`\`bash
curl -s -X PATCH "https://lessxkxujvcmublgwdaa.supabase.co/rest/v1/agent_tasks?id=eq.${task.id}" \\
  -H "apikey: ${SUPABASE_KEY}" \\
  -H "Authorization: Bearer ${SUPABASE_KEY}" \\
  -H "Content-Type: application/json" \\
  -d '{"status":"completed","completed_at":"'$(date -u +%Y-%m-%dT%H:%M:%SZ)'","qa_result":{"passed":true,"notes":"WHAT YOU VERIFIED"}}'
\`\`\`

**If QA fails:**
\`\`\`bash
curl -s -X PATCH "https://lessxkxujvcmublgwdaa.supabase.co/rest/v1/agent_tasks?id=eq.${task.id}" \\
  -H "apikey: ${SUPABASE_KEY}" \\
  -H "Authorization: Bearer ${SUPABASE_KEY}" \\
  -H "Content-Type: application/json" \\
  -d '{"status":"failed","completed_at":"'$(date -u +%Y-%m-%dT%H:%M:%SZ)'","qa_result":{"passed":false,"failures":["SPECIFIC ISSUE"]}}'
\`\`\``;

            const betaAgent = AGENTS.beta;
            if (betaAgent?.token) {
              const resp = await fetch(betaAgent.url, {
                method: "POST",
                headers: {
                  Authorization: `Bearer ${betaAgent.token}`,
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
                console.log(`[QA] Dispatched task ${task.id} to beta for QA review`);
                // Move to qa_testing in one step (no intermediate 'qa' status)
                await supabase
                  .from("agent_tasks")
                  .update({ status: "qa_testing", qa_agent: "beta" })
                  .eq("id", task.id);

                // Trigger auto-scaler if queue is building up
                const { data: qaQueueCheck } = await supabase
                  .from("agent_tasks")
                  .select("id")
                  .eq("status", "qa_testing")
                  .is("qa_agent", null);
                if (qaQueueCheck && qaQueueCheck.length > 1) {
                  qaAutoScaler();
                }
              } else {
                console.error(`[QA] Failed to dispatch to beta: ${resp.status}`);
              }
            }
          } catch (e) {
            console.error(`[QA] Error routing task ${task.id} to QA: ${e.message}`);
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
        supabase.from("agent_tasks").select("assigned_agent").in("status", ["assigned", "in_progress"]),
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
      .in("status", ["in_progress", "assigned", "qa_testing"]);

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
        ? (task.qa_agent?.toLowerCase() || "beta")
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
        // Idle timeout: session exists but hasn't been updated in 5 minutes
        // This catches orphaned hook sessions where the LLM never started working
        const IDLE_TIMEOUT = 5 * 60 * 1000; // 5 minutes
        const hookSession = sessions?.find(s => s.key === (isQaTesting ? `agent:main:hook:qa:${task.id}` : `agent:main:hook:task:${task.id}`));
        const idleMs = hookSession ? Date.now() - (hookSession.updatedAt || 0) : 0;
        if (idleMs > IDLE_TIMEOUT) {
          console.log(`[MONITOR] Idle timeout: task ${task.id} ("${task.title.slice(0,40)}") → ${agentName} idle ${Math.floor(idleMs/60000)}min → resetting to todo`);
          await supabase.from("agent_tasks").update({
            status: "todo",
            assigned_agent: null,
            started_at: null,
            error: `Idle timeout: hook session idle ${Math.floor(idleMs/60000)}min — agent was occupied elsewhere`,
          }).eq("id", task.id);
          activeTasks.delete(task.id);
          continue;
        }

        // Session is active — only hard-timeout if REALLY old (safety net)
        const startTime = task.started_at || task.created_at;
        const timeout = task.type === "qa" ? QA_HARD_TIMEOUT : task.type === "coding" ? CODING_HARD_TIMEOUT : TASK_HARD_TIMEOUT;
        const elapsed = startTime ? Date.now() - new Date(startTime).getTime() : 0;
        // Even with active session, kill after 2x the timeout (absolute safety)
        if (elapsed > timeout * 2) {
          console.log(`[MONITOR] Absolute timeout: task ${task.id} ("${task.title}") → ${agentName} (${Math.round(elapsed / 60000)}min, session still alive but too old)`);
          await supabase.from("agent_tasks").update({
            status: "failed",
            error: `Absolute timeout: task ran for ${Math.round(elapsed / 60000)} minutes`,
            completed_at: new Date().toISOString(),
          }).eq("id", task.id);
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
            // QA session crashed — reset to done + clear qa_agent so qaAutoScaler re-dispatches
            console.log(`[MONITOR] QA session gone for task ${task.id} ("${task.title}") → resetting to done for re-dispatch`);
            await supabase.from("agent_tasks").update({
              status: "done",
              qa_agent: null,
            }).eq("id", task.id);
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
    const cards = await getAgentCards();
    if (!cards.length) {
      console.log("[SCHEDULER] No online agents found — skipping cycle");
      return;
    }

    const { data: activeTasks_db, error: activeErr } = await supabase
      .from("agent_tasks")
      .select("assigned_agent")
      .in("status", ["assigned", "in_progress"]);

    if (activeErr) {
      console.error("[SCHEDULER] Error fetching active tasks:", activeErr.message);
      return;
    }

    // Build load map from active tasks
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

    if (available.length === 0) return;

    // Belt-and-suspenders: fetch disabled agents and exclude them
    const { data: disabledAgents } = await supabase
      .from('agent_cards')
      .select('name')
      .eq('status', 'disabled');
    const disabledNames = new Set((disabledAgents || []).map(a => a.name.toLowerCase()));
    const availableFiltered = available.filter(a => !disabledNames.has(a.name));
    if (availableFiltered.length === 0) return;

    // Gateway concurrency check: remove agents that are currently busy
    const busyChecks = await Promise.all(
      availableFiltered.map(async a => ({ name: a.name, busy: await checkAgentBusy(a.name) }))
    );
    const busyAgents = new Set(busyChecks.filter(c => c.busy).map(c => c.name));
    const freeAgents = availableFiltered.filter(a => !busyAgents.has(a.name));
    
    if (busyAgents.size > 0) {
      console.log(`[SCHEDULER] Busy agents skipped: ${[...busyAgents].join(', ')}`);
    }
    if (freeAgents.length === 0) {
      if (busyAgents.size > 0) console.log(`[SCHEDULER] All agents busy, waiting for next cycle`);
      return;
    }

    const { data: todoTasks, error: todoErr } = await supabase
      .from("agent_tasks")
      .select("*")
      .eq("status", "todo")
      .order("created_at", { ascending: true });

    if (todoErr) {
      console.error("[SCHEDULER] Error fetching todo tasks:", todoErr.message);
      return;
    }

    if (!todoTasks?.length) return;

    todoTasks.sort((a, b) => (PRIORITY_ORDER[a.priority] ?? 2) - (PRIORITY_ORDER[b.priority] ?? 2));

    let assigned = 0;

    for (const task of todoTasks) {
      // Respect assigned_agent hint
      if (task.assigned_agent) {
        const hintAgent = task.assigned_agent.toLowerCase();
        const agentSlot = freeAgents.find(a => a.name === hintAgent && a.remaining > 0);
        if (agentSlot) {
          console.log(`[SCHEDULER] Assigning task ${task.id} ("${task.title}") \u2192 ${hintAgent} (hint)`);
          await supabase
            .from("agent_tasks")
            .update({ status: "assigned", assigned_agent: hintAgent })
            .eq("id", task.id);
          agentSlot.remaining--;
          assigned++;
          continue;
        }
        continue;
      }

      const taskType = task.type || "general";

      // Score agents: must have capability, then rank by capacity + priority affinity
      const candidates = freeAgents
        .filter(a => a.remaining > 0 && a.capabilities.includes(taskType))
        .map(a => {
          let score = a.remaining;
          const affinityMultiplier = a.priority_affinity[task.priority];
          if (affinityMultiplier) score *= affinityMultiplier;
          return { ...a, score };
        })
        .sort((a, b) => b.score - a.score);

      const bestCandidate = candidates[0];

      if (bestCandidate) {
        // Decrement the ORIGINAL agent in freeAgents (not the spread copy)
        const originalAgent = freeAgents.find(a => a.name === bestCandidate.name);
        if (!originalAgent || originalAgent.remaining <= 0) continue;
        
        // Double-check: re-query DB for in-flight tasks this agent has RIGHT NOW
        const { data: inflightCheck } = await supabase
          .from("agent_tasks")
          .select("id")
          .eq("assigned_agent", bestCandidate.name)
          .in("status", ["assigned", "in_progress"])
          .limit(5);
        const inflightCount = inflightCheck?.length || 0;
        if (inflightCount >= originalAgent.max_concurrent) {
          console.log(`[SCHEDULER] ${bestCandidate.name} already has ${inflightCount} in-flight tasks (max ${originalAgent.max_concurrent}), skipping`);
          originalAgent.remaining = 0;
          continue;
        }

        console.log(`[SCHEDULER] Auto-assigning task ${task.id} ("${task.title}") \u2192 ${bestCandidate.name} (type: ${taskType}, remaining: ${originalAgent.remaining})`);
        await supabase
          .from("agent_tasks")
          .update({ status: "assigned", assigned_agent: bestCandidate.name })
          .eq("id", task.id);
        originalAgent.remaining--;
        assigned++;
      } else {
        console.log(`[SCHEDULER] No capable agent for task ${task.id} (type: ${taskType}), keeping in queue`);
      }
    }

    if (assigned > 0) {
      console.log(`[SCHEDULER] Assigned ${assigned} tasks this cycle`);
    }
  } catch (e) {
    console.error("[SCHEDULER] Error:", e.message);
  }
}

// Stale agent detection — mark agents offline if last_heartbeat > 10min ago
const STALE_AGENT_INTERVAL = 60_000;
async function staleAgentDetector() {
  try {
    const tenMinAgo = new Date(Date.now() - 10 * 60 * 1000).toISOString();
    const { data, error } = await supabase
      .from('agent_cards')
      .select('id, name, status, last_heartbeat')
      .eq('status', 'online')
      .lt('last_heartbeat', tenMinAgo);
    if (error) {
      console.error('[STALE] Failed to check stale agents:', error.message);
      return;
    }
    for (const agent of (data || [])) {
      console.log(`[STALE] Marking ${agent.name} as offline (last_heartbeat: ${agent.last_heartbeat})`);
      await supabase
        .from('agent_cards')
        .update({ status: 'offline' })
        .eq('id', agent.id)
        .neq('status', 'disabled');
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

async function getArgoAppsViaHTTP() {
  if (!ARGOCD_USERNAME || !ARGOCD_PASSWORD) return [];
  try {
    const sessionResp = await fetch(`${ARGOCD_URL}/api/v1/session`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username: ARGOCD_USERNAME, password: ARGOCD_PASSWORD }),
    });
    if (!sessionResp.ok) throw new Error(`Session: ${sessionResp.status}`);
    const { token } = await sessionResp.json();
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
  return syncStatus === "Synced" && healthStatus === "Healthy";
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
    if (!apps.length) {
      console.log("[DEPLOY-DETECT] No ArgoCD apps found");
      return;
    }

    // Check dev app specifically (primary deploy target)
    const devApp = apps.find(a => a.metadata?.name === "dev");
    const prodApp = apps.find(a => a.metadata?.name === "prod");

    const devSynced = devApp && isAppSyncedHealthy(devApp);
    const prodSynced = prodApp && isAppSyncedHealthy(prodApp);

    if (!devSynced && !prodSynced) {
      // Neither environment is synced+healthy, skip
      return;
    }

    const devSyncTime = devApp ? getAppSyncFinishedAt(devApp) : null;
    const prodSyncTime = prodApp ? getAppSyncFinishedAt(prodApp) : null;

    // 2. Query completed tasks (QA passed) that haven't been marked as deployed yet
    const { data: completedTasks, error } = await supabase
      .from("agent_tasks")
      .select("id, title, status, completed_at, result, type")
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

      // For coding tasks: check if ArgoCD synced after task completion
      // The sync must have happened AFTER the task was completed (meaning the new image is live)
      let isDeployed = false;
      let deployEnv = null;

      if (devSynced && devSyncTime && devSyncTime > completedAt) {
        isDeployed = true;
        deployEnv = "dev";
      }
      if (prodSynced && prodSyncTime && prodSyncTime > completedAt) {
        isDeployed = true;
        deployEnv = deployEnv ? "dev+prod" : "prod";
      }

      // Also check: extract commit SHA or PR from result for logging
      let commitRef = null;
      if (task.result) {
        const resultStr = typeof task.result === "string" ? task.result : JSON.stringify(task.result);
        // Match PR numbers like "PR #13" or "PR#13" or "#13 merged"
        const prMatch = resultStr.match(/PR\s*#(\d+)/i) || resultStr.match(/#(\d+)\s*merged/i);
        if (prMatch) commitRef = `PR #${prMatch[1]}`;
        // Match commit SHAs
        const shaMatch = resultStr.match(/\b([0-9a-f]{7,40})\b/);
        if (!commitRef && shaMatch) commitRef = shaMatch[1].slice(0, 7);
      }

      if (isDeployed) {
        console.log(`[DEPLOY-DETECT] Task ${task.id} ("${task.title.slice(0, 40)}") → deployed (${deployEnv}, sync after completion${commitRef ? `, ref: ${commitRef}` : ""})`);
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
scheduler();
console.log(`[BOOT] Auto-scheduler running every ${SCHEDULER_INTERVAL / 1000}s`);

// Unified task monitor (replaces watchdog + session checker)
setInterval(taskMonitor, MONITOR_INTERVAL);
setTimeout(taskMonitor, 5000); // Run 5s after boot (let realtime connect first)
console.log(`[BOOT] Task monitor running every ${MONITOR_INTERVAL / 1000}s (hard timeout: ${TASK_HARD_TIMEOUT / 60000}min, grace: ${SESSION_GONE_GRACE / 1000}s)`);

// QA Auto-Scaler
setInterval(qaAutoScaler, 30000);
console.log("[BOOT] QA auto-scaler running every 30s");

// Stale agent detector
setInterval(staleAgentDetector, STALE_AGENT_INTERVAL);
setTimeout(staleAgentDetector, 10000);
console.log("[BOOT] Stale agent detector running every 60s (threshold: 10min)");

// Auto-deploy detector
setInterval(autoDeployDetector, DEPLOY_DETECT_INTERVAL);
setTimeout(autoDeployDetector, 15000); // Run 15s after boot
console.log(`[BOOT] Auto-deploy detector running every ${DEPLOY_DETECT_INTERVAL / 1000}s`);

// Graceful shutdown
process.on("SIGTERM", () => {
  console.log("[SHUTDOWN] Received SIGTERM");
  process.exit(0);
});
