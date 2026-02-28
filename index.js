import { createClient } from "@supabase/supabase-js";
import * as k8s from "@kubernetes/client-node";

// K8s client setup
const kc = new k8s.KubeConfig();
kc.loadFromCluster();
const batchApi = kc.makeApiClient(k8s.BatchV1Api);
const coreApi = kc.makeApiClient(k8s.CoreV1Api);
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
const SCHEDULER_INTERVAL = 30_000; // 30 seconds
const FACTORY_POLL_INTERVAL = 15_000; // 15 seconds
const FACTORY_MAX_WAIT = 10 * 60 * 1000; // 10 minutes

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
        description: task.description || null,
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
          lastStage = newStage;
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
            result: { output: "Factory pipeline completed", deployment_url: deploymentUrl, project_id: projectId },
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

  // Route coding tasks through the Dante ID factory pipeline
  if (task.type === "coding") {
    const handled = await dispatchViaFactory(task);
    if (handled) return;
    console.log(`[DISPATCH] Factory fallback: dispatching coding task ${task.id} to agent`);
  }

  const agentName = task.assigned_agent?.toLowerCase();
  const agent = AGENTS[agentName];

  if (!agent) {
    console.error(`[SKIP] Unknown agent: ${agentName} for task ${task.id}`);
    return;
  }

  if (!agent.token) {
    console.error(`[SKIP] No token for agent: ${agentName}`);
    return;
  }

  const message = `## Task Assigned: ${task.title}

${task.description || ""}
${task.prompt ? `**Prompt:** ${task.prompt}` : ""}

**Task ID:** ${task.id}
**Type:** ${task.type}
**Priority:** ${task.priority}
**Dispatched by:** ${task.dispatched_by}${task.parent_task_id ? `\n\n**Parent Task:** ${task.parent_task_id}\n**Sub-task:** This is a sub-task of a larger task. Complete your portion and update status.` : ""}

---
## ⚠️ MANDATORY: Update task status when done

When you finish this task, you MUST update its status. Run this command:

**On success:**
\`\`\`bash
curl -s -X PATCH "https://lessxkxujvcmublgwdaa.supabase.co/rest/v1/agent_tasks?id=eq.${task.id}" \\
  -H "apikey: ${SUPABASE_KEY}" \\
  -H "Authorization: Bearer ${SUPABASE_KEY}" \\
  -H "Content-Type: application/json" \\
  -d '{"status":"done","completed_at":"'$(date -u +%Y-%m-%dT%H:%M:%SZ)'","result":{"output":"DESCRIBE WHAT YOU DID"}}'
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
async function qaAutoScaler() {
  try {
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
        const qaMessage = `## QA Review: ${task.title}\n\n**Task ID:** ${task.id}\nReview this task and update status when done.`;
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
        if (task?.status === 'done' || task?.status === 'failed' || task?.status === 'completed') {
          if (activeTasks.has(task.id)) {
            console.log(`[TRACKER] Task ${task.id} completed (${task.status}), removing from active tracking`);
            activeTasks.delete(task.id);
          }
          // A2A: check if parent task should be completed
          await checkParentCompletion(task);
        }

        // QA routing: when task moves to "done", move it to "qa" and dispatch to Beta
        // No separate QA task — the original task goes through QA itself
        if (task?.status === "done" && eventType === "UPDATE" && prev?.status !== "done") {
          // Don't QA tasks that were already QA'd (prevent loops)
          if (task.type === "qa") return;

          console.log(`[QA] Task ${task.id} ("${task.title}") → moving to qa_testing, dispatching to beta`);

          try {

            // Dispatch the SAME task to Beta for review
            const qaMessage = `## QA Review: ${task.title}

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
      .select("id, title, status, assigned_agent, started_at, created_at")
      .in("status", ["in_progress", "assigned"]);

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

      const agentName = task.assigned_agent?.toLowerCase();
      const agent = AGENTS[agentName];

      if (!agent) continue;

      // Use gateway token for session polling (NOT hooks token)
      const authToken = agent.gatewayToken || agent.token;
      const invokeUrl = agent.url.replace('/hooks/agent', '/tools/invoke');

      // Only check sessions for in_progress tasks (assigned tasks haven't started yet)
      if (task.status !== "in_progress") continue;

      // Check if session still exists FIRST (before any timeout logic)
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
          const sessions = data?.result?.details?.sessions || [];
          const sessionKey = `agent:main:hook:task:${task.id}`;
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

          if (elapsed > timeout) {
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
    if (!cards.length) return;

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
        available.push({ name: card.name, remaining, capabilities: card.capabilities, priority_affinity: card.priority_affinity || {} });
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
        const agentSlot = availableFiltered.find(a => a.name === hintAgent && a.remaining > 0);
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
      const candidates = availableFiltered
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
        // Decrement the ORIGINAL agent in availableFiltered (not the spread copy)
        const originalAgent = availableFiltered.find(a => a.name === bestCandidate.name);
        if (!originalAgent || originalAgent.remaining <= 0) continue;
        
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

// Graceful shutdown
process.on("SIGTERM", () => {
  console.log("[SHUTDOWN] Received SIGTERM");
  process.exit(0);
});
