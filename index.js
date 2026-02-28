import { createClient } from "@supabase/supabase-js";

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

// Agent capacity and type affinity
const AGENT_CONFIG = {
  neo:  { maxConcurrent: 2, types: ["coding", "ops", "general", "research"] },
  mu:   { maxConcurrent: 2, types: ["coding", "ops", "general"] },
  beta: { maxConcurrent: 1, types: ["qa"] },  // QA only
  flow: { maxConcurrent: 2, types: ["general", "research", "ops"] },
};

const PRIORITY_ORDER = { urgent: 0, high: 1, normal: 2, low: 3 };
const SCHEDULER_INTERVAL = 30_000; // 30 seconds

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// Track active dispatched tasks: taskId → { agentName, dispatchedAt, sessionKey }
const activeTasks = new Map();

// --- Dispatch task to agent via /hooks/agent ---
async function dispatchToAgent(task) {
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
**Dispatched by:** ${task.dispatched_by}

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
        if (task?.status === 'done' || task?.status === 'failed') {
          if (activeTasks.has(task.id)) {
            console.log(`[TRACKER] Task ${task.id} completed (${task.status}), removing from active tracking`);
            activeTasks.delete(task.id);
          }
        }

        // QA routing: when task moves to "done", move it to "qa" and dispatch to Beta
        // No separate QA task — the original task goes through QA itself
        if (task?.status === "done" && eventType === "UPDATE" && prev?.status !== "done") {
          // Don't QA tasks that were already QA'd (prevent loops)
          if (task.type === "qa") return;

          console.log(`[QA] Task ${task.id} ("${task.title}") → moving to qa, dispatching to beta`);

          try {
            // Move task to "qa" status (waiting for QA)
            await supabase
              .from("agent_tasks")
              .update({ status: "qa", qa_agent: "beta" })
              .eq("id", task.id);

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
                // Move to qa_testing (Beta is actively reviewing)
                await supabase
                  .from("agent_tasks")
                  .update({ status: "qa_testing" })
                  .eq("id", task.id);
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
  if (req.url === "/health" || req.url === "/") {
    let capacity = {};
    try {
      const { data } = await supabase
        .from("agent_tasks")
        .select("assigned_agent")
        .in("status", ["assigned", "in_progress"]);
      for (const [name, config] of Object.entries(AGENT_CONFIG)) {
        const load = (data || []).filter(t => t.assigned_agent?.toLowerCase() === name).length;
        capacity[name] = { load, max: config.maxConcurrent, available: config.maxConcurrent - load };
      }
    } catch {}
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, service: "task-dispatcher", activeTasks: activeTasks.size, capacity }));
  } else {
    res.writeHead(404);
    res.end("Not found");
  }
}).listen(PORT, () => {
  console.log(`[HEALTH] Listening on :${PORT}`);
});

// --- Unified Task Monitor ---
// Replaces both watchdog and session checker
// Queries ALL in_progress tasks from Supabase (not just locally tracked ones)
// Checks if agent session still exists, auto-closes if not
const MONITOR_INTERVAL = 30_000; // 30 seconds
const TASK_HARD_TIMEOUT = 10 * 60 * 1000; // 10 minutes for normal tasks
const QA_HARD_TIMEOUT = 20 * 60 * 1000; // 20 minutes for QA tasks (Beta is slow)
const SESSION_GONE_GRACE = 90_000; // 1.5 minute grace after session disappears

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

      if (sessionAlive) {
        // Session is active — only hard-timeout if REALLY old (safety net)
        const startTime = task.started_at || task.created_at;
        const timeout = task.type === "qa" ? QA_HARD_TIMEOUT : TASK_HARD_TIMEOUT;
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
          const timeout = task.type === "qa" ? QA_HARD_TIMEOUT : TASK_HARD_TIMEOUT;
          const elapsed = startTime ? Date.now() - new Date(startTime).getTime() : 0;

          if (elapsed > timeout) {
            console.log(`[MONITOR] Timeout + session gone: task ${task.id} ("${task.title}") → failed (${Math.round(elapsed / 60000)}min)`);
            await supabase.from("agent_tasks").update({
              status: "failed",
              error: `Timeout: session ended after ${Math.round(elapsed / 60000)} minutes without completion`,
              completed_at: new Date().toISOString(),
            }).eq("id", task.id);
          } else {
            console.log(`[MONITOR] Auto-closing task ${task.id} ("${task.title}") — session ended normally`);
            await supabase.from("agent_tasks").update({
              status: "done",
              completed_at: new Date().toISOString(),
              result: { output: "Auto-closed by dispatcher: agent session ended" },
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
    const { data: activeTasks_db, error: activeErr } = await supabase
      .from("agent_tasks")
      .select("assigned_agent")
      .in("status", ["assigned", "in_progress"]);

    if (activeErr) {
      console.error("[SCHEDULER] Error fetching active tasks:", activeErr.message);
      return;
    }

    const agentLoad = {};
    for (const name of Object.keys(AGENT_CONFIG)) agentLoad[name] = 0;
    for (const t of activeTasks_db || []) {
      const agent = t.assigned_agent?.toLowerCase();
      if (agent && agentLoad[agent] !== undefined) agentLoad[agent]++;
    }

    const available = [];
    for (const [name, config] of Object.entries(AGENT_CONFIG)) {
      const remaining = config.maxConcurrent - (agentLoad[name] || 0);
      if (remaining > 0) {
        available.push({ name, remaining, types: config.types });
      }
    }

    if (available.length === 0) return;

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
      if (task.assigned_agent) {
        const hintAgent = task.assigned_agent.toLowerCase();
        const agentSlot = available.find(a => a.name === hintAgent && a.remaining > 0);
        if (agentSlot) {
          console.log(`[SCHEDULER] Assigning task ${task.id} ("${task.title}") → ${hintAgent} (hint)`);
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
      const bestAgent = available
        .filter(a => a.remaining > 0 && a.types.includes(taskType))
        .sort((a, b) => b.remaining - a.remaining)[0];

      if (bestAgent) {
        console.log(`[SCHEDULER] Auto-assigning task ${task.id} ("${task.title}") → ${bestAgent.name} (type: ${taskType})`);
        await supabase
          .from("agent_tasks")
          .update({ status: "assigned", assigned_agent: bestAgent.name })
          .eq("id", task.id);
        bestAgent.remaining--;
        assigned++;
      }
    }

    if (assigned > 0) {
      console.log(`[SCHEDULER] Assigned ${assigned} tasks this cycle`);
    }
  } catch (e) {
    console.error("[SCHEDULER] Error:", e.message);
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

// Graceful shutdown
process.on("SIGTERM", () => {
  console.log("[SHUTDOWN] Received SIGTERM");
  process.exit(0);
});
