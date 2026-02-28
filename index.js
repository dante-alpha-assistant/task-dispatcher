import { createClient } from "@supabase/supabase-js";

// --- Config ---
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

// Agent registry: name → gateway hook URL + token
const AGENTS = {
  neo: {
    url: process.env.NEO_HOOKS_URL || `http://neo.agents.svc.cluster.local:18789/hooks/agent`,
    token: process.env.NEO_HOOKS_TOKEN,
  },
  mu: {
    url: process.env.MU_HOOKS_URL || `http://mu.agents.svc.cluster.local:18789/hooks/agent`,
    token: process.env.MU_HOOKS_TOKEN,
  },
  beta: {
    url: process.env.BETA_HOOKS_URL || `http://beta.agents.svc.cluster.local:18789/hooks/agent`,
    token: process.env.BETA_HOOKS_TOKEN,
  },
  flow: {
    url: process.env.FLOW_HOOKS_URL || `http://flow.agents.svc.cluster.local:18789/hooks/agent`,
    token: process.env.FLOW_HOOKS_TOKEN,
  },
};

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

        // QA routing: when task moves to "done", create QA task for Beta
        if (task?.status === "done" && eventType === "UPDATE" && prev?.status !== "done") {
          // Don't QA tasks that are already QA tasks (prevent infinite loop)
          if (task.type === "qa") return;

          console.log(`[QA] Task ${task.id} ("${task.title}") moved to done — creating QA task for beta`);

          const qaDescription = [
            `## QA Review: ${task.title}`,
            ``,
            `**Original Task:** ${task.id}`,
            `**Type:** ${task.type}`,
            `**Assigned to:** ${task.assigned_agent}`,
            `**Dispatched by:** ${task.dispatched_by}`,
            ``,
            `### Description`,
            task.description || "(none)",
            ``,
            `### Result`,
            task.result ? JSON.stringify(task.result, null, 2) : "(no result reported)",
            ``,
            `### Acceptance Criteria`,
            task.acceptance_criteria || "Verify the task was completed correctly based on the description and result.",
            ``,
            `### Your Job`,
            `1. Review the result against the acceptance criteria`,
            `2. If the task involved code: check the repo, run tests if possible`,
            `3. If the task involved a URL/service: test it`,
            `4. Update the ORIGINAL task (not this QA task) status:`,
            `   - If passed: update to "completed"`,
            `   - If failed: update to "failed" with error describing what's wrong`,
          ].join("\n");

          try {
            const { data: qaTask, error } = await supabase
              .from("agent_tasks")
              .insert({
                title: `QA: ${task.title}`,
                description: qaDescription,
                type: "qa",
                priority: task.priority,
                status: "assigned",
                assigned_agent: "beta",
                dispatched_by: "dispatcher",
                acceptance_criteria: task.acceptance_criteria,
              })
              .select()
              .single();

            if (error) {
              console.error(`[QA] Failed to create QA task: ${error.message}`);
            } else {
              console.log(`[QA] Created QA task ${qaTask.id} assigned to beta`);

              // Also update original task status to "qa"
              await supabase
                .from("agent_tasks")
                .update({ status: "qa", qa_agent: "beta" })
                .eq("id", task.id);
            }
          } catch (e) {
            console.error(`[QA] Error creating QA task: ${e.message}`);
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
createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ ok: true, service: "task-dispatcher", activeTasks: activeTasks.size }));
}).listen(PORT, () => {
  console.log(`[HEALTH] Listening on :${PORT}`);
});

// --- Watchdog: auto-fail stale tasks ---
const WATCHDOG_INTERVAL = 5 * 60 * 1000; // 5 minutes
const TASK_TIMEOUT = 30 * 60 * 1000; // 30 minutes

async function watchdog() {
  try {
    const cutoff = new Date(Date.now() - TASK_TIMEOUT).toISOString();
    const { data: staleTasks, error } = await supabase
      .from("agent_tasks")
      .select("id, title, assigned_agent, started_at")
      .eq("status", "in_progress")
      .lt("started_at", cutoff);

    if (error) {
      console.error("[WATCHDOG] Query error:", error.message);
      return;
    }

    for (const task of staleTasks || []) {
      console.log(`[WATCHDOG] Timing out task ${task.id} ("${task.title}") assigned to ${task.assigned_agent}`);
      await supabase
        .from("agent_tasks")
        .update({
          status: "failed",
          error: "Timeout: agent did not complete within 30 minutes",
          completed_at: new Date().toISOString(),
        })
        .eq("id", task.id);
    }

    if (staleTasks?.length) {
      console.log(`[WATCHDOG] Timed out ${staleTasks.length} stale tasks`);
    }
  } catch (e) {
    console.error("[WATCHDOG] Error:", e.message);
  }
}

// --- Session checker: poll agent gateways for hook session status ---
const SESSION_CHECK_INTERVAL = 10_000; // 10 seconds

async function checkActiveSessions() {
  for (const [taskId, info] of activeTasks) {
    try {
      const agent = AGENTS[info.agentName];
      if (!agent) continue;

      const resp = await fetch(agent.url.replace('/hooks/agent', '/tools/invoke'), {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${agent.token}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          tool: 'sessions_list',
          params: { limit: 50, messageLimit: 0 },
        }),
      });

      if (!resp.ok) {
        console.log(`[SESSION] Cannot reach ${info.agentName} gateway for task ${taskId}`);
        continue;
      }

      const data = await resp.json();
      const sessions = data?.result?.details?.sessions || [];
      const hookSession = sessions.find(s => s.key === `agent:main:${info.sessionKey}`);

      if (!hookSession) {
        const { data: task } = await supabase
          .from('agent_tasks')
          .select('status')
          .eq('id', taskId)
          .single();

        if (task?.status === 'in_progress') {
          const elapsed = Date.now() - info.dispatchedAt;
          if (elapsed > 60_000) {
            console.log(`[SESSION] Task ${taskId} session gone, marking done (agent likely completed without updating)`);
            await supabase
              .from('agent_tasks')
              .update({
                status: 'done',
                completed_at: new Date().toISOString(),
                result: { output: 'Session completed (auto-detected by dispatcher)' },
              })
              .eq('id', taskId);
            activeTasks.delete(taskId);
          }
        } else {
          activeTasks.delete(taskId);
        }
      } else {
        const lastActivity = hookSession.updatedAt;
        const idleSec = Math.floor((Date.now() - lastActivity) / 1000);

        if (idleSec > 0 && idleSec % 60 < 10) {
          console.log(`[SESSION] Task ${taskId} → ${info.agentName}: session active, idle ${idleSec}s`);
        }
      }
    } catch (e) {
      console.error(`[SESSION] Error checking task ${taskId}: ${e.message}`);
    }
  }
}

// --- Start ---
subscribe();
setInterval(watchdog, WATCHDOG_INTERVAL);
watchdog();
setInterval(checkActiveSessions, SESSION_CHECK_INTERVAL);
console.log(`[BOOT] Session checker running every ${SESSION_CHECK_INTERVAL / 1000}s`);

// Graceful shutdown
process.on("SIGTERM", () => {
  console.log("[SHUTDOWN] Received SIGTERM");
  process.exit(0);
});
