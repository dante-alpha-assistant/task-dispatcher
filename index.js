import { createClient } from "@supabase/supabase-js";

// --- Config ---
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

// Agent registry: name → gateway hook URL + token
const AGENTS = {
  neo: {
    url: `http://neo.agents.svc.cluster.local:18789/hooks/agent`,
    token: process.env.NEO_GATEWAY_TOKEN,
  },
  mu: {
    url: `http://mu.agents.svc.cluster.local:18789/hooks/agent`,
    token: process.env.MU_GATEWAY_TOKEN,
  },
  beta: {
    url: `http://beta.agents.svc.cluster.local:18789/hooks/agent`,
    token: process.env.BETA_GATEWAY_TOKEN,
  },
  flow: {
    url: `http://flow.agents.svc.cluster.local:18789/hooks/agent`,
    token: process.env.FLOW_GATEWAY_TOKEN,
  },
};

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

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

  const message = [
    `## Task Assigned: ${task.title}`,
    task.description ? `\n${task.description}` : "",
    task.prompt ? `\n**Prompt:** ${task.prompt}` : "",
    `\n**Task ID:** ${task.id}`,
    `**Type:** ${task.type}`,
    `**Priority:** ${task.priority}`,
    `**Dispatched by:** ${task.dispatched_by}`,
    `\nWhen done, update the task status to 'done' or 'failed' via Supabase.`,
  ].join("\n");

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
      (payload) => {
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
  res.end(JSON.stringify({ ok: true, service: "task-dispatcher" }));
}).listen(PORT, () => {
  console.log(`[HEALTH] Listening on :${PORT}`);
});

// --- Start ---
subscribe();

// Graceful shutdown
process.on("SIGTERM", () => {
  console.log("[SHUTDOWN] Received SIGTERM");
  process.exit(0);
});
