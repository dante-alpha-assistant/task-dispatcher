/**
 * Langfuse Cloud integration — task-level cost tracking.
 * Uses REST API directly to avoid SDK version compatibility issues.
 */

const LANGFUSE_PUBLIC_KEY = process.env.LANGFUSE_PUBLIC_KEY || "";
const LANGFUSE_SECRET_KEY = process.env.LANGFUSE_SECRET_KEY || "";
const LANGFUSE_BASE_URL = process.env.LANGFUSE_BASE_URL || "https://cloud.langfuse.com";

let enabled = false;

export function initLangfuse() {
  if (!LANGFUSE_PUBLIC_KEY || !LANGFUSE_SECRET_KEY) {
    console.log("[LANGFUSE] Not configured — cost tracking disabled");
    return;
  }
  enabled = true;
  console.log("[LANGFUSE] Initialized — task-level cost tracking enabled");
}

function authHeader() {
  return "Basic " + Buffer.from(`${LANGFUSE_PUBLIC_KEY}:${LANGFUSE_SECRET_KEY}`).toString("base64");
}

/**
 * Create a trace for a task phase transition.
 */
export async function traceTaskPhase(task, phase, agentName) {
  if (!enabled) return;
  try {
    await fetch(`${LANGFUSE_BASE_URL}/api/public/ingestion`, {
      method: "POST",
      headers: { Authorization: authHeader(), "Content-Type": "application/json" },
      body: JSON.stringify({
        batch: [{
          id: `${task.id}-${phase}-${Date.now()}`,
          type: "trace-create",
          timestamp: new Date().toISOString(),
          body: {
            id: `${task.id}-${phase}-${Date.now()}`,
            name: `task-${phase}`,
            sessionId: task.id,
            userId: agentName || "dispatcher",
            metadata: {
              taskId: task.id,
              taskTitle: task.title,
              taskType: task.type,
              phase,
              priority: task.priority,
              deployTarget: task.deploy_target,
            },
            tags: [phase, task.type, agentName].filter(Boolean),
          },
        }],
      }),
    });
  } catch (e) {
    // Silent fail — don't break dispatch for telemetry
  }
}

/**
 * Record task phase cost in task metadata.
 */
export async function recordPhaseCost(supabase, taskId, phase, costData) {
  if (!costData) return;
  try {
    const { data: task } = await supabase
      .from("agent_tasks")
      .select("metadata")
      .eq("id", taskId)
      .single();

    const metadata = task?.metadata || {};
    if (!metadata.costs) metadata.costs = {};

    metadata.costs[phase] = {
      model: costData.model,
      inputTokens: costData.inputTokens || 0,
      outputTokens: costData.outputTokens || 0,
      totalTokens: (costData.inputTokens || 0) + (costData.outputTokens || 0),
      cost: costData.cost || 0,
      currency: "USD",
      durationMs: costData.durationMs || 0,
      timestamp: new Date().toISOString(),
    };

    const phases = Object.values(metadata.costs);
    metadata.totalCost = phases.reduce((sum, p) => sum + (p.cost || 0), 0);
    metadata.totalTokens = phases.reduce((sum, p) => sum + (p.totalTokens || 0), 0);

    await supabase.from("agent_tasks").update({ metadata }).eq("id", taskId);
  } catch (e) {
    console.error(`[LANGFUSE] Error recording phase cost: ${e.message}`);
  }
}

export async function flushLangfuse() {
  // No-op — REST API sends immediately
}

export function getLangfuse() { return enabled; }
export function logGeneration() {} // Stub for future use
