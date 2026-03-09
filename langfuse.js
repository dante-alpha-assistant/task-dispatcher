import { Langfuse } from "@langfuse/client";

let langfuse = null;

export function initLangfuse() {
  const publicKey = process.env.LANGFUSE_PUBLIC_KEY;
  const secretKey = process.env.LANGFUSE_SECRET_KEY;
  const baseUrl = process.env.LANGFUSE_BASE_URL || "https://cloud.langfuse.com";

  if (!publicKey || !secretKey) {
    console.log("[LANGFUSE] Not configured (missing LANGFUSE_PUBLIC_KEY/SECRET_KEY) — cost tracking disabled");
    return null;
  }

  langfuse = new Langfuse({ publicKey, secretKey, baseUrl });
  console.log("[LANGFUSE] Initialized — task-level cost tracking enabled");
  return langfuse;
}

export function getLangfuse() {
  return langfuse;
}

/**
 * Create a trace for a task lifecycle phase.
 * Phases: dispatch, coding, qa_testing, deploy
 */
export function traceTaskPhase(task, phase, agentName) {
  if (!langfuse) return null;

  const trace = langfuse.trace({
    name: `task-${phase}`,
    id: `${task.id}-${phase}-${Date.now()}`,
    sessionId: task.id, // Groups all phases under one session
    userId: agentName || "dispatcher",
    metadata: {
      taskId: task.id,
      taskTitle: task.title,
      taskType: task.type,
      phase,
      agentName,
      priority: task.priority,
      deployTarget: task.deploy_target,
    },
    tags: [phase, task.type, agentName || "system"].filter(Boolean),
  });

  return trace;
}

/**
 * Log a generation (LLM call) within a task phase.
 * Called when we know the token usage.
 */
export function logGeneration(trace, { model, inputTokens, outputTokens, cost, durationMs, input, output }) {
  if (!trace) return;

  trace.generation({
    name: `${model}-call`,
    model,
    input: input || "task prompt",
    output: output || "agent response",
    usage: {
      input: inputTokens || 0,
      output: outputTokens || 0,
      total: (inputTokens || 0) + (outputTokens || 0),
    },
    ...(cost !== undefined && { costDetails: { total: cost } }),
    ...(durationMs && { completionStartTime: new Date(Date.now() - durationMs) }),
  });
}

/**
 * Record task phase completion with cost summary.
 * Stores cost data in the task's metadata field.
 */
export async function recordPhaseCost(supabase, taskId, phase, costData) {
  if (!costData) return;

  try {
    // Get current metadata
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

    // Calculate totals
    const phases = Object.values(metadata.costs);
    metadata.totalCost = phases.reduce((sum, p) => sum + (p.cost || 0), 0);
    metadata.totalTokens = phases.reduce((sum, p) => sum + (p.totalTokens || 0), 0);

    await supabase
      .from("agent_tasks")
      .update({ metadata })
      .eq("id", taskId);

  } catch (e) {
    console.error(`[LANGFUSE] Error recording phase cost for ${taskId}:`, e.message);
  }
}

/**
 * Flush pending events to Langfuse Cloud.
 * Call periodically or on shutdown.
 */
export async function flushLangfuse() {
  if (langfuse) {
    await langfuse.flush();
  }
}
