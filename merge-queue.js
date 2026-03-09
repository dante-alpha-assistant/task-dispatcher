/**
 * Merge Queue — Sequential PR merging per repository.
 * 
 * Flow:
 * 1. QA passes → task status = completed (QA does NOT merge)
 * 2. Merge queue picks up completed coding tasks with PRs
 * 3. One task per repo processed at a time (per-repo mutex)
 * 4. Rebase PR against latest main
 * 5. If rebase succeeds → squash merge → mark deployed
 * 6. If conflict → back to todo with rebase context for coding agent
 */

// Per-repo merge locks: repo → { taskId, startedAt }
const mergeLocks = new Map();

// Queue processing interval
const MERGE_QUEUE_INTERVAL = 30_000; // 30s

/**
 * Start the merge queue processor.
 * @param {object} supabase - Supabase client
 * @param {function} logTaskActivity - Activity logger
 */
export function startMergeQueue(supabase, logTaskActivity) {
  console.log("[MERGE-QUEUE] Started — processing every 30s");

  setInterval(async () => {
    try {
      await processMergeQueue(supabase, logTaskActivity);
    } catch (e) {
      console.error("[MERGE-QUEUE] Error:", e.message);
    }
  }, MERGE_QUEUE_INTERVAL);

  // Run immediately on start
  setTimeout(() => processMergeQueue(supabase, logTaskActivity).catch(() => {}), 5000);
}

async function processMergeQueue(supabase, logTaskActivity) {
  // Fetch completed coding tasks with PRs that haven't been merged yet
  const { data: tasks } = await supabase
    .from("agent_tasks")
    .select("id, title, type, pull_request_url, repository_url, metadata, deploy_target")
    .eq("status", "completed")
    .eq("type", "coding")
    .not("pull_request_url", "is", null)
    .order("completed_at", { ascending: true });

  if (!tasks?.length) return;

  // Group by repo
  const byRepo = new Map();
  for (const task of tasks) {
    const prUrls = task.pull_request_url || [];
    if (!prUrls.length) continue;

    const prUrl = prUrls[0];
    const repoMatch = prUrl.match(/github\.com\/([\w-]+\/[\w-]+)\/pull\/(\d+)/);
    if (!repoMatch) continue;

    const repo = repoMatch[1];
    const prNumber = parseInt(repoMatch[2]);

    if (!byRepo.has(repo)) byRepo.set(repo, []);
    byRepo.get(repo).push({ ...task, repo, prNumber, prUrl });
  }

  // Process one task per repo
  for (const [repo, repoTasks] of byRepo) {
    // Check lock
    const lock = mergeLocks.get(repo);
    if (lock) {
      // Timeout stale locks after 5 minutes
      if (Date.now() - lock.startedAt > 5 * 60 * 1000) {
        console.log(`[MERGE-QUEUE] Stale lock for ${repo} (task ${lock.taskId}) — releasing`);
        mergeLocks.delete(repo);
      } else {
        continue; // Repo is busy
      }
    }

    // Pick first task (FIFO by completed_at)
    const task = repoTasks[0];

    // Acquire lock
    mergeLocks.set(repo, { taskId: task.id, startedAt: Date.now() });
    console.log(`[MERGE-QUEUE] Processing task ${task.id.slice(0, 8)} — PR #${task.prNumber} on ${repo}`);

    try {
      await mergeTask(supabase, logTaskActivity, task);
    } catch (e) {
      console.error(`[MERGE-QUEUE] Failed to merge task ${task.id.slice(0, 8)}:`, e.message);
      await logTaskActivity(task.id, "merge_error", null, `Merge queue error: ${e.message}`, "dispatcher");
    } finally {
      mergeLocks.delete(repo);
    }
  }
}

async function mergeTask(supabase, logTaskActivity, task) {
  const { id, repo, prNumber, prUrl } = task;
  const ghToken = process.env.GH_TOKEN || process.env.GITHUB_TOKEN || "";

  if (!ghToken) {
    console.error("[MERGE-QUEUE] No GH_TOKEN — cannot merge");
    return;
  }

  const headers = {
    Authorization: `token ${ghToken}`,
    Accept: "application/vnd.github.v3+json",
    "Content-Type": "application/json",
  };

  // 1. Check PR status
  const prRes = await fetch(`https://api.github.com/repos/${repo}/pulls/${prNumber}`, { headers });
  if (!prRes.ok) {
    console.error(`[MERGE-QUEUE] PR #${prNumber} fetch failed: ${prRes.status}`);
    return;
  }
  const pr = await prRes.json();

  if (pr.state === "closed" || pr.merged) {
    console.log(`[MERGE-QUEUE] PR #${prNumber} already ${pr.merged ? "merged" : "closed"} — marking task deployed`);
    await supabase.from("agent_tasks").update({
      status: pr.merged ? "deployed" : "failed",
      ...(pr.merged ? {} : { error: "PR was closed without merging" }),
    }).eq("id", id);
    await logTaskActivity(id, "merge_complete", null, `PR #${prNumber} was already ${pr.merged ? "merged" : "closed"}`, "merge-queue");
    return;
  }

  // 2. Check mergeable status
  if (pr.mergeable === false || pr.mergeable_state === "dirty") {
    console.log(`[MERGE-QUEUE] PR #${prNumber} has conflicts — sending back for rebase`);

    // Set rebase metadata and send back to coding agent
    const metadata = task.metadata || {};
    metadata.rebase_requested = true;
    metadata.rebase_pr = {
      number: prNumber,
      repo,
      branch: pr.head.ref,
      base: pr.base.ref,
      url: prUrl,
    };

    await supabase.from("agent_tasks").update({
      status: "todo",
      assigned_agent: null,
      qa_agent: null,
      result: null,
      qa_result: null,
      started_at: null,
      completed_at: null,
      metadata,
      error: `Merge queue: PR #${prNumber} has conflicts. Branch ${pr.head.ref} needs rebase against ${pr.base.ref}.`,
    }).eq("id", id);

    await logTaskActivity(id, "merge_conflict", null, `PR #${prNumber} has merge conflicts — sent back for rebase`, "merge-queue");
    return;
  }

  // 3. If mergeable is null, GitHub is still computing — wait
  if (pr.mergeable === null) {
    console.log(`[MERGE-QUEUE] PR #${prNumber} mergeable status unknown — will retry next cycle`);
    return; // Don't release lock yet — will timeout after 5 min
  }

  // 4. Check if CI is passing (check runs)
  const checksRes = await fetch(`https://api.github.com/repos/${repo}/commits/${pr.head.sha}/check-runs`, { headers });
  if (checksRes.ok) {
    const checks = await checksRes.json();
    const failedChecks = checks.check_runs?.filter(c => c.conclusion === "failure") || [];
    const pendingChecks = checks.check_runs?.filter(c => c.status !== "completed") || [];

    if (pendingChecks.length > 0) {
      console.log(`[MERGE-QUEUE] PR #${prNumber} has ${pendingChecks.length} pending checks — will retry`);
      return;
    }

    if (failedChecks.length > 0) {
      console.log(`[MERGE-QUEUE] PR #${prNumber} has ${failedChecks.length} failed checks — sending back`);
      await supabase.from("agent_tasks").update({
        status: "todo",
        assigned_agent: null,
        result: null,
        qa_result: null,
        started_at: null,
        completed_at: null,
        error: `Merge queue: CI checks failed on PR #${prNumber}: ${failedChecks.map(c => c.name + ": " + c.conclusion).join(", ")}`,
      }).eq("id", id);
      await logTaskActivity(id, "merge_ci_failed", null, `PR #${prNumber} CI failed: ${failedChecks.map(c => c.name).join(", ")}`, "merge-queue");
      return;
    }
  }

  // 5. All clear — squash merge
  console.log(`[MERGE-QUEUE] Merging PR #${prNumber} on ${repo}...`);
  const mergeRes = await fetch(`https://api.github.com/repos/${repo}/pulls/${prNumber}/merge`, {
    method: "PUT",
    headers,
    body: JSON.stringify({
      merge_method: "squash",
      commit_title: `${pr.title} (#${prNumber})`,
    }),
  });

  if (mergeRes.ok) {
    console.log(`[MERGE-QUEUE] ✅ PR #${prNumber} merged successfully`);

    // Delete the branch
    fetch(`https://api.github.com/repos/${repo}/git/refs/heads/${pr.head.ref}`, {
      method: "DELETE",
      headers,
    }).catch(() => {});

    // Update task to deployed
    await supabase.from("agent_tasks").update({
      status: "deployed",
      deployment_url: prUrl,
    }).eq("id", id);

    await logTaskActivity(id, "merge_complete", null, `PR #${prNumber} squash-merged and branch deleted`, "merge-queue");
  } else {
    const errBody = await mergeRes.json().catch(() => ({}));
    const errMsg = errBody.message || `HTTP ${mergeRes.status}`;
    console.error(`[MERGE-QUEUE] ❌ Merge failed for PR #${prNumber}: ${errMsg}`);

    if (errMsg.includes("not mergeable") || errMsg.includes("conflict")) {
      // Conflict appeared between check and merge — retry next cycle
      console.log(`[MERGE-QUEUE] Conflict appeared during merge — will retry`);
    } else {
      await logTaskActivity(id, "merge_error", null, `Merge failed: ${errMsg}`, "merge-queue");
    }
  }
}

export { processMergeQueue };
