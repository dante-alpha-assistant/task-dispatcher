import { describe, it, expect } from 'vitest';

// Health checker logic extracted for testing
// These mirror the detection rules in index.js healthChecker()

function detectHealthFlags(task, now = Date.now()) {
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

  return flags;
}

describe('Health Checker — Suspicious State Detection', () => {
  const now = Date.now();

  it('1. detects completed + active error + no QA pass', () => {
    const flags = detectHealthFlags({ status: 'completed', error: 'Something broke', metadata: {} });
    expect(flags).toHaveLength(1);
    expect(flags[0].code).toBe('COMPLETED_WITH_ERROR');
  });

  it('1. skips if QA passed', () => {
    const flags = detectHealthFlags({ status: 'completed', error: 'err', metadata: { qa_result: { passed: true } } });
    expect(flags).toHaveLength(0);
  });

  it('2. detects deployed + active error (critical)', () => {
    const flags = detectHealthFlags({ status: 'deployed', error: 'Unresolved bug' });
    expect(flags).toHaveLength(1);
    expect(flags[0].code).toBe('DEPLOYED_WITH_ERROR');
    expect(flags[0].severity).toBe('critical');
  });

  it('3. detects in_progress + no assigned_agent', () => {
    const flags = detectHealthFlags({ status: 'in_progress', assigned_agent: null });
    expect(flags).toHaveLength(1);
    expect(flags[0].code).toBe('IN_PROGRESS_UNASSIGNED');
  });

  it('3. no flag when agent assigned', () => {
    const flags = detectHealthFlags({ status: 'in_progress', assigned_agent: 'neo' });
    expect(flags).toHaveLength(0);
  });

  it('4. detects qa_testing stuck without agent >10 min', () => {
    const flags = detectHealthFlags({
      status: 'qa_testing', qa_agent: null,
      updated_at: new Date(now - 15 * 60_000).toISOString(),
    }, now);
    expect(flags).toHaveLength(1);
    expect(flags[0].code).toBe('QA_STUCK_NO_AGENT');
  });

  it('4. no flag if qa_testing < 10 min', () => {
    const flags = detectHealthFlags({
      status: 'qa_testing', qa_agent: null,
      updated_at: new Date(now - 5 * 60_000).toISOString(),
    }, now);
    expect(flags).toHaveLength(0);
  });

  it('5. detects zombie task (in_progress >60min, no update >30min)', () => {
    const flags = detectHealthFlags({
      status: 'in_progress', assigned_agent: 'neo',
      started_at: new Date(now - 90 * 60_000).toISOString(),
      updated_at: new Date(now - 45 * 60_000).toISOString(),
    }, now);
    expect(flags).toHaveLength(1);
    expect(flags[0].code).toBe('ZOMBIE_TASK');
  });

  it('5. no flag if recently updated', () => {
    const flags = detectHealthFlags({
      status: 'in_progress', assigned_agent: 'neo',
      started_at: new Date(now - 90 * 60_000).toISOString(),
      updated_at: new Date(now - 5 * 60_000).toISOString(),
    }, now);
    expect(flags).toHaveLength(0);
  });

  it('6. detects coding task completed without PR', () => {
    const flags = detectHealthFlags({ status: 'completed', type: 'coding', pull_request_url: null });
    expect(flags).toHaveLength(1);
    expect(flags[0].code).toBe('CODING_NO_PR');
  });

  it('6. no flag with PR present', () => {
    const flags = detectHealthFlags({ status: 'completed', type: 'coding', pull_request_url: ['https://github.com/org/repo/pull/1'] });
    expect(flags).toHaveLength(0);
  });

  it('7. detects todo + assigned_agent', () => {
    const flags = detectHealthFlags({ status: 'todo', assigned_agent: 'neo' });
    expect(flags).toHaveLength(1);
    expect(flags[0].code).toBe('TODO_ASSIGNED');
    expect(flags[0].severity).toBe('info');
  });

  it('8. detects completed with failed QA retries >= 2', () => {
    const flags = detectHealthFlags({
      status: 'completed',
      metadata: { qa_retries: 3, qa_result: { passed: false } },
    });
    expect(flags.some(f => f.code === 'COMPLETED_QA_FAILED')).toBe(true);
  });

  it('clean task has no flags', () => {
    const flags = detectHealthFlags({
      status: 'in_progress', assigned_agent: 'neo',
      started_at: new Date(now - 10 * 60_000).toISOString(),
      updated_at: new Date(now - 2 * 60_000).toISOString(),
    }, now);
    expect(flags).toHaveLength(0);
  });

  it('multiple flags can coexist', () => {
    const flags = detectHealthFlags({
      status: 'completed', type: 'coding', error: 'bug',
      pull_request_url: null, metadata: { qa_retries: 2, qa_result: { passed: false } },
    });
    expect(flags.length).toBeGreaterThanOrEqual(3); // COMPLETED_WITH_ERROR + CODING_NO_PR + COMPLETED_QA_FAILED
  });
});
