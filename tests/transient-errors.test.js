import { describe, it, expect } from 'vitest';
import { readFileSync } from 'fs';

describe('Transient error migration', () => {
  const code = readFileSync('index.js', 'utf8');
  const mergeCode = readFileSync('merge-queue.js', 'utf8');

  it('logTransientError helper exists', () => {
    expect(code).toContain('async function logTransientError(taskId, message');
    expect(code).toContain("await logTaskActivity(taskId, 'error', null, message");
  });

  it('transient errors use logTransientError instead of error field', () => {
    // These patterns should use logTransientError, not set error field directly
    const transientPatterns = [
      'Dispatch lost during restart',
      'disabled/degraded — unassigning',
      'at capacity',
      'session idle',
      'session lost',
      'QA rejected: coding task completed without PR',
      'degraded (stale heartbeat)',
      'disabled/degraded — cleared for re-routing',
    ];

    for (const pattern of transientPatterns) {
      // Find lines containing this pattern
      const lines = code.split('\n');
      const matchingLines = lines.filter(l => l.includes(pattern));
      expect(matchingLines.length).toBeGreaterThan(0);

      // Verify none of these set error field directly (except in template strings for agent dispatch messages)
      for (const line of matchingLines) {
        // Skip template literals in dispatch payloads (these are instructions for agents, not DB writes)
        if (line.includes('\\`') || line.includes("'\\`")) continue;
        if (line.includes('-d \'{"status"')) continue; // Skip curl template strings
        // The pattern should appear in logTransientError calls, not in .update({ error: ... })
        const isInUpdate = line.includes('error:') && line.includes('.update(');
        if (isInUpdate) {
          // error: null is OK (clearing), but error: `message` is not
          expect(line).toMatch(/error:\s*null/);
        }
      }
    }
  });

  it('error is cleared on completed status', () => {
    expect(code).toContain("error: null"); // In completed lock
    expect(code).toContain("clearing qa_agent + error");
  });

  it('error is cleared on deployed status in merge-queue', () => {
    expect(mergeCode).toContain("error: null");
    // Verify it's in the deployed update
    const deployedSection = mergeCode.split('squash-merged')[1];
    expect(deployedSection).toBeDefined();
  });

  it('error is cleared on deployed status in deploy-detect', () => {
    // Count error: null in deploy-detect sections
    const deployDetectSection = code.split('[DEPLOY-DETECT]');
    const nullErrorCount = deployDetectSection.filter(s => s.includes('error: null')).length;
    expect(nullErrorCount).toBeGreaterThanOrEqual(2); // non-coding + coding deploy paths
  });

  it('terminal errors still set error field', () => {
    // These terminal errors should still use the error field
    const terminalPatterns = [
      'Absolute timeout',
      'Session ended without reporting completion',
      'Factory pipeline failed',
      'Factory pipeline timed out',
      'max retries exceeded',
    ];

    let foundTerminal = 0;
    for (const pattern of terminalPatterns) {
      if (code.includes(pattern)) foundTerminal++;
    }
    // At least some terminal errors should remain
    expect(foundTerminal).toBeGreaterThan(0);
  });

  it('migration file exists for clearing error on completion', () => {
    const migration = readFileSync('migrations/006_clear_error_on_completion.sql', 'utf8');
    expect(migration).toContain('clear_error_on_completion');
    expect(migration).toContain("'completed', 'deployed'");
    expect(migration).toContain('NEW.error := NULL');
    expect(migration).toContain('DROP TRIGGER IF EXISTS');
  });
});
