import { describe, it, expect } from 'vitest';
import { classifyError, getTriageAction, ERROR_CATEGORIES, getCategoryMeta } from '../error-classifier.js';

describe('classifyError', () => {
  it('classifies merge conflicts', () => {
    expect(classifyError('CONFLICT (content): merge conflict in src/index.js').category).toBe('merge_conflict');
    expect(classifyError('PR is not mergeable, mergeStateStatus: dirty').category).toBe('merge_conflict');
    expect(classifyError('Cannot merge: rebase needed').category).toBe('merge_conflict');
  });

  it('classifies CI failures', () => {
    expect(classifyError('CI check failed: tests did not pass').category).toBe('ci_failure');
    expect(classifyError('Build failed with exit code 1').category).toBe('ci_failure');
    expect(classifyError('npm ERR! test failed').category).toBe('ci_failure');
    expect(classifyError('GitHub Actions workflow failed').category).toBe('ci_failure');
  });

  it('classifies timeouts', () => {
    expect(classifyError('Absolute timeout: task ran for 45 minutes').category).toBe('timeout');
    expect(classifyError('Idle timeout: neo-worker session idle >5min').category).toBe('timeout');
    expect(classifyError('Request timed out after 30s').category).toBe('timeout');
  });

  it('classifies session lost', () => {
    expect(classifyError('Session ended without reporting completion').category).toBe('session_lost');
    expect(classifyError('Agent crashed during execution').category).toBe('session_lost');
    expect(classifyError('Dispatch lost during restart — re-queued').category).toBe('session_lost');
    expect(classifyError('ECONNREFUSED 127.0.0.1:18789').category).toBe('session_lost');
  });

  it('classifies QA rejections', () => {
    expect(classifyError('QA failed: missing database migration file').category).toBe('qa_rejection');
    expect(classifyError('No PR URL found — cannot perform code review').category).toBe('qa_rejection');
    expect(classifyError('QA rejected: rubber-stamped without review').category).toBe('qa_rejection');
  });

  it('classifies auth errors', () => {
    expect(classifyError('Auth preflight failed for neo-worker (HTTP 401)').category).toBe('auth_error');
    expect(classifyError('Token expired, unauthorized access').category).toBe('auth_error');
    expect(classifyError('403 Forbidden: permission denied').category).toBe('auth_error');
  });

  it('classifies resource errors', () => {
    expect(classifyError('Container killed: out of memory (OOMKilled)').category).toBe('resource_error');
    expect(classifyError('ENOSPC: no space left on device').category).toBe('resource_error');
    expect(classifyError('Pod evicted due to memory limit').category).toBe('resource_error');
  });

  it('returns unknown for unrecognized errors', () => {
    const result = classifyError('Something weird happened');
    expect(result.category).toBe('unknown');
    expect(result.confidence).toBe('low');
  });

  it('handles null/empty input', () => {
    expect(classifyError(null).category).toBe('unknown');
    expect(classifyError('').category).toBe('unknown');
    expect(classifyError(undefined).category).toBe('unknown');
  });

  it('returns matched pattern text', () => {
    const result = classifyError('Auth preflight failed for neo (HTTP 401)');
    expect(result.matchedPattern).toBeTruthy();
    expect(result.confidence).toBe('high');
  });
});

describe('getTriageAction', () => {
  it('returns auto_retry for retriable categories', () => {
    expect(getTriageAction('timeout').retriable).toBe(true);
    expect(getTriageAction('timeout').action).toBe('auto_retry');
    expect(getTriageAction('session_lost').retriable).toBe(true);
    expect(getTriageAction('session_lost').action).toBe('auto_retry');
  });

  it('returns auto_retry_rebase for merge conflicts', () => {
    expect(getTriageAction('merge_conflict').retriable).toBe(true);
    expect(getTriageAction('merge_conflict').action).toBe('auto_retry_rebase');
  });

  it('returns route_to_coding_agent for code issues', () => {
    expect(getTriageAction('ci_failure').action).toBe('route_to_coding_agent');
    expect(getTriageAction('qa_rejection').action).toBe('route_to_coding_agent');
  });

  it('flags manual intervention for auth/resource', () => {
    expect(getTriageAction('auth_error').action).toBe('flag_manual');
    expect(getTriageAction('resource_error').action).toBe('flag_infra');
  });

  it('handles unknown category', () => {
    expect(getTriageAction('nonexistent').action).toBe('none');
  });
});

describe('getCategoryMeta', () => {
  it('returns metadata for all categories', () => {
    for (const cat of Object.keys(ERROR_CATEGORIES)) {
      const meta = getCategoryMeta(cat);
      expect(meta.label).toBeTruthy();
      expect(meta.color).toBeTruthy();
      expect(meta.icon).toBeTruthy();
    }
  });

  it('returns unknown for invalid category', () => {
    expect(getCategoryMeta('bogus').label).toBe('Unknown');
  });
});
