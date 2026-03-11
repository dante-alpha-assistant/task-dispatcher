import { describe, it, expect } from 'vitest';

// Extract the checkAgentCredentials function for testing
// Since it's embedded in index.js, we replicate the logic here
function checkAgentCredentials(appContext, agentCredentials, credentialType = 'coding') {
  if (!appContext) return { ok: true, missing: [] };
  const required = credentialType === 'qa'
    ? (appContext.required_qa_credentials || [])
    : (appContext.required_credentials || []);
  if (!required.length) return { ok: true, missing: [] };
  const available = new Set(agentCredentials || []);
  const missing = required.filter(c => !available.has(c));
  return { ok: missing.length === 0, missing };
}

describe('checkAgentCredentials', () => {
  it('returns ok when no app context', () => {
    const result = checkAgentCredentials(null, ['GH_TOKEN']);
    expect(result.ok).toBe(true);
    expect(result.missing).toEqual([]);
  });

  it('returns ok when app has no required credentials', () => {
    const app = { name: 'Test App', required_credentials: [], required_qa_credentials: [] };
    const result = checkAgentCredentials(app, ['GH_TOKEN']);
    expect(result.ok).toBe(true);
  });

  it('returns ok when agent has all required credentials', () => {
    const app = { name: 'Test App', required_credentials: ['GH_TOKEN', 'SUPABASE_KEY'] };
    const result = checkAgentCredentials(app, ['GH_TOKEN', 'SUPABASE_KEY', 'EXTRA_TOKEN'], 'coding');
    expect(result.ok).toBe(true);
    expect(result.missing).toEqual([]);
  });

  it('returns not ok with missing credentials', () => {
    const app = { name: 'Test App', required_credentials: ['GH_TOKEN', 'VERCEL_TOKEN'] };
    const result = checkAgentCredentials(app, ['GH_TOKEN'], 'coding');
    expect(result.ok).toBe(false);
    expect(result.missing).toEqual(['VERCEL_TOKEN']);
  });

  it('returns not ok when agent has no credentials', () => {
    const app = { name: 'Test App', required_credentials: ['GH_TOKEN'] };
    const result = checkAgentCredentials(app, [], 'coding');
    expect(result.ok).toBe(false);
    expect(result.missing).toEqual(['GH_TOKEN']);
  });

  it('checks qa credentials when credentialType is qa', () => {
    const app = {
      name: 'Test App',
      required_credentials: ['GH_TOKEN', 'VERCEL_TOKEN'],
      required_qa_credentials: ['GH_TOKEN'],
    };
    // Agent only has GH_TOKEN — should pass QA but fail coding
    const qaResult = checkAgentCredentials(app, ['GH_TOKEN'], 'qa');
    expect(qaResult.ok).toBe(true);

    const codingResult = checkAgentCredentials(app, ['GH_TOKEN'], 'coding');
    expect(codingResult.ok).toBe(false);
    expect(codingResult.missing).toEqual(['VERCEL_TOKEN']);
  });

  it('handles undefined credential arrays gracefully', () => {
    const app = { name: 'Test App' }; // no credential fields at all
    const result = checkAgentCredentials(app, ['GH_TOKEN'], 'coding');
    expect(result.ok).toBe(true);
  });

  it('handles null agent credentials gracefully', () => {
    const app = { name: 'Test App', required_credentials: ['GH_TOKEN'] };
    const result = checkAgentCredentials(app, null, 'coding');
    expect(result.ok).toBe(false);
    expect(result.missing).toEqual(['GH_TOKEN']);
  });

  it('returns multiple missing credentials', () => {
    const app = { name: 'Test App', required_credentials: ['GH_TOKEN', 'VERCEL_TOKEN', 'SUPABASE_KEY'] };
    const result = checkAgentCredentials(app, ['SUPABASE_KEY'], 'coding');
    expect(result.ok).toBe(false);
    expect(result.missing).toEqual(['GH_TOKEN', 'VERCEL_TOKEN']);
  });
});
