import { describe, it, expect } from 'vitest';

// Inline the checkAgentCredentials function for testing (same logic as in index.js)
function checkAgentCredentials(agentCard, appContext, { isQa = false } = {}) {
  if (!appContext) return { ok: true, missing: [] };
  const required = isQa
    ? (appContext.required_qa_credentials || [])
    : (appContext.required_credentials || []);
  if (!required.length) return { ok: true, missing: [] };
  const available = new Set(agentCard.available_credentials || []);
  const missing = required.filter(cred => !available.has(cred));
  return { ok: missing.length === 0, missing };
}

describe('checkAgentCredentials', () => {
  const appWithCreds = {
    name: 'Queue Dashboard',
    required_credentials: ['GH_TOKEN', 'SUPABASE_KEY'],
    required_qa_credentials: ['GH_TOKEN'],
  };

  const appNoCreds = {
    name: 'GitOps',
    required_credentials: [],
    required_qa_credentials: [],
  };

  it('returns ok when agent has all required credentials', () => {
    const agent = { available_credentials: ['GH_TOKEN', 'SUPABASE_KEY', 'EXTRA'] };
    const result = checkAgentCredentials(agent, appWithCreds);
    expect(result.ok).toBe(true);
    expect(result.missing).toEqual([]);
  });

  it('returns missing credentials when agent lacks some', () => {
    const agent = { available_credentials: ['GH_TOKEN'] };
    const result = checkAgentCredentials(agent, appWithCreds);
    expect(result.ok).toBe(false);
    expect(result.missing).toEqual(['SUPABASE_KEY']);
  });

  it('returns all missing when agent has no credentials', () => {
    const agent = { available_credentials: [] };
    const result = checkAgentCredentials(agent, appWithCreds);
    expect(result.ok).toBe(false);
    expect(result.missing).toEqual(['GH_TOKEN', 'SUPABASE_KEY']);
  });

  it('returns ok when app has no required credentials', () => {
    const agent = { available_credentials: [] };
    const result = checkAgentCredentials(agent, appNoCreds);
    expect(result.ok).toBe(true);
    expect(result.missing).toEqual([]);
  });

  it('returns ok when appContext is null', () => {
    const agent = { available_credentials: [] };
    const result = checkAgentCredentials(agent, null);
    expect(result.ok).toBe(true);
    expect(result.missing).toEqual([]);
  });

  it('uses required_qa_credentials for QA dispatch', () => {
    const agent = { available_credentials: ['GH_TOKEN'] };
    // QA only needs GH_TOKEN, not SUPABASE_KEY
    const result = checkAgentCredentials(agent, appWithCreds, { isQa: true });
    expect(result.ok).toBe(true);
    expect(result.missing).toEqual([]);
  });

  it('fails QA dispatch when QA credentials missing', () => {
    const agent = { available_credentials: [] };
    const result = checkAgentCredentials(agent, appWithCreds, { isQa: true });
    expect(result.ok).toBe(false);
    expect(result.missing).toEqual(['GH_TOKEN']);
  });

  it('handles undefined available_credentials on agent', () => {
    const agent = {};
    const result = checkAgentCredentials(agent, appWithCreds);
    expect(result.ok).toBe(false);
    expect(result.missing).toEqual(['GH_TOKEN', 'SUPABASE_KEY']);
  });

  it('handles undefined credential arrays on app', () => {
    const agent = { available_credentials: ['GH_TOKEN'] };
    const app = { name: 'TestApp' }; // no credential arrays
    const result = checkAgentCredentials(agent, app);
    expect(result.ok).toBe(true);
    expect(result.missing).toEqual([]);
  });
});
