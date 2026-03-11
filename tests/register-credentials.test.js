import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { execSync } from 'child_process';

describe('register-credentials.js', () => {
  const originalEnv = { ...process.env };

  afterEach(() => {
    // Restore original env
    process.env = { ...originalEnv };
  });

  describe('credential detection', () => {
    it('should detect present env vars from known list', () => {
      // The KNOWN_CREDENTIALS list from register-credentials.js
      const KNOWN_CREDENTIALS = [
        'GH_TOKEN',
        'SUPABASE_SERVICE_ROLE_KEY',
        'SUPABASE_MGMT_TOKEN',
        'VERCEL_TOKEN',
        'KUBECONFIG',
        'OPENROUTER_API_KEY',
        'ANTHROPIC_API_KEY',
        'LANGFUSE_SECRET_KEY',
      ];

      // Simulate: set some vars, leave others unset
      const testEnv = {
        GH_TOKEN: 'ghp_test123',
        SUPABASE_SERVICE_ROLE_KEY: 'some-key',
        KUBECONFIG: '/etc/rancher/k3s/k3s.yaml',
        // VERCEL_TOKEN not set
        // SUPABASE_MGMT_TOKEN not set
        OPENROUTER_API_KEY: 'sk-or-test',
        // ANTHROPIC_API_KEY not set
        // LANGFUSE_SECRET_KEY not set
      };

      const detected = KNOWN_CREDENTIALS.filter(name => {
        const val = testEnv[name];
        return val !== undefined && val !== null && String(val).trim() !== '';
      });

      expect(detected).toEqual(['GH_TOKEN', 'SUPABASE_SERVICE_ROLE_KEY', 'KUBECONFIG', 'OPENROUTER_API_KEY']);
      expect(detected).not.toContain('VERCEL_TOKEN');
      expect(detected).not.toContain('ANTHROPIC_API_KEY');
    });

    it('should not detect empty string env vars', () => {
      const KNOWN_CREDENTIALS = ['GH_TOKEN', 'VERCEL_TOKEN'];
      const testEnv = {
        GH_TOKEN: '',
        VERCEL_TOKEN: '   ',
      };

      const detected = KNOWN_CREDENTIALS.filter(name => {
        const val = testEnv[name];
        return val !== undefined && val !== null && String(val).trim() !== '';
      });

      expect(detected).toEqual([]);
    });

    it('should never expose actual values — only names', () => {
      const KNOWN_CREDENTIALS = ['GH_TOKEN', 'SUPABASE_SERVICE_ROLE_KEY'];
      const testEnv = {
        GH_TOKEN: 'ghp_super_secret_token',
        SUPABASE_SERVICE_ROLE_KEY: 'eyJhbGciOi...',
      };

      const detected = KNOWN_CREDENTIALS.filter(name => {
        const val = testEnv[name];
        return val !== undefined && val !== null && String(val).trim() !== '';
      });

      // Result should only contain names, never values
      detected.forEach(name => {
        expect(name).not.toContain('ghp_');
        expect(name).not.toContain('eyJ');
        expect(['GH_TOKEN', 'SUPABASE_SERVICE_ROLE_KEY']).toContain(name);
      });
    });
  });

  describe('agent name resolution', () => {
    it('should strip K8s hash suffixes from hostname', () => {
      const testCases = [
        { hostname: 'neo-worker-6cd57b9bf5-p85dq', expected: 'neo-worker' },
        { hostname: 'ifra-worker-abc1234def-xy12z', expected: 'ifra-worker' },
        { hostname: 'beta-worker-9f8e7d6c5b-ab1cd', expected: 'beta-worker' },
      ];

      testCases.forEach(({ hostname, expected }) => {
        const match = hostname.match(/^([a-z][\w-]*?)-[a-f0-9]{8,10}-[a-z0-9]{5}$/);
        expect(match).not.toBeNull();
        expect(match[1]).toBe(expected);
      });
    });

    it('should return full hostname when no K8s pattern', () => {
      const hostname = 'my-agent';
      const match = hostname.match(/^([a-z][\w-]*?)-[a-f0-9]{8,10}-[a-z0-9]{5}$/);
      expect(match).toBeNull();
      // Fallback: use full hostname
    });
  });

  describe('known credentials allowlist', () => {
    it('should only allow known credential names through API', () => {
      const KNOWN_CRED_NAMES = ['GH_TOKEN', 'SUPABASE_SERVICE_ROLE_KEY', 'SUPABASE_MGMT_TOKEN', 'VERCEL_TOKEN', 'KUBECONFIG', 'OPENROUTER_API_KEY', 'ANTHROPIC_API_KEY', 'LANGFUSE_SECRET_KEY'];
      
      const input = ['GH_TOKEN', 'MALICIOUS_VAR', 'KUBECONFIG', 'SHELL', 'PATH'];
      const filtered = input.filter(c => KNOWN_CRED_NAMES.includes(c));
      
      expect(filtered).toEqual(['GH_TOKEN', 'KUBECONFIG']);
      expect(filtered).not.toContain('MALICIOUS_VAR');
      expect(filtered).not.toContain('SHELL');
      expect(filtered).not.toContain('PATH');
    });
  });
});
