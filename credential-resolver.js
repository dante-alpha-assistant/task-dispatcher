/**
 * App-level credential resolution service
 * Resolves credentials based on app_id and task_type
 */

import { createClient } from '@supabase/supabase-js';

const supabase = createClient(
  process.env.SUPABASE_URL || 'https://lessxkxujvcmublgwdaa.supabase.co',
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

/**
 * Resolve credentials for a given app and task type
 * @param {string} appId - The app UUID
 * @param {string} taskType - The task type (coding, deploy, qa, etc.)
 * @returns {Promise<{ok: boolean, credentials: object, missing: string[], k8sSecrets: object}>}
 */
export async function resolveAppCredentials(appId, taskType) {
  try {
    // Fetch app and its credential configuration
    const { data: app, error } = await supabase
      .from('apps')
      .select('id, name, credentials, required_credentials')
      .eq('id', appId)
      .single();

    if (error || !app) {
      return {
        ok: false,
        error: `App ${appId} not found`,
        credentials: {},
        missing: [],
        k8sSecrets: {}
      };
    }

    const appCredentials = app.credentials || {};
    const requiredCreds = app.required_credentials || {};
    const requiredForTask = requiredCreds[taskType] || [];

    console.log(`[CREDENTIAL-RESOLVER] App: ${app.name}, Task: ${taskType}, Required: ${requiredForTask.join(', ')}`);

    const resolvedCredentials = {};
    const k8sSecretRefs = {};
    const missing = [];

    // Resolve each required credential
    for (const credName of requiredForTask) {
      const credConfig = appCredentials[credName];
      
      if (!credConfig) {
        missing.push(credName);
        console.warn(`[CREDENTIAL-RESOLVER] Missing credential config for ${credName}`);
        continue;
      }

      // Build K8s secret reference
      if (credConfig.k8s_secret && credConfig.k8s_key) {
        k8sSecretRefs[credName] = {
          secretName: credConfig.k8s_secret,
          secretKey: credConfig.k8s_key
        };
        resolvedCredentials[credName] = credConfig;
        console.log(`[CREDENTIAL-RESOLVER] Resolved ${credName} -> ${credConfig.k8s_secret}/${credConfig.k8s_key}`);
      } else {
        missing.push(credName);
        console.warn(`[CREDENTIAL-RESOLVER] Invalid credential config for ${credName}: missing k8s_secret or k8s_key`);
      }
    }

    return {
      ok: missing.length === 0,
      app: app.name,
      credentials: resolvedCredentials,
      k8sSecrets: k8sSecretRefs,
      missing,
      error: missing.length > 0 ? `Missing credentials: ${missing.join(', ')}` : null
    };

  } catch (error) {
    console.error(`[CREDENTIAL-RESOLVER] Error resolving credentials for app ${appId}:`, error);
    return {
      ok: false,
      error: error.message,
      credentials: {},
      missing: [],
      k8sSecrets: {}
    };
  }
}

/**
 * Check if an agent has the necessary credential access for an app/task combination
 * @param {string} appId - The app UUID  
 * @param {string} taskType - The task type
 * @param {string[]} agentCredentials - Agent's available credentials (for legacy compatibility)
 * @returns {Promise<{ok: boolean, missing: string[]}>}
 */
export async function checkAgentCredentials(appId, taskType, agentCredentials = []) {
  const resolution = await resolveAppCredentials(appId, taskType);
  
  if (!resolution.ok) {
    return { ok: false, missing: resolution.missing };
  }

  // In app-level credential system, all agents can access app credentials
  // The credential resolution happens at dispatch time, not at agent level
  return { ok: true, missing: [] };
}

/**
 * Format credentials for agent environment injection
 * @param {object} k8sSecrets - K8s secret references from resolveAppCredentials  
 * @returns {object} Environment variable configuration
 */
export function formatCredentialsForAgent(k8sSecrets) {
  const envConfig = {};
  
  for (const [credName, secretRef] of Object.entries(k8sSecrets)) {
    envConfig[credName] = {
      valueFrom: {
        secretKeyRef: {
          name: secretRef.secretName,
          key: secretRef.secretKey
        }
      }
    };
  }
  
  return envConfig;
}

/**
 * Fetch actual credential values from K8s secrets (for direct injection)
 * @param {object} k8sSecrets - K8s secret references from resolveAppCredentials
 * @returns {Promise<object>} Object with credential names and actual values
 */
export async function fetchCredentialValues(k8sSecrets) {
  const { execSync } = await import('child_process');
  const credentials = {};

  for (const [credName, secretRef] of Object.entries(k8sSecrets)) {
    try {
      console.log(`[CREDENTIAL-RESOLVER] Fetching ${credName} from ${secretRef.secretName}/${secretRef.secretKey}`);
      
      // Fetch the actual secret value using kubectl
      const secretValue = execSync(
        `kubectl get secret ${secretRef.secretName} -n agents -o jsonpath='{.data.${secretRef.secretKey}}' | base64 -d`,
        { encoding: 'utf8', timeout: 10000 }
      ).trim();

      if (secretValue) {
        credentials[credName] = secretValue;
        console.log(`[CREDENTIAL-RESOLVER] ✅ Fetched ${credName} (${secretValue.length} chars)`);
      } else {
        console.warn(`[CREDENTIAL-RESOLVER] ⚠️ Empty value for ${credName}`);
      }
    } catch (error) {
      console.error(`[CREDENTIAL-RESOLVER] ❌ Failed to fetch ${credName}: ${error.message}`);
    }
  }

  return credentials;
}

/**
 * Legacy credential check function - maintained for backward compatibility
 * @deprecated Use resolveAppCredentials instead
 */
export function checkAgentCredentialsLegacy(appContext, agentCredentials, credType) {
  console.warn('[CREDENTIAL-RESOLVER] Using legacy credential check - consider migrating to app-level credentials');
  
  if (!appContext) return { ok: true, missing: [] };
  
  const required = appContext.required_credentials?.[credType] || [];
  const missing = required.filter(cred => !agentCredentials.includes(cred));
  
  return {
    ok: missing.length === 0,
    missing
  };
}