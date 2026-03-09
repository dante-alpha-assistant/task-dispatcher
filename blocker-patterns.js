// blocker-patterns.js — Auto-detect common blocker patterns from agent error messages
// Converts failed tasks to blocked with structured metadata when patterns match.
// Pattern list is configurable (not hardcoded regex).

/**
 * @typedef {Object} BlockerPattern
 * @property {string} type - Blocker type identifier
 * @property {string} title - Human-readable title
 * @property {string[]} keywords - Case-insensitive keywords to match
 * @property {RegExp[]} [regexes] - Optional regex patterns for more specific matching
 * @property {Object[]} required_inputs - Suggested inputs for resolution
 * @property {string} suggested_action - What the human should do
 */

/** @type {BlockerPattern[]} */
const DEFAULT_PATTERNS = [
  {
    type: "missing_credential",
    title: "Missing or invalid credentials",
    keywords: [
      "not configured", "missing key", "api key", "api_key", "missing token",
      "empty token", "token is empty", "no token", "invalid token",
      "credential", "secret not set", "env var", "environment variable",
    ],
    regexes: [
      /\b(?:API|AUTH|SECRET|TOKEN|KEY|PASSWORD)\w*\s+(?:is\s+)?(?:not\s+(?:set|configured|defined|found)|missing|empty|undefined)\b/i,
      /\b(?:missing|empty|undefined|unset)\s+(?:env(?:ironment)?\s*(?:var(?:iable)?)?|secret|credential|key|token)\b/i,
    ],
    required_inputs: [
      { key: "CREDENTIAL_NAME", label: "Which credential/env var is missing?", type: "text", placeholder: "e.g. GH_TOKEN" },
      { key: "CREDENTIAL_VALUE", label: "Credential value", type: "password", placeholder: "paste value" },
    ],
    suggested_action: "Set the missing environment variable or secret in the agent's deployment config.",
  },
  {
    type: "permission_denied",
    title: "Permission or authorization error",
    keywords: [
      "unauthorized", "forbidden", "permission denied", "access denied",
      "not authorized", "insufficient permissions", "rbac", "403", "401",
    ],
    regexes: [
      /\bHTTP\s+(?:401|403)\b/i,
      /\b(?:status|code|error)\s*[:=]\s*(?:401|403)\b/i,
      /\bforbidden\b/i,
      /\bunauthorized\b/i,
    ],
    required_inputs: [
      { key: "RESOURCE", label: "What resource was denied?", type: "text", placeholder: "e.g. GitHub repo, K8s namespace" },
      { key: "ACTION_NEEDED", label: "What access is needed?", type: "text", placeholder: "e.g. write access to repo" },
    ],
    suggested_action: "Grant the agent appropriate permissions or update RBAC rules.",
  },
  {
    type: "infrastructure",
    title: "Infrastructure or service unavailable",
    keywords: [
      "econnrefused", "econnreset", "etimedout", "enotfound",
      "connection refused", "connection reset", "unreachable",
      "service unavailable", "bad gateway", "502", "503", "504",
      "dns resolution", "network error",
    ],
    regexes: [
      /\bHTTP\s+(?:502|503|504)\b/i,
      /\b(?:status|code|error)\s*[:=]\s*(?:502|503|504)\b/i,
      /\bECONN(?:REFUSED|RESET|ABORTED)\b/i,
      /\bETIMEDOUT\b/i,
      /\bENOTFOUND\b/i,
    ],
    required_inputs: [
      { key: "SERVICE", label: "Which service is down?", type: "text", placeholder: "e.g. Supabase, ArgoCD" },
    ],
    suggested_action: "Check infrastructure health, restart the affected service, or wait for it to recover.",
  },
  {
    type: "ambiguous_requirement",
    title: "Ambiguous or unclear task requirements",
    keywords: [
      "unclear", "need clarification", "which approach", "ambiguous",
      "not sure what", "conflicting requirements", "need more details",
      "please clarify", "what exactly", "undefined scope", "vague",
    ],
    regexes: [
      /\bneed(?:s)?\s+(?:more\s+)?clarification\b/i,
      /\bunclear\s+(?:what|how|which|whether)\b/i,
      /\bambiguous\s+(?:requirement|spec|task|description)\b/i,
    ],
    required_inputs: [
      { key: "CLARIFICATION", label: "Provide clarification", type: "text", placeholder: "Describe the intended behavior" },
    ],
    suggested_action: "Clarify the task requirements and update the description with more specific details.",
  },
  {
    type: "resource_limit",
    title: "Resource limit exceeded",
    keywords: [
      "out of memory", "oom", "oomkilled", "disk full", "no space left",
      "quota exceeded", "rate limit", "rate_limit", "too many requests",
      "429", "memory limit", "cpu limit", "storage full",
    ],
    regexes: [
      /\bOOM(?:Killed)?\b/i,
      /\bno\s+space\s+left\s+on\s+device\b/i,
      /\bquota\s+exceeded\b/i,
      /\b(?:rate|request)\s*limit\b/i,
      /\bHTTP\s+429\b/i,
      /\b(?:status|code|error)\s*[:=]\s*429\b/i,
    ],
    required_inputs: [
      { key: "RESOURCE_TYPE", label: "Which resource is exhausted?", type: "select", placeholder: "memory|disk|api_quota|cpu" },
    ],
    suggested_action: "Increase resource limits, clean up disk space, or wait for rate limits to reset.",
  },
  {
    type: "dependency_missing",
    title: "Missing dependency or tool",
    keywords: [
      "command not found", "module not found", "cannot find module",
      "no such file", "not installed", "package not found",
      "binary not found", "executable not found",
    ],
    regexes: [
      /\bcommand\s+not\s+found\b/i,
      /\bcannot\s+find\s+module\b/i,
      /\bno\s+such\s+file\s+or\s+directory\b/i,
      /\bENOENT\b/,
    ],
    required_inputs: [
      { key: "DEPENDENCY", label: "What is missing?", type: "text", placeholder: "e.g. kubectl, npm package name" },
    ],
    suggested_action: "Install the missing dependency or tool in the agent's environment.",
  },
];

/**
 * Match error text against blocker patterns.
 * Returns the first matching pattern with extracted details, or null.
 *
 * @param {string} errorText - The error message to analyze
 * @param {BlockerPattern[]} [patterns] - Custom patterns (defaults to DEFAULT_PATTERNS)
 * @returns {{ pattern: BlockerPattern, details: string, matchedKeywords: string[] } | null}
 */
export function detectBlockerPattern(errorText, patterns = DEFAULT_PATTERNS) {
  if (!errorText || typeof errorText !== "string") return null;

  const lowerError = errorText.toLowerCase();

  for (const pattern of patterns) {
    // Check keywords first (fast path)
    const matchedKeywords = pattern.keywords.filter((kw) =>
      lowerError.includes(kw.toLowerCase())
    );

    // Check regex patterns
    let regexMatched = false;
    let regexMatch = null;
    if (pattern.regexes) {
      for (const rx of pattern.regexes) {
        const m = errorText.match(rx);
        if (m) {
          regexMatched = true;
          regexMatch = m[0];
          break;
        }
      }
    }

    // Need at least one keyword OR one regex match
    if (matchedKeywords.length === 0 && !regexMatched) continue;

    // Extract a short detail snippet around the match
    const matchTerm = regexMatch || matchedKeywords[0];
    const idx = lowerError.indexOf(matchTerm.toLowerCase());
    const start = Math.max(0, idx - 50);
    const end = Math.min(errorText.length, idx + matchTerm.length + 100);
    const details = (start > 0 ? "…" : "") + errorText.slice(start, end).trim() + (end < errorText.length ? "…" : "");

    return {
      pattern,
      details,
      matchedKeywords,
    };
  }

  return null;
}

/**
 * Build structured blocker metadata from a detected pattern.
 *
 * @param {{ pattern: BlockerPattern, details: string, matchedKeywords: string[] }} detection
 * @returns {Object} Blocker metadata suitable for task update
 */
export function buildBlockerMetadata(detection) {
  const { pattern, details, matchedKeywords } = detection;
  return {
    type: pattern.type,
    title: pattern.title,
    description: `Auto-detected from error: ${details}`,
    matched_keywords: matchedKeywords,
    required_inputs: pattern.required_inputs,
    suggested_action: pattern.suggested_action,
    auto_detected: true,
    detected_at: new Date().toISOString(),
  };
}

/**
 * Get the default patterns (for testing/introspection).
 * @returns {BlockerPattern[]}
 */
export function getDefaultPatterns() {
  return DEFAULT_PATTERNS;
}
