/**
 * Error Classifier — categorizes error text into predefined categories
 * and suggests triage actions for each category.
 */

export const ERROR_CATEGORIES = {
  merge_conflict: {
    label: 'Merge Conflict',
    color: '#E65100',
    icon: '🔀',
    action: 'auto_retry_rebase',
    description: 'PR has conflicts, needs rebase',
    retriable: true,
  },
  ci_failure: {
    label: 'CI Failure',
    color: '#D32F2F',
    icon: '🔴',
    action: 'route_to_coding_agent',
    description: 'Tests failed, needs code fix',
    retriable: false,
  },
  timeout: {
    label: 'Timeout',
    color: '#F57C00',
    icon: '⏱️',
    action: 'auto_retry',
    description: 'Agent session timed out, needs retry',
    retriable: true,
  },
  session_lost: {
    label: 'Session Lost',
    color: '#FF9800',
    icon: '💀',
    action: 'auto_retry',
    description: 'Agent crashed/disconnected, needs retry',
    retriable: true,
  },
  qa_rejection: {
    label: 'QA Rejection',
    color: '#7B1FA2',
    icon: '🧪',
    action: 'route_to_coding_agent',
    description: 'QA found issues, needs code fix',
    retriable: false,
  },
  auth_error: {
    label: 'Auth Error',
    color: '#C62828',
    icon: '🔑',
    action: 'flag_manual',
    description: 'Token/permission issue, needs infra fix',
    retriable: false,
  },
  resource_error: {
    label: 'Resource Error',
    color: '#4E342E',
    icon: '💾',
    action: 'flag_infra',
    description: 'OOM, disk full, needs scaling',
    retriable: false,
  },
  unknown: {
    label: 'Unknown',
    color: '#757575',
    icon: '❓',
    action: 'none',
    description: 'Uncategorized error',
    retriable: false,
  },
};

// Pattern definitions: order matters — first match wins
const PATTERNS = [
  // Merge conflicts
  { category: 'merge_conflict', patterns: [
    /merge conflict/i,
    /conflict.*rebase/i,
    /rebase.*conflict/i,
    /CONFLICT \(content\)/i,
    /mergeable.*false/i,
    /mergestatestatus.*dirty/i,
    /cannot.*merge/i,
    /git.*conflict/i,
  ]},
  // CI failures
  { category: 'ci_failure', patterns: [
    /ci.*fail/i,
    /test.*fail/i,
    /build.*fail/i,
    /check.*fail/i,
    /pipeline.*fail/i,
    /lint.*error/i,
    /compilation.*error/i,
    /npm.*err/i,
    /exit code [1-9]/i,
    /workflow.*fail/i,
    /github.*actions.*fail/i,
  ]},
  // Timeouts
  { category: 'timeout', patterns: [
    /timeout/i,
    /timed?\s*out/i,
    /exceeded.*time/i,
    /deadline.*exceeded/i,
    /idle.*timeout/i,
    /absolute.*timeout/i,
    /ran for \d+ minutes/i,
  ]},
  // Session lost
  { category: 'session_lost', patterns: [
    /session.*lost/i,
    /session.*ended/i,
    /session.*gone/i,
    /session.*died/i,
    /session.*crash/i,
    /agent.*crash/i,
    /agent.*disconnect/i,
    /dispatch.*lost/i,
    /pod.*restart/i,
    /connection.*reset/i,
    /ECONNREFUSED/i,
    /ECONNRESET/i,
  ]},
  // QA rejection
  { category: 'qa_rejection', patterns: [
    /qa.*fail/i,
    /qa.*reject/i,
    /qa.*found.*issue/i,
    /review.*fail/i,
    /code.*review.*reject/i,
    /missing.*migration/i,
    /rubber.?stamp/i,
    /no pr.*found/i,
    /cannot.*perform.*code.*review/i,
  ]},
  // Auth errors
  { category: 'auth_error', patterns: [
    /auth.*fail/i,
    /401/,
    /403/,
    /unauthorized/i,
    /forbidden/i,
    /token.*expir/i,
    /token.*invalid/i,
    /credential.*fail/i,
    /permission.*denied/i,
    /access.*denied/i,
    /auth.*preflight.*fail/i,
  ]},
  // Resource errors
  { category: 'resource_error', patterns: [
    /out of memory/i,
    /oom/i,
    /disk.*full/i,
    /no space.*left/i,
    /ENOMEM/i,
    /ENOSPC/i,
    /resource.*exhaust/i,
    /memory.*limit/i,
    /cpu.*limit/i,
    /evict/i,
  ]},
];

/**
 * Classify an error string into a category.
 * @param {string} errorText - The error message to classify
 * @returns {{ category: string, confidence: 'high'|'medium'|'low', matchedPattern: string }}
 */
export function classifyError(errorText) {
  if (!errorText || typeof errorText !== 'string') {
    return { category: 'unknown', confidence: 'low', matchedPattern: null };
  }

  for (const group of PATTERNS) {
    for (const pattern of group.patterns) {
      const match = errorText.match(pattern);
      if (match) {
        return {
          category: group.category,
          confidence: 'high',
          matchedPattern: match[0],
        };
      }
    }
  }

  return { category: 'unknown', confidence: 'low', matchedPattern: null };
}

/**
 * Get the triage action for a category.
 * @param {string} category
 * @returns {{ action: string, retriable: boolean, description: string }}
 */
export function getTriageAction(category) {
  const cat = ERROR_CATEGORIES[category] || ERROR_CATEGORIES.unknown;
  return {
    action: cat.action,
    retriable: cat.retriable,
    description: cat.description,
  };
}

/**
 * Get category metadata for display purposes.
 * @param {string} category
 * @returns {object}
 */
export function getCategoryMeta(category) {
  return ERROR_CATEGORIES[category] || ERROR_CATEGORIES.unknown;
}
