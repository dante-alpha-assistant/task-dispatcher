/**
 * Error Classifier — Categorizes task errors and suggests triage actions.
 * 
 * Categories:
 *   merge_conflict, ci_failure, timeout, session_lost,
 *   qa_rejection, auth_error, resource_error, unknown
 */

const ERROR_PATTERNS = [
  {
    category: 'merge_conflict',
    patterns: [
      /merge conflict/i,
      /conflict.*rebase/i,
      /rebase.*conflict/i,
      /CONFLICT.*Merge/i,
      /cannot.*merge/i,
      /not.*mergeable/i,
      /mergeable_state.*dirty/i,
      /needs.*rebase/i,
    ],
    action: 'auto_rebase',
    label: 'Merge Conflict',
    color: '#E65100',
    retriable: true,
  },
  {
    category: 'ci_failure',
    patterns: [
      /ci.*fail/i,
      /test.*fail/i,
      /build.*fail/i,
      /check.*fail/i,
      /pipeline.*fail/i,
      /linting.*error/i,
      /compilation.*error/i,
      /npm.*run.*build.*fail/i,
      /exit code [1-9]/i,
      /jest.*fail/i,
      /vitest.*fail/i,
    ],
    action: 'route_to_coder',
    label: 'CI Failure',
    color: '#D32F2F',
    retriable: false,
  },
  {
    category: 'resource_error',
    patterns: [
      /OOM/i,
      /out of memory/i,
      /disk.*full/i,
      /no space/i,
      /ENOSPC/i,
      /resource.*quota/i,
      /evicted/i,
      /insufficient.*resource/i,
      /memory.*limit/i,
    ],
    action: 'flag_infra',
    label: 'Resource Error',
    color: '#AD1457',
    retriable: false,
  },
  {
    category: 'timeout',
    patterns: [
      /timed?\s*out/i,
      /timeout/i,
      /exceeded.*time/i,
      /deadline.*exceeded/i,
      /took too long/i,
      /absolute timeout/i,
    ],
    action: 'auto_retry',
    label: 'Timeout',
    color: '#FF6F00',
    retriable: true,
  },
  {
    category: 'session_lost',
    patterns: [
      /session.*lost/i,
      /session.*closed/i,
      /session.*expired/i,
      /agent.*crash/i,
      /agent.*disconnect/i,
      /connection.*reset/i,
      /ECONNRESET/i,
      /ECONNREFUSED/i,
      /socket hang up/i,
      /dispatch.*lost/i,
      /pod.*restart/i,
    ],
    action: 'auto_retry',
    label: 'Session Lost',
    color: '#F57C00',
    retriable: true,
  },
  {
    category: 'qa_rejection',
    patterns: [
      /qa.*fail/i,
      /qa.*reject/i,
      /qa.*found.*issue/i,
      /review.*fail/i,
      /code.*review.*issue/i,
    ],
    action: 'route_to_coder',
    label: 'QA Rejection',
    color: '#7B1FA2',
    retriable: false,
  },
  {
    category: 'auth_error',
    patterns: [
      /401/,
      /403/,
      /unauthorized/i,
      /forbidden/i,
      /invalid.*token/i,
      /token.*expired/i,
      /auth.*fail/i,
      /permission.*denied/i,
      /not.*authorized/i,
      /GH_TOKEN/i,
      /RBAC/i,
    ],
    action: 'flag_manual',
    label: 'Auth Error',
    color: '#C62828',
    retriable: false,
  },
];

/**
 * Classify an error string into a category.
 * @param {string} errorText - The error message to classify
 * @returns {{ category: string, action: string, label: string, color: string, retriable: boolean }}
 */
export function classifyError(errorText) {
  if (!errorText || typeof errorText !== 'string') {
    return { category: 'unknown', action: 'flag_manual', label: 'Unknown', color: '#9E9E9E', retriable: false };
  }

  for (const rule of ERROR_PATTERNS) {
    for (const pattern of rule.patterns) {
      if (pattern.test(errorText)) {
        return {
          category: rule.category,
          action: rule.action,
          label: rule.label,
          color: rule.color,
          retriable: rule.retriable,
        };
      }
    }
  }

  return { category: 'unknown', action: 'flag_manual', label: 'Unknown', color: '#9E9E9E', retriable: false };
}

/**
 * Get all error categories with metadata (for stats/UI).
 */
export function getErrorCategories() {
  return [
    ...ERROR_PATTERNS.map(p => ({
      category: p.category,
      label: p.label,
      color: p.color,
      action: p.action,
      retriable: p.retriable,
    })),
    { category: 'unknown', label: 'Unknown', color: '#9E9E9E', action: 'flag_manual', retriable: false },
  ];
}
