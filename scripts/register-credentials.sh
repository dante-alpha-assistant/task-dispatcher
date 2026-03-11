#!/usr/bin/env bash
# register-credentials.sh — Agent credential self-registration
#
# On startup, checks which known env vars are present (non-empty)
# and PATCHes agent_cards.available_credentials in Supabase.
#
# SECURITY: Only checks for EXISTENCE of env vars. Never logs or
# transmits actual values.
#
# Usage:
#   AGENT_NAME=ifra-worker \
#   SUPABASE_URL=https://xxx.supabase.co \
#   SUPABASE_SERVICE_ROLE_KEY=eyJ... \
#   ./register-credentials.sh
#
# Or pass agent name as argument:
#   ./register-credentials.sh ifra-worker

set -euo pipefail

# --- Configuration ---
# Known credential env vars to check
KNOWN_CREDENTIALS=(
  "GH_TOKEN"
  "SUPABASE_SERVICE_ROLE_KEY"
  "SUPABASE_MGMT_TOKEN"
  "VERCEL_TOKEN"
  "KUBECONFIG"
  "ANTHROPIC_API_KEY"
  "OPENROUTER_API_KEY"
  "DOCKER_TOKEN"
  "NPM_TOKEN"
  "AWS_ACCESS_KEY_ID"
)

# --- Resolve agent name ---
AGENT_NAME="${AGENT_NAME:-${1:-}}"
if [ -z "$AGENT_NAME" ]; then
  echo "[CRED-REG] ERROR: AGENT_NAME not set and no argument provided" >&2
  exit 1
fi

# --- Resolve Supabase connection ---
SUPABASE_URL="${SUPABASE_URL:-}"
SUPABASE_KEY="${SUPABASE_SERVICE_ROLE_KEY:-}"

if [ -z "$SUPABASE_URL" ] || [ -z "$SUPABASE_KEY" ]; then
  echo "[CRED-REG] ERROR: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set" >&2
  exit 1
fi

# --- Check which credentials are present ---
FOUND=()
for VAR_NAME in "${KNOWN_CREDENTIALS[@]}"; do
  VAL="${!VAR_NAME:-}"
  if [ -n "$VAL" ]; then
    FOUND+=("$VAR_NAME")
  fi
done

echo "[CRED-REG] Agent: $AGENT_NAME"
echo "[CRED-REG] Found credentials: ${FOUND[*]:-none}"

# --- Build JSON array ---
if [ ${#FOUND[@]} -eq 0 ]; then
  JSON_ARRAY="[]"
else
  JSON_ARRAY="["
  for i in "${!FOUND[@]}"; do
    [ "$i" -gt 0 ] && JSON_ARRAY+=","
    JSON_ARRAY+="\"${FOUND[$i]}\""
  done
  JSON_ARRAY+="]"
fi

# --- PATCH agent_cards ---
RESPONSE=$(curl -s -w "\n%{http_code}" -X PATCH \
  "${SUPABASE_URL}/rest/v1/agent_cards?id=eq.${AGENT_NAME}" \
  -H "apikey: ${SUPABASE_KEY}" \
  -H "Authorization: Bearer ${SUPABASE_KEY}" \
  -H "Content-Type: application/json" \
  -H "Prefer: return=minimal" \
  -d "{\"available_credentials\": ${JSON_ARRAY}}")

HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -n -1)

if [ "$HTTP_CODE" -ge 200 ] && [ "$HTTP_CODE" -lt 300 ]; then
  echo "[CRED-REG] SUCCESS: Updated agent_cards for $AGENT_NAME (HTTP $HTTP_CODE)"
  echo "[CRED-REG] Registered ${#FOUND[@]} credential(s): ${FOUND[*]:-none}"
else
  echo "[CRED-REG] FAILED: HTTP $HTTP_CODE — $BODY" >&2
  exit 1
fi
