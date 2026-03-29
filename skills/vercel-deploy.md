# vercel-deploy — Vercel Deployment Skill

Deploy a project to Vercel via the Vercel REST API. Works for any GitHub-connected repo.

## Prerequisites

- `VERCEL_TOKEN` environment variable must be set
- GitHub repo must be under the `dante-alpha-assistant` organization
- Vercel account: `lautaro450` (GitHub integration connected)

## Quick Deploy (Script)

For simple deployments, use the helper script:

```bash
# Usage: deploy.sh <github-org> <repo-name> [branch]
bash /path/to/skills/vercel-deploy/deploy.sh dante-alpha-assistant my-repo main
```

The script handles all steps below automatically and outputs the production URL.

## Manual Steps

### Step 1: Verify VERCEL_TOKEN

```bash
if [ -z "$VERCEL_TOKEN" ]; then
  echo "ERROR: VERCEL_TOKEN is not set. Cannot deploy to Vercel."
  echo "Set VERCEL_TOKEN env var with a valid Vercel API token."
  exit 1
fi
```

If `VERCEL_TOKEN` is missing, **fail immediately**. Do NOT proceed. Set task status to `blocked` with blocker type `missing_credential`.

### Step 2: Check if Vercel Project Exists

```bash
REPO_NAME="<repo-name>"
RESPONSE=$(curl -s "https://api.vercel.com/v9/projects?search=${REPO_NAME}" \
  -H "Authorization: Bearer ${VERCEL_TOKEN}")

PROJECT_ID=$(echo "$RESPONSE" | jq -r ".projects[] | select(.name == \"${REPO_NAME}\") | .id")

if [ -n "$PROJECT_ID" ]; then
  echo "Found existing Vercel project: ${PROJECT_ID}"
else
  echo "No Vercel project found for ${REPO_NAME}. Will create one."
fi
```

### Step 3: Create Vercel Project (if needed)

Only run this if Step 2 found no existing project.

```bash
GITHUB_ORG="dante-alpha-assistant"

PROJECT_RESPONSE=$(curl -s -X POST "https://api.vercel.com/v10/projects" \
  -H "Authorization: Bearer ${VERCEL_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "'${REPO_NAME}'",
    "framework": null,
    "gitRepository": {
      "type": "github",
      "repo": "'${GITHUB_ORG}/${REPO_NAME}'"
    }
  }')

PROJECT_ID=$(echo "$PROJECT_RESPONSE" | jq -r '.id')

if [ "$PROJECT_ID" = "null" ] || [ -z "$PROJECT_ID" ]; then
  echo "ERROR: Failed to create Vercel project"
  echo "$PROJECT_RESPONSE" | jq .
  exit 1
fi

echo "Created Vercel project: ${PROJECT_ID}"
```

**Note on framework:** Vercel auto-detects the framework. Set `"framework": null` unless you need to override (e.g., `"nextjs"`, `"vite"`, `"create-react-app"`).

### Step 4: Trigger Production Deployment

```bash
BRANCH="${DEPLOY_BRANCH:-main}"

DEPLOY_RESPONSE=$(curl -s -X POST "https://api.vercel.com/v13/deployments" \
  -H "Authorization: Bearer ${VERCEL_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "'${REPO_NAME}'",
    "project": "'${PROJECT_ID}'",
    "gitSource": {
      "type": "github",
      "org": "'${GITHUB_ORG}'",
      "repo": "'${REPO_NAME}'",
      "ref": "'${BRANCH}'"
    },
    "target": "production"
  }')

DEPLOYMENT_ID=$(echo "$DEPLOY_RESPONSE" | jq -r '.id')
DEPLOYMENT_URL=$(echo "$DEPLOY_RESPONSE" | jq -r '.url')

if [ "$DEPLOYMENT_ID" = "null" ] || [ -z "$DEPLOYMENT_ID" ]; then
  echo "ERROR: Failed to trigger deployment"
  echo "$DEPLOY_RESPONSE" | jq .
  exit 1
fi

echo "Deployment triggered: ${DEPLOYMENT_ID}"
echo "Preview URL: https://${DEPLOYMENT_URL}"
```

### Step 5: Poll Deployment Status

Poll every 10 seconds for up to 10 minutes.

```bash
MAX_ATTEMPTS=60
ATTEMPT=0

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
  STATUS_RESPONSE=$(curl -s "https://api.vercel.com/v13/deployments/${DEPLOYMENT_ID}" \
    -H "Authorization: Bearer ${VERCEL_TOKEN}")

  STATE=$(echo "$STATUS_RESPONSE" | jq -r '.readyState')
  echo "Deployment state: ${STATE} (attempt $((ATTEMPT+1))/${MAX_ATTEMPTS})"

  case "$STATE" in
    "READY")
      PRODUCTION_URL=$(echo "$STATUS_RESPONSE" | jq -r '.alias[0] // .url')
      echo "✅ Deployment successful!"
      echo "Production URL: https://${PRODUCTION_URL}"
      break
      ;;
    "ERROR"|"CANCELED")
      echo "❌ Deployment failed with state: ${STATE}"
      # Get error details
      echo "$STATUS_RESPONSE" | jq '{readyState, errorCode, errorMessage}'
      exit 1
      ;;
    *)
      # QUEUED, BUILDING, INITIALIZING — keep waiting
      sleep 10
      ATTEMPT=$((ATTEMPT+1))
      ;;
  esac
done

if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
  echo "ERROR: Deployment timed out after 10 minutes"
  exit 1
fi
```

### Step 6: Return Production URL

After a successful deployment, the production URL is available as `https://${PRODUCTION_URL}`.

For custom domains, check:
```bash
curl -s "https://api.vercel.com/v9/projects/${PROJECT_ID}/domains" \
  -H "Authorization: Bearer ${VERCEL_TOKEN}" | jq '.domains[].name'
```

### Step 7: Update Task with Deployment URL

If running as part of a dispatched task, update the task with the deployment URL:

```bash
TASK_ID="<task-id>"
SUPABASE_KEY="<service-role-key>"

curl -s -X PATCH "https://lessxkxujvcmublgwdaa.supabase.co/rest/v1/agent_tasks?id=eq.${TASK_ID}" \
  -H "apikey: ${SUPABASE_KEY}" \
  -H "Authorization: Bearer ${SUPABASE_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"metadata":{"deployment_url":"https://'${PRODUCTION_URL}'"}}'
```

## Environment Variables for Vercel Projects

To set env vars on the Vercel project (e.g., API keys for the app):

```bash
curl -s -X POST "https://api.vercel.com/v10/projects/${PROJECT_ID}/env" \
  -H "Authorization: Bearer ${VERCEL_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "encrypted",
    "key": "ENV_VAR_NAME",
    "value": "env_var_value",
    "target": ["production", "preview"]
  }'
```

## Vercel API Reference

- **Base URL:** `https://api.vercel.com`
- **Auth:** `Authorization: Bearer ${VERCEL_TOKEN}`
- **Account:** `lautaro450`
- **GitHub org:** `dante-alpha-assistant`

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/v9/projects?search=` | GET | Search for existing project |
| `/v10/projects` | POST | Create new project |
| `/v9/projects/{id}/domains` | GET | List project domains |
| `/v10/projects/{id}/env` | POST | Set environment variables |
| `/v13/deployments` | POST | Trigger deployment |
| `/v13/deployments/{id}` | GET | Check deployment status |

## Error Handling

| Error | Action |
|-------|--------|
| `VERCEL_TOKEN` missing | Block task with `missing_credential` |
| Project creation fails (403) | Check Vercel token permissions — needs project creation scope |
| Deployment fails (ERROR state) | Check build logs, report error in task result |
| Deployment times out | Report timeout, check Vercel dashboard manually |
| GitHub repo not found | Verify repo exists and GitHub integration is connected |

## Rules

- **ALWAYS check `VERCEL_TOKEN` first** — fail fast if missing
- **ALWAYS poll until READY or ERROR** — never fire-and-forget
- **ALWAYS update the task** with `deployment_url` in metadata on success
- **NEVER hardcode tokens** — always use environment variables
- **Use `target: "production"`** for production deploys (not preview)
