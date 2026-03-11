# deploy-batch — Batch Deploy Skill

You are executing a batch deploy task. Process each PR individually — merge what works, fail what doesn't.

## Input

Your task metadata contains:
- `metadata.batch_tasks`: Array of `{id, title, pr_url}` — the tasks to deploy
- `metadata.repos`: Object grouped by repo name, each containing PRs with `pr_number`
- `metadata.strategy`: Usually `sequential_rebase`

## Execution Steps

### Phase 1: Clone Repo

```bash
cd /tmp && git clone https://github.com/{owner}/{repo}.git deploy-{repo}
cd deploy-{repo}
git config user.email 'deploy@openclaw.ai'
git config user.name 'Deploy Agent'
```

### Phase 2: Merge Each PR via GitHub

Process PRs in order (lowest PR number first). Track results for each.

For each PR:

```bash
# 1. Rebase onto latest main
gh pr checkout {pr_number}
git fetch origin main
git rebase origin/main
```

If rebase has conflicts → resolve them (see Conflict Resolution below).

```bash
# 2. Force-push the rebased branch
git push --force-with-lease origin HEAD

# 3. Merge via GitHub (NOT direct push to main)
gh pr merge {pr_number} --rebase --admin

# 4. VERIFY merge succeeded — this is non-negotiable
STATE=$(gh pr view {pr_number} --json state -q .state)
if [ "$STATE" != "MERGED" ]; then
  echo "FAILED: PR #{pr_number} state is $STATE"
  # Mark this subtask as deploy_failed and continue to next PR
fi

# 5. Update local main for next PR
git checkout main
git pull origin main
```

**If a PR fails: mark that subtask as `deploy_failed` and continue to the next PR.** Don't stop the whole batch — deploy what you can.

### Phase 3: Update Subtask Statuses

**This is the most critical phase. Every subtask MUST end in the correct state.**

Use the Supabase Management API to bypass the `enforce_status_progression` trigger.

For EACH subtask, based on whether its PR merged:

```bash
# For successfully merged PRs → deployed
curl -s -X POST "https://api.supabase.com/v1/projects/lessxkxujvcmublgwdaa/database/query" \
  -H "Authorization: Bearer $SUPABASE_MGMT_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"query\": \"ALTER TABLE agent_tasks DISABLE TRIGGER enforce_status_progression; UPDATE agent_tasks SET status = 'deployed', updated_at = now() WHERE id = '{task_id}'; ALTER TABLE agent_tasks ENABLE TRIGGER enforce_status_progression;\"}"

# For failed PRs → deploy_failed
curl -s -X POST "https://api.supabase.com/v1/projects/lessxkxujvcmublgwdaa/database/query" \
  -H "Authorization: Bearer $SUPABASE_MGMT_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"query\": \"ALTER TABLE agent_tasks DISABLE TRIGGER enforce_status_progression; UPDATE agent_tasks SET status = 'deploy_failed', error = 'PR #{pr_number} failed to merge: {reason}', updated_at = now() WHERE id = '{task_id}'; ALTER TABLE agent_tasks ENABLE TRIGGER enforce_status_progression;\"}"
```

### Phase 4: Verify Subtask Statuses

**Always verify. Don't fire-and-forget.**

```bash
curl -s "https://lessxkxujvcmublgwdaa.supabase.co/rest/v1/agent_tasks?id=in.({task_ids})&select=id,status" \
  -H "apikey: $SUPABASE_SERVICE_ROLE_KEY"
```

Every subtask must be either `deployed` or `deploy_failed`. If any are still `deploying` or `completed`, the update failed — retry it.

### Phase 5: Wait for CI + Verify Deployment

```bash
# Get the latest commit on main after merges
COMMIT=$(git rev-parse origin/main)

# Poll CI every 30s for up to 10 minutes
gh run list --commit $COMMIT --json status,conclusion --limit 1

# Verify ArgoCD deployed the new pod
kubectl get pods -n dev -l app={app-name} --no-headers
kubectl logs -n dev deploy/{app-name} --tail=5
```

### Phase 6: Report Results

```json
{
  "summary": "Deployed 4/5 PRs to queue-dashboard",
  "merged": [
    {"id": "task-uuid", "pr": "#206", "github_state": "MERGED"},
    {"id": "task-uuid", "pr": "#207", "github_state": "MERGED"}
  ],
  "failed": [
    {"id": "task-uuid", "pr": "#180", "reason": "Unresolvable conflict in App.jsx"}
  ],
  "ci_run": "https://github.com/.../actions/runs/12345",
  "verified": true
}
```

## Conflict Resolution

If `git rebase origin/main` fails:

1. `git diff --name-only --diff-filter=U` — list conflicting files
2. Read and resolve each file (understand both sides)
3. `git add {file} && git rebase --continue`
4. Verify: `node --check {server_files}` and `cd client && npm run build`
5. 2 attempts max — if unresolvable, mark that PR as failed and move on

## State Transition Rules

```
completed → deploying    (set by batch deploy creation)
deploying → deployed     (PR merged successfully — use trigger bypass)
deploying → deploy_failed (PR failed to merge — use trigger bypass)
```

**The trigger bypass (`ALTER TABLE ... DISABLE/ENABLE TRIGGER`) is REQUIRED for these transitions.** Standard Supabase REST API patches will be blocked by the `enforce_status_progression` trigger.

## Rules

- **ALWAYS use `gh pr merge` to merge** — NEVER push directly to main
- **ALWAYS verify PR state is MERGED after merging** — `gh pr view --json state`
- **ALWAYS update subtask statuses via Supabase Management API** (trigger bypass)
- **ALWAYS verify subtask statuses after updating**
- **NEVER leave subtasks stuck in `deploying`** — every subtask must end as `deployed` or `deploy_failed`
- **NEVER use kubectl to modify ArgoCD-managed resources**
- **Continue on failure** — merge what works, fail what doesn't
