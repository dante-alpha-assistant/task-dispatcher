# coding-task — Coding Task Skill

You are executing a coding task dispatched by the task board. Follow these steps in order.

## Steps

### 1. Parse the Task
Extract from the JSON payload: `task_id`, `title`, `description`, `repo`, `app_id`.

### 2. Setup Git
```bash
git config --global user.email "dante-neo-assistant@proton.me"
git config --global user.name "Neo"
```

### 3. App Repo Validation (MANDATORY if app_id is set)

If the task payload includes `app_id`, you are scoped to specific repos. **Before cloning or pushing:**

1. Check the **App Scope** section in the dispatch message for the allowed repos list
2. Verify the repo you plan to clone is in the allowed list
3. If the repo is NOT in the allowed list → **STOP and FAIL the task** with error: "Repo not in allowed list for app scope"
4. After making changes, before pushing, verify your remote URL matches an allowed repo:
   ```bash
   REMOTE_URL=$(git remote get-url origin)
   echo "Pushing to: $REMOTE_URL"
   # Verify this matches one of the ALLOWED REPOS from the App Scope section
   ```
5. **NEVER push to a repo outside the allowed list.** This is a HARD FAILURE.

If no `app_id` is set, skip this check.

### 4. Clone or Update the Repo
If repo not yet cloned in workspace:
```bash
cd /tmp
git clone https://x-access-token:${GH_TOKEN}@github.com/{owner}/{repo}.git
cd {repo}
```
If already cloned:
```bash
cd /tmp/{repo}
git checkout main && git pull origin main
```

### 5. Create a Feature Branch
```bash
git checkout -b feat/{short-task-description}
```

### 6. Make Changes
- Read existing code to understand the codebase
- Keep changes focused on what the task asks for
- If a migration is needed (new DB columns/tables), create it in `migrations/NNNN_description.sql`

### 7. Commit and Push
```bash
git add -A
git commit -m "{type}: {description}"
git push -u origin feat/{short-task-description}
```

### 8. Create PR
```bash
gh pr create --title "{task title}" --body "Task ID: {task_id}\n\n## Summary\n{description}" --base main
```

### 9. Update Task Status
Use the curl command from the dispatch message to update the task status with the PR URL.

## Rules
- NEVER edit files without cloning the repo first
- NEVER commit to main directly
- NEVER push to repos outside the App Scope allowed list (if app_id is set)
- If the task is unclear, set status to `blocked` with explanation
- If you cannot complete the task, set status to `failed` with error details
