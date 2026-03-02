# coding-task — Dispatched Coding Task Skill

## When to Use

This skill applies when you receive a message containing:
- A JSON block with `task_id`, `title`, `type: "coding"`
- The header `## Task Assigned:`
- A `## Coding Task` section with repo/branch info

**If you see these markers, follow this skill exactly.**

## Step 1: Parse the Task

Extract from the JSON payload:
- `task_id` — unique identifier (use first 8 chars for branch names)
- `title` — short title
- `description` — full requirements
- `acceptance_criteria` — what "done" looks like (if provided)
- `repo` — target repository (also check the Coding Task section)
- `priority` — urgency level

Known repos (all under `dante-alpha-assistant`):
| Repo | Description | Build |
|------|-------------|-------|
| `queue-dashboard` | Dashboard frontend (Vite) + Express backend | `npm run build` |
| `task-dispatcher` | Task dispatcher service (Node.js ESM) | `node --check index.js` |
| `dante-gitops` | K8s manifests + ArgoCD configs | YAML validation |

If the task mentions a repo not listed here, use the full GitHub URL from the description.

## Step 2: Setup Git

```bash
git config --global user.email "dante-neo-assistant@proton.me"
git config --global user.name "Neo"
```

## Step 3: Prepare the Repo

**CRITICAL: Repos are pre-cloned in your workspace. NEVER clone to `/tmp/`.**

```bash
cd /root/.openclaw/workspace/<repo-name>
git fetch origin
git checkout main
git pull origin main
```

If the repo dir doesn't exist (rare):
```bash
cd /root/.openclaw/workspace
git clone https://x-access-token:${GH_TOKEN}@github.com/dante-alpha-assistant/<repo-name>.git
cd <repo-name>
```

## Step 4: Create a Feature Branch

```bash
git checkout -b feat/<short-kebab-description>
```

Branch naming:
- `feat/<description>` — new features
- `fix/<description>` — bug fixes
- `refactor/<description>` — refactoring
- `chore/<description>` — maintenance

## Step 5: Understand Before Editing

Before making any changes:
1. Read relevant files to understand existing patterns
2. Check project structure (`ls`, `find -name "*.js" | head -20`)
3. Look at recent commits for style conventions (`git log --oneline -10`)
4. Check `package.json` scripts if applicable

## Step 6: Make the Changes

- Keep changes **focused** on the task — don't refactor unrelated code
- Follow existing code style and conventions
- Add comments for complex logic
- Handle errors properly (don't swallow exceptions)

## Step 7: Build and Test

**Always verify before committing:**

```bash
# Node.js projects (queue-dashboard, task-dispatcher)
npm install          # only if package.json changed
npm run build        # if build script exists (queue-dashboard)
npm run lint         # if lint script exists

# task-dispatcher (ESM, no build step)
node --check index.js

# K8s manifests (dante-gitops)
cat <file>.yaml | python3 -c "import sys,yaml; yaml.safe_load(sys.stdin)"
```

If build/lint fails, **fix the issues** before committing.

## Step 8: Commit and Push

```bash
git add -A
git commit -m "<type>: <description>"
git push -u origin feat/<branch-name>
```

Commit message format: `<type>: <concise description>`
Types: `feat`, `fix`, `refactor`, `chore`, `docs`

## Step 9: Create a Pull Request

```bash
gh pr create \
  --repo dante-alpha-assistant/<repo-name> \
  --title "<task title>" \
  --body "## Summary
<what changed and why>

## Changes
- <list key changes>

## Task
- ID: <task_id>
- Priority: <priority>
- Dispatched by: <dispatched_by>

## Testing
<how to verify the changes>" \
  --base main
```

## Step 10: Update Task Status

Use the **curl commands from the dispatch message**. Replace placeholders:
- `DESCRIBE WHAT YOU DID` → real summary including PR number and repo
- Example: `"Created feat/add-search for queue-dashboard. Added search filter to task list with debounce. PR #29 on dante-alpha-assistant/queue-dashboard."`

**NEVER skip this step.** The task board must reflect your work.

## Multi-Repo Tasks

If the task spans multiple repos:
1. Work on each repo sequentially
2. Create a PR in each repo
3. Reference related PRs in each PR body
4. Report ALL PRs in the status update summary

## Error Handling

| Error | Action |
|-------|--------|
| Auth errors | Verify `GH_TOKEN`: `echo $GH_TOKEN \| head -c4`. Report failure if missing. |
| Repo not found | Double-check org/repo name. Report failure with URL tried. |
| Merge conflicts | Pull latest main, rebase, resolve. Report failure if complex. |
| Unclear requirements | Report failure explaining what's ambiguous. |
| Build fails | Include build output in failure report. |

## Common Mistakes to AVOID

- ❌ Editing files without pulling latest first
- ❌ Cloning to `/tmp/` — always use workspace
- ❌ Committing directly to `main`
- ❌ Not running build/tests before committing
- ❌ Generic commit messages — be specific
- ❌ Leaving task `in_progress` — ALWAYS update status when done
- ❌ Forgetting to include PR number in the status update
