# Skills

Agent skills that are deployed to worker agent workspaces.

## coding-task

The coding-task skill defines the full workflow for coding tasks:
git setup → repo prep → branching → code changes → build/test → commit → PR → status update.

**Location in worker workspace:** `skills/coding-task/SKILL.md`

The task dispatcher automatically injects a `## Coding Task` section in dispatch messages
for `type: "coding"` tasks, which instructs the worker to read and follow this skill.
