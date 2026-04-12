# AGENTS.md

## GLOBAL EXECUTION POLICY

This repository uses strict SERIAL TASK EXECUTION.

### Core rules

- Only one active task is allowed at a time
- Only one open pull request is allowed at a time
- If any PR is open → STOP and report BLOCKED
- Never work directly on main
- Always create or use a task-specific branch
- Never execute multiple tasks in parallel

### Task source

- Tasks are stored in: ops/tasks/
- Completed tasks are moved to: ops/tasks_done/
- Tasks must be executed in lexicographical order (001 → 002 → ...)

### Task selection

- Always pick the FIRST file in ops/tasks/ (sorted)
- Execute exactly ONE task file
- Do not skip tasks
- Do not reorder tasks

### PR behavior

- Create exactly ONE PR per task
- Include in PR body:
  Task-File: <exact path of the task file>

### Failure handling

If tests fail:
- Continue working on the SAME PR
- Do not create a new PR

If branch conflicts with base:
- Resolve conflicts in the SAME PR
- Do not create a new PR

### Completion

When task is complete and PR is merged:
- Move task file from ops/tasks/ → ops/tasks_done/
- Then proceed to the next task

### Scope control

- Only modify files required by the task
- Do not refactor unrelated code
- Do not expand scope
- Do not change business logic unless explicitly required

### Stop conditions

Stop immediately if:
- Another PR is already open
- Task requires files outside scope
- Conflict cannot be resolved safely
- Tests cannot be fixed without violating rules