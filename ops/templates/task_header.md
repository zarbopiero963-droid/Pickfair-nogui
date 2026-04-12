# SERIAL EXECUTION POLICY

- Only one active task at a time
- Only one open pull request at a time
- If any repository PR is already open, STOP and report BLOCKED
- Never work directly on main
- If tests fail, keep fixing the SAME PR
- If branch conflicts with base, resolve on the SAME PR
- Do not start any later task
- Execute exactly one task file only
- Do not modify task files except when explicitly instructed by repository automation

## EXECUTION MODE

- Always read this file before executing any task
- Always read the selected task file from ops/tasks/
- Treat the task file as the single source of truth
- Do not invent requirements outside the task
- Do not expand scope