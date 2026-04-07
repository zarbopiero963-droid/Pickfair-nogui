# AGENTS.md

## HARD RULES

- Always create a NEW branch from `main`
- Never work directly on `main`
- Never target `Codex`
- Never reuse existing branches
- Never continue work on old `codex/analyze-*` branches

## PR RULES

- One task = one branch = one PR
- If another open PR already touches the same files, STOP and report conflict risk
- Always target `main`

## FILE SCOPE

- Modify ONLY explicitly allowlisted files
- If any unrelated file changes, revert it before finishing
- Do NOT refactor unless explicitly requested
- Do NOT touch production code unless explicitly allowed

## FAILURE CONDITIONS

Abort immediately if:
- branch is not freshly created from `main`
- more files than expected are modified
- task attempts to reuse an old Codex branch

## OUTPUT RULES

Always report:
- branch used
- changed files
- scope respected
- tests run
- push status