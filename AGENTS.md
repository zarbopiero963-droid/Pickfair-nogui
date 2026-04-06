# AGENTS.md

## HARD RULES

- Always create a NEW branch from `main`
- Never work on `main`
- Never target `Codex`
- Never reuse existing branches
- Never continue work on old `codex/analyze-*` branches

## PR RULES

- One task = one branch = one PR
- If another open PR touches the same files → STOP
- Always target `main`

## FILE SCOPE

- Modify ONLY allowlisted files
- If any unrelated file changes → revert it
- Do NOT refactor
- Do NOT touch production code unless explicitly required

## FAILURE CONDITIONS

Abort immediately if:
- branch is not freshly created from `main`
- more files than expected are modified
- Codex tries to reuse an old branch

## STYLE

- Small atomic patches
- Deterministic tests
- Prefer invariants over implementation details