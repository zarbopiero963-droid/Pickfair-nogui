# AGENTS.md

## Branch policy
- Always branch from `main`
- Never target `Codex` as base branch
- Never reuse task branches
- One task = one branch = one PR

## Conflict prevention
- If an open PR already modifies the same files, stop and report conflict risk
- Do not open parallel PRs on the same files
- Do not stack follow-up PRs for the same fix

## Scope lock
- Modify only the requested files
- Do not refactor unrelated code
- Do not reformat unrelated files
- If additional files seem required, stop and report

## Merge discipline
- Prefer one clean PR over many partial PRs
- After merge, always restart from fresh `main`

## Output discipline
Always report:
- branch used
- changed files
- scope respected
- tests run
- push status