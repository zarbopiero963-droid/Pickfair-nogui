# AGENTS.md

## GLOBAL EXECUTION POLICY

This repository uses strict SERIAL TASK EXECUTION.

The goal is to prevent parallel work, duplicate pull requests, scope creep, unsafe changes, and accidental work on `main`, while still allowing agents to:

1. create a new PR when no PR exists for the requested task;
2. continue fixing the currently open PR when checks, review comments, static analysis, or handoff files request follow-up fixes.

---

## Core rules

- Only one active task is allowed at a time.
- Only one open pull request is allowed at a time.
- Never work directly on `main`.
- Never execute multiple tasks in parallel.
- Never create a second PR while another PR is open.
- Never merge a PR unless explicitly instructed by the repository owner.
- Never mark work complete while required checks are failing or blocking review comments remain unresolved.
- Never expand scope beyond the current task, current PR, or provided handoff file.

---

## Auto mode detection

Before starting work, determine the mode.

### Mode A — Current PR repair mode

Use this mode if any of these are true:

- The prompt mentions an existing PR number.
- The prompt mentions an existing PR branch.
- The current branch is already associated with an open PR.
- A handoff `.md` references an existing PR.
- The request is about failing checks, review comments, Codacy, DeepSource, CodeRabbit, Sourcery, Gitar, GitHub Actions, or other feedback on an existing PR.
- The request says to continue on the same PR, same branch, or current PR.

Behavior:

- Continue on the existing PR branch.
- Do not create a new branch.
- Do not create a new PR.
- Do not merge.
- Do not work on `main`.
- Fix only the reported current-PR issues.
- Commit and push to the same PR branch.
- Report the commit SHA and new PR head SHA.

### Mode B — New task / new PR mode

Use this mode only if all are true:

- No open PR exists for the requested work.
- The request is a new task, not a fix for an existing PR.
- The task is explicitly provided by the repository owner, Codex Web, Codex CLI, Linear, Slack, Telegram, GitHub issue/comment, or another clear prompt/handoff.
- The work can be completed without violating scope rules.

Behavior:

- Create a new branch from the correct base branch.
- Implement only the requested task.
- Create exactly one PR.
- Do not merge.
- Include a clear PR body with:
  - what changed
  - why it changed
  - tests/checks run
  - limitations or follow-up notes
  - task identifier or source if provided

### Mode C — Blocked mode

Use this mode if:

- A different unrelated PR is already open and the request is trying to start a new task.
- The task requires files outside allowed scope.
- The requested work would require opening a second PR.
- The requested work would require working directly on `main`.
- The requested work would require merging without explicit owner instruction.
- The current PR branch cannot be determined safely.
- Git remote or credentials are missing and pushing/creating a PR is required.

Behavior:

- Stop immediately.
- Report `BLOCKED` or `NEEDS_MANUAL_UPDATE_BRANCH` as appropriate.
- Explain exactly what owner action is required.

---

## Task source

Tasks may come from:

- Codex Web prompts.
- Codex CLI prompts.
- Repository owner instructions.
- GitHub issue/PR comments.
- Linear/Slack/Telegram handoff messages.
- Generated `.md` handoff files.
- GitHub Actions automation reports.
- Failing checks and review feedback on the current PR.

There is no required `ops/tasks/` task directory.

Do not assume tasks are stored in `ops/tasks/`.
Do not move task files to `ops/tasks_done/`.
Do not create, delete, or reorganize task files unless the repository owner explicitly requests it.

---

## New task behavior

When no PR exists for the requested task:

1. Confirm the current branch is not `main` before editing.
2. Create a dedicated branch for the task.
3. Implement only the requested task requirements.
4. Run relevant tests/checks.
5. Commit only relevant files.
6. Push the branch.
7. Create exactly one PR.
8. Do not merge the PR.

The PR body must include:

```text
Summary:
- <what changed>

Reason:
- <why it changed>

Tests:
- <commands run and results>

Scope:
- <files changed>

Notes:
- <limitations or follow-up>
```

If the task includes a task file path, task ID, Linear issue, GitHub issue, Slack/Telegram handoff, or other identifier, include it in the PR body.

When there is already an open PR:

- If the requested work is unrelated to the open PR, stop and report `BLOCKED`.
- If the requested work is about fixing the open PR, continue on the same PR branch.
- If the request includes a handoff `.md` for the open PR, treat it as current-PR repair work.

---

## Current PR fix behavior

If the current request is about the currently open PR:

- Continue on the same PR branch.
- Push to the same PR branch.
- Do not create a new PR.
- Do not create a new branch unless explicitly required to recover from a broken local checkout.
- Do not merge.
- Do not work on `main`.
- Fix only the reported problems.
- Keep the scope limited to the current PR.
- After making changes, push a new commit to the current PR branch.
- Report the commit SHA and the new PR head SHA.

This applies to:

- GitHub review comments.
- GitHub review threads.
- Failing GitHub Actions workflows.
- Failing checks.
- Codacy findings.
- DeepSource findings.
- CodeRabbit comments.
- Sourcery comments.
- Gitar comments.
- Security/scanner feedback attached to the PR.
- Generated `.md` handoff files for the current PR.
- Telegram/Slack/Linear/GitHub handoff reports that clearly reference the current PR.

---

## Handoff file behavior

A handoff file may contain failing checks, logs, annotations, review comments, and a section such as:

```text
FIX THESE ISSUES ONLY
```

When a handoff file is provided for the current PR:

- Treat it as the source of truth for the current PR fix cycle.
- Start from `FIX THESE ISSUES ONLY` if that section exists.
- Fix only the deduplicated issues listed there.
- Use failing checks, logs, annotations, and review comments as evidence.
- Ignore duplicated old monitor comments.
- Ignore PR Health Monitor / PR Telegram Notify / PR Codex Monitor self-check noise.
- Ignore Node.js/action deprecation warnings unless they are directly blocking the current PR.
- Do not chase unrelated style cleanup outside files touched by the PR.
- Do not refactor unrelated code.
- Do not modify unrelated tests.
- Do not change business logic unless the handoff/task explicitly requires it.

If the handoff conflicts with repository rules, follow repository rules and report the conflict.

If the handoff asks to create a new PR while the current task already has an open PR, do not create a new PR. Continue on the existing PR branch and report that the handoff was interpreted as a current-PR fix request.

When a handoff file is provided and no PR exists:

- Treat it as a new task only if it clearly describes new work.
- Create one branch and one PR.
- Do not treat old PR logs as active work unless they clearly apply to the new task.

---

## PR behavior

- Create exactly one PR per task.
- If no PR exists for the task, create one PR after completing the requested work.
- If a PR already exists for the current task, continue working on that PR.
- Do not create a new PR for follow-up fixes.
- Do not create a new PR for review comment fixes.
- Do not create a new PR for failing check fixes.
- Do not create a new PR for Codacy/DeepSource/static analysis fixes.
- Do not create a new PR from a handoff file that references the current PR.
- Do not merge PRs.

---

## Failure handling

If tests fail:

- Continue working on the same PR if this is current-PR repair mode.
- Do not create a new PR.
- Do not merge.
- Fix the failing tests if they are in scope.
- Push the fix to the same PR branch.

If checks fail:

- Continue working on the same PR if this is current-PR repair mode.
- Do not create a new PR.
- Do not merge.
- Fix the failing checks if they are in scope.
- Push the fix to the same PR branch.

If review comments are present:

- Continue working on the same PR.
- Do not create a new PR.
- Do not merge.
- Address active, non-outdated, non-resolved comments.
- If a comment is already outdated or already fixed by the current code, explain that clearly.
- For each comment actually fixed, reply in the related GitHub thread with:

```text
Fatto in commit <SHA>
```

If branch conflicts with base:

- Resolve conflicts in the same PR.
- Do not create a new PR.
- Do not merge unless explicitly instructed by the repository owner.
- If the conflict cannot be resolved safely, stop and report `BLOCKED`.

---

## Git and branch safety

Before making changes:

- Confirm the current branch is not `main`.
- If fixing an existing PR, confirm the branch matches the current PR branch.
- If creating a new PR, create a dedicated task branch from the correct base.
- Confirm the Git remote exists.
- Fetch the latest remote state before editing.
- Do not force-push unless explicitly instructed by the repository owner.

When committing:

- Commit only relevant files.
- Use a clear commit message.
- Do not include generated temporary files, logs, secrets, local caches, or unrelated artifacts.
- Push only to:
  - the existing PR branch in current-PR repair mode; or
  - the newly created task branch in new-task mode.

If unable to push:

Respond exactly:

```text
NEEDS_MANUAL_UPDATE_BRANCH
```

Then explain why, including:

- current branch
- expected branch
- whether `git remote -v` exists
- whether push failed
- whether credentials are missing

---

## Completion

A task is not complete until:

- The PR has been created or updated.
- Required tests/checks pass or are clearly outside scope.
- Blocking review comments are resolved, outdated, or explicitly handled.
- The PR is ready for owner review or merge.

Do not mark work complete while checks are failing.
Do not mark work complete while active review comments remain unresolved.
Do not move files to `ops/tasks_done/` unless explicitly instructed by the repository owner.

---

## Scope control

- Modify only files required by the task, checks, review comments, or handoff file.
- Do not refactor unrelated code.
- Do not expand scope.
- Do not change business logic unless explicitly required.
- Do not make broad cleanup changes unless necessary for the current task.
- Do not modify unrelated tests.
- Do not modify CI configuration unless the task or failing check specifically requires it.
- Do not silence tests or checks just to make the PR green.
- Do not delete tests unless the task explicitly requires it and the reason is documented.
- Do not remove guardrails.
- Do not bypass security/static analysis findings by ignoring them without justification.

---

## Stop conditions

Stop immediately and report `BLOCKED` if:

- A different unrelated PR is already open and the current request is trying to start a new task.
- The task requires files outside the allowed scope.
- The conflict cannot be resolved safely.
- Tests cannot be fixed without violating scope rules.
- The requested work would require opening a second PR.
- The requested work would require working directly on `main`.
- The requested work would require merging without explicit owner instruction.
- The requested work would require disabling project guardrails.
- The requested work would require exposing secrets or credentials.
- The requested mode cannot be determined safely.

Do not stop if:

- The task is to fix the currently open PR.
- The task is triggered by GitHub review comments on the currently open PR.
- The task is triggered by failing checks on the currently open PR.
- The task is triggered by Codacy feedback on the currently open PR.
- The task is triggered by DeepSource feedback on the currently open PR.
- The task is triggered by CodeRabbit feedback on the currently open PR.
- The task is triggered by Sourcery feedback on the currently open PR.
- The task is triggered by Gitar feedback on the currently open PR.
- The task is triggered by GitHub Actions feedback on the currently open PR.
- The task is triggered by a handoff `.md` file for the currently open PR.

In those cases:

- Continue on the same PR.
- Push to the same PR branch.
- Do not open a new PR.
- Do not merge.
- Report what changed and provide the commit SHA.

---

## Required response format after creating a new PR

After completing a new task and creating a PR, respond with:

```text
DONE / PARTIAL / NOT DONE

Summary:
- <what was changed>

Branch:
- <branch name>

PR:
- <PR URL or number>

Commit:
- <commit SHA>

Checks:
- <command run>: pass/fail/skipped with reason

Files changed:
- <file path>

Files created:
- <file path>

Notes:
- <anything the repository owner must know>
```

If unable to create the PR or push the branch, respond exactly:

```text
NEEDS_MANUAL_UPDATE_BRANCH
```

and explain why.

---

## Required response format after fixing current PR

After fixing a current PR request, respond with:

```text
DONE / PARTIAL / NOT DONE

Summary:
- <what was changed>

Commit:
- <commit SHA>

New PR head SHA:
- <new PR head SHA>

Checks:
- <check name>: expected status or result
- <command run>: pass/fail/skipped with reason

Review comments handled:
- <comment/thread URL or summary>: Fatto in commit <SHA>
- <comment/thread URL or summary>: skipped because <reason>

Files changed:
- <file path>

Notes:
- <anything the repository owner must know>
```

If unable to push to the PR branch, respond exactly:

```text
NEEDS_MANUAL_UPDATE_BRANCH
```

and explain why.

---

## Required response format when blocked

When blocked, respond with:

```text
BLOCKED

Reason:
- <why work cannot proceed safely>

Detected mode:
- <Current PR repair / New task / Unknown>

Current state:
- Open PR: <number or unknown>
- Current branch: <branch or unknown>
- Expected branch: <branch or unknown>

Required owner action:
- <what the repository owner must do next>
```

---

## Automation-specific rules

Automated agents, Codex CLI, Codex Web, self-hosted runners, Slack, Telegram, Linear, and GitHub Actions must all follow this file.

For new-task automation:

- Run only when no unrelated PR is open.
- Create one branch.
- Create one PR.
- Do not merge.
- Stop after PR creation and notify the owner.

For automated PR repair loops:

- Use the handoff file as input.
- Apply only current PR fixes.
- Make at most one fix commit per automation attempt.
- Push only to the current PR branch.
- Let GitHub checks run after push.
- If checks remain red, generate or wait for a new handoff and run another controlled attempt.
- Do not loop forever.
- Stop after the configured maximum attempts and notify the owner.

Recommended automation limit:

```text
Maximum automatic attempts per PR/check state: 3
```

If the maximum attempt limit is reached:

```text
PARTIAL

Summary:
- Automatic repair attempts reached the configured limit.

Notes:
- Owner review required before continuing.
```