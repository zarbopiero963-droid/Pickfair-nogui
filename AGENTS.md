# AGENTS.md

## GLOBAL EXECUTION POLICY

This repository uses strict SERIAL TASK EXECUTION.

The goal is to prevent parallel tasks and duplicate PRs, while still allowing agents to continue fixing the currently open PR when checks, review comments, Codacy, DeepSource, CodeRabbit, Sourcery, Gitar, or GitHub Actions report problems.

---

## Core rules

- Only one active task is allowed at a time.
- Only one open pull request is allowed at a time.
- Never work directly on main.
- Never execute multiple tasks in parallel.
- Never create a second PR while another PR is open.

Important exception:

- If a PR is already open and the current request is explicitly about fixing that same PR, continue working on that same PR.
- Failing checks, review comments, Codacy findings, DeepSource findings, CodeRabbit comments, Sourcery comments, Gitar comments, and GitHub Actions failures on the currently open PR are part of the same active task.
- These PR-fix requests must NOT be blocked just because the PR is already open.

Correct behavior:

- New unrelated task while a PR is open: report BLOCKED.
- Fix/review/check request for the currently open PR: continue on the same PR branch.

---

## Task source

- Pending tasks are stored in: ops/tasks/
- Completed tasks are moved to: ops/tasks_done/
- Tasks must be executed in lexicographical order.
- Do not start a later task while an earlier task is still active.
- Do not start a new task if there is already an open PR for another task.

---

## PR behavior

- Create exactly ONE PR per task.
- Include in the PR body: Task-File: <exact path of the task file>
- If a PR already exists for the current task, continue working on that PR.
- Do not create a new PR for follow-up fixes on the same task.
- Do not create a new PR for review comment fixes.
- Do not create a new PR for failing check fixes.
- Do not merge PRs.

---

## Current PR fix behavior

If the current request is about the currently open PR:

- Continue on the same PR.
- Push to the same PR branch.
- Do not open a new PR.
- Do not merge.
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

---

## Failure handling

If tests fail:

- Continue working on the SAME PR.
- Do not create a new PR.
- Do not merge.
- Fix the failing tests if they are in scope.
- Push the fix to the same PR branch.

If checks fail:

- Continue working on the SAME PR.
- Do not create a new PR.
- Do not merge.
- Fix the failing checks if they are in scope.
- Push the fix to the same PR branch.

If review comments are present:

- Continue working on the SAME PR.
- Do not create a new PR.
- Do not merge.
- Address the active, non-outdated, non-resolved comments.
- If a comment is already outdated or already fixed by the current code, explain that clearly.
- For each comment actually fixed, reply in the related GitHub thread with: Fatto in commit <SHA>

If branch conflicts with base:

- Resolve conflicts in the SAME PR.
- Do not create a new PR.
- Do not merge unless explicitly instructed by the repository owner.

---

## Completion

When the task is complete and merged:

- Move the task file from ops/tasks/ to ops/tasks_done/.
- Do not move the task file before the PR is merged.
- Do not mark the task complete while checks are failing.
- Do not mark the task complete while active review comments remain unresolved.

A task is not complete until:

- The PR has been updated.
- Required tests/checks pass.
- Blocking review comments are resolved, outdated, or explicitly handled.
- The PR is ready for owner review or merge.

---

## Scope control

- Modify only files required by the task.
- Do not refactor unrelated code.
- Do not expand scope.
- Do not change business logic unless explicitly required.
- Do not make broad cleanup changes unless they are necessary for the current task.
- Do not modify unrelated tests.
- Do not modify CI configuration unless the task or failing check specifically requires it.

---

## Stop conditions

Stop immediately and report BLOCKED if:

- A different unrelated PR is already open and the current request is trying to start a new task.
- The task requires files outside the allowed scope.
- The conflict cannot be resolved safely.
- Tests cannot be fixed without violating scope rules.
- The requested work would require opening a second PR.
- The requested work would require working directly on main.

Do NOT stop if:

- The task is to fix the currently open PR.
- The task is triggered by GitHub review comments on the currently open PR.
- The task is triggered by failing checks on the currently open PR.
- The task is triggered by Codacy feedback on the currently open PR.
- The task is triggered by DeepSource feedback on the currently open PR.
- The task is triggered by CodeRabbit feedback on the currently open PR.
- The task is triggered by Sourcery feedback on the currently open PR.
- The task is triggered by Gitar feedback on the currently open PR.
- The task is triggered by GitHub Actions feedback on the currently open PR.

In those cases:

- Continue on the same PR.
- Push to the same PR branch.
- Do not open a new PR.
- Do not merge.
- Report what changed and provide the commit SHA.

---

## Required response format after fixing current PR

After fixing a current PR request, respond with:

DONE / PARTIAL / NOT DONE

Summary:
- <what was changed>

Commit:
- <commit SHA>

New PR head SHA:
- <new PR head SHA>

Review comments handled:
- <comment/thread URL or summary>: Fatto in commit <SHA>
- <comment/thread URL or summary>: skipped because <reason>

Checks:
- <check name>: expected status or result

Notes:
- <anything the repository owner must know>

If unable to push to the PR branch, respond exactly:

NEEDS_MANUAL_UPDATE_BRANCH

and explain why.