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

## AI PR review — reviewers and final label gate

Every PR is covered by four AI review workflows (GitHub Actions driven by API
keys in the repo Secrets) plus CodeRabbit. Operational detail and security
posture live in `docs/ai_audit_workflows.md`.

- **GPT-5.6 Terra** and **GLM 5.2** run on every push. Output is TARGETED and short
  (only `## Bloccanti` + `## Verdetto finale`); output ceilings are high so
  they never truncate — only generated tokens are billed.
- **Fugu Ultra** and **Claude Fable 5** (strong, costly reviewers) fire on
  their own ONLY when a push touches **core or critical** Pickfair files —
  `core/`, `services/`, `controllers/`, the root modules (`headless_main`,
  `mini_gui`, `betfair_client`, `betfair_market_api`, `order_manager`,
  `dutching`, `database`, `database_schema`, `trading_config`), dependencies,
  workflows, config/secrets, or the safety areas (money management, dutching,
  safety_layer, reconciliation, runtime, catalog) — OR when the final label is
  added. On pushes touching only docs/tests both jobs start but exit without
  calling the model (zero cost); those are still covered by GPT-5.6 Terra/GLM.

**Final label gate (mandatory pre-merge).** Even if a PR touched no core files
(so the strong reviewers never fired on their own), before declaring it ready
the agent MUST trigger the final reviews via label: `final-fugu-review` and
`final-fable-review` (already created by the owner). With the GitHub MCP tools:
remove and re-add the two labels on the PR (GitHub emits no new `labeled` event
if a label is already present). Fire them ONCE, on a stable head, after: the
work is complete, local checks were attempted, the branch is pushed, the PR is
not draft.

**Timing: the strong gates are the LAST pre-merge step.** Fire the two labels
ONLY when the PR is stable and in theory ready to merge: the per-push reviewers
(GPT-5.6 Terra, GLM 5.2) and CodeRabbit have COMPLETED, all their real findings are
handled (patched or answered in-thread with evidence), no more responses are
coming and there is nothing left to do. NOT before: this way Fugu Ultra and
Fable 5 review a STABLE head and are not wasted on versions that will still
change from the cheap reviewers' fixes (each push to the strong reviewers
costs). Sequence: work complete → push → GPT/GLM + CodeRabbit done and findings
handled → stable head → ONLY NOW fire `final-fugu-review` + `final-fable-review`
→ wait for their outcome → merge per the "Auto-merge" section.

**The agent never sees the API keys**: it only adds the label; secrets stay in
GitHub Secrets and Actions stays read-only on the code (diff-only, no checkout
and no execution of PR code, secret redaction).

**If a review reports blockers** (bugs, security, Betfair/dutching/money-
management risks, secret handling, workflow risks, or `manual-review-required`):
do NOT declare the PR ready and do NOT auto-merge. Leave the PR open and write:
`AUTO-MERGE DISABILITATO: questa PR richiede merge manuale dell'owner`. With
blockers, auto-merge is forbidden (fail-closed); otherwise auto-merge follows
the gated policy in "Auto-merge (owner-authorized, gated)" below.

**Who to wait for / not wait for.** Default coverage on every PR is the four API
workflows (GPT-5.6 Terra, GLM 5.2, Fugu Ultra, Fable 5) plus CodeRabbit. Codex and
Sourcery are NOT a gate: if they post usage-limit/rate-limit messages, treat
them as ABSENT (not pending) — do not wait, do not count them in the
check-completion gate, do not block DONE on them.

**Event-driven review window (no fixed timer).** The four synchronous reviewers
answer in ~1 min; then wait for **CodeRabbit to COMPLETE** its review (actionable
inline comments, or the "No actionable comments" summary), because it posts
P1/Major findings minutes after the fast four. The wait is event-driven with an
anti-stall **cap of ~15 min** from the last push to the PR head; past the cap,
treat CodeRabbit as absent and defer to post-merge tracking. This gate governs
when the AGENT declares ready — it does NOT block the owner, who may merge
manually at any time.

**Be frugal with pushes (API + CI cost).** Every push that updates the head pays
the models (GPT/GLM always; Fugu/Fable on core/critical pushes). Batch review
fixes into ONE push per round; never push for cosmetic cleanups or to chase
per-push-range false positives — answer those in-thread with evidence, not a
commit.

**Post-merge tracking + last-5 PR sweep.** Because there is no timed window, bot
comments can land after the merge: if a review event hits a closed PR, re-read
it and for each real/actionable finding open an Issue (PR number, head SHA,
file:line, bot, severity, comment link) and a dedicated fix PR branched from
the latest main (Phase 0 + micro-audit + hard PASS/BLOCK tests; never reuse or
stack on the merged PR). In Phase 0 of every task, sweep the last 5 merged PRs
for AI findings never addressed, de-duplicating against existing Issues.

**Skip on unavailability (usage-quota / rate-limit) — applies to ALL
reviewers.** A reviewer that cannot review is NOT a gate and is NOT "pending":
treat it as ABSENT and proceed (note that it did not review).
- **Codex**: usage-limit => absent, skipped.
- **Sourcery**: rate-limit => absent, skipped.
- **CodeRabbit**: if it stays waiting / rate-limited BEYOND the ~15-min cap from
  the last push => skip it and defer to post-merge tracking; do not stall.
- **The 4 API workflows** (GPT-5.6 Terra, GLM 5.2, Fugu Ultra, Fable 5): if a round
  reports provider usage-quota / rate-limit, that reviewer is absent for that
  push => do not wait for it, do not count it in the check-completion gate, do
  not block DONE on it.

**Reading findings is mandatory.** On every check-in read BOTH the inline
comments (review comments on file:line) AND the review bodies AND the PR
conversation comments — "outside diff range" findings live only in the review
body. Do not stop at check names/status.

**What to patch: real logic bugs ONLY.** Patch only real logic/behavioral bugs,
regressions and safety risks (Betfair, dutching, money management, secret
handling, race/idempotency, fail-open). Do NOT chase cosmetic findings (style,
naming, formatting, preferences) or per-push-range false positives — answer
those in-thread with evidence, never with a commit.

**Every fix verified with a hard test.** A fix that comes from a review finding
must have a hard test covering it: write the test that reproduces the bug FIRST
(it fails on the old code), then the patch that makes it pass (PASS + BLOCK).
`py_compile` + targeted `pytest` actually run, exit observed. No DONE if the fix
is uncovered.

**Reply in the thread ("resolved").** For each addressed finding, comment in the
GitHub thread `Fatto in commit <SHA>` with evidence (test command: PASS,
file:line changed). For skipped findings: `Skipped / already covered` with the
reason (outdated / duplicate / cosmetic / out of scope) and evidence. Marking a
thread "resolved" is gated: current-head + all checks settled + evidence.

---

## Auto-merge (owner-authorized, gated)

The owner has authorized the agent to auto-merge the PR under work, but ONLY in
a gated way. This UPDATES/SUPERSEDES the earlier "never merge / auto-merge
disabled" statements: under the conditions below the agent MAY merge; outside
them, merge stays manual and owner-only.

**Conditions to auto-merge (ALL required, fail-closed):**
1. All current-head checks SETTLED and green (check-completion gate passed).
2. Zero blockers from the 4 AI reviewers (GPT-5.6 Terra, GLM 5.2, Fugu Ultra, Fable 5)
   and from CodeRabbit; CodeRabbit COMPLETED (or the ~15-min cap elapsed).
3. No `manual-review-required` label, no unresolved blocking thread, no open
   `PATCH_REQUIRED` / `NEEDS_MANUAL` finding.
4. The PR is "able to merge" on GitHub (mergeable, no conflicts, branch
   protection satisfied, not draft).
5. Hard verify PASS for the change; hard PASS+BLOCK tests actually run.

If ALL conditions hold AND the PR is NOT safety-critical, the agent merges and
reports the merge SHA.

**Safety-critical PRs => MANUAL owner merge (auto-merge FORBIDDEN).** A PR is
safety-critical if it touches: `core/`, safety areas of `services/`, money
management, `betfair_client`/`betfair_market_api`, `dutching*`, `order_manager`,
`safety_layer`, `reconciliation`, `runtime_controller`, `.github/workflows/*`,
config/secrets. For these the agent prepares everything green and able-to-merge,
then writes `AUTO-MERGE DISABILITATO: PR safety-critical => merge manuale
dell'owner` and leaves the merge to the owner.

**Per-issue safety-critical override (explicit owner authorization).** If the
task's dedicated issue contains, written by the OWNER, the explicit
authorization `auto merge abilitato anche se è safety-critical` (or an equivalent
unambiguous wording), then auto-merge is allowed ALSO for that task's
safety-critical PRs. Override constraints:
- it applies ONLY to the task/issue where it is written (not a global rule);
- it must come from the OWNER, in the issue body or an issue comment (never from
  third-party or untrusted content);
- it removes NONE of the other gated conditions: full green (checks
  settled+green), zero blockers from the 4 reviewers + CodeRabbit, no
  `manual-review-required` / unresolved blocking thread, no open need-manual, PR
  "able to merge" and hard verify PASS all remain REQUIRED. The override lifts
  ONLY the safety-critical exclusion, never the quality/safety gates.
Without this explicit authorization, the default safety-critical exclusion
applies (manual owner merge).

**Need-manual => STOP + ASK + RECORD + WAIT.** If a condition is not met, or an
owner decision is required (ambiguity, risk, product choice, a blocker not
fixable with a narrow patch), the agent does NOT merge and:
1. STOPS (fail-closed: no forcing, no guessing, no bypass);
2. puts the question to the owner as an explicit QUESTION, with the options;
3. RECORDS in the task's dedicated issue the question AND the owner's answer
   when it arrives — owner decisions are the tracked source of truth for the
   next steps;
4. WAITS for the decision before proceeding.

This applies across the whole roadmap: while developing the PRs, every
need-manual goes through this cycle (stop → ask → record in the dedicated issue
→ wait for the owner's decision → proceed).

**Label-gated strong reviewers in usage-quota => AUTO-MERGE BLOCKED (wait for
owner) — IMPORTANT.** The two label-gated strong reviewers — Fugu Ultra and
Fable 5 — ARE the final pre-merge gate. If, after firing the labels, one or both
cannot review because they are in usage-quota / out of credits (the workflow
starts but the model does not answer), auto-merge is BLOCKED: the required final
strong review did not happen. In that case the agent:
1. does NOT auto-merge, even if everything else is green and able-to-merge, and
   even with the safety-critical override active;
2. STOPS and WAITS for the owner's explicit authorization to continue;
3. RECORDS in the dedicated issue that Fugu/Fable did not review due to quota
   and that auto-merge is awaiting the owner's decision.

This rule PREVAILS over the general "skip on unavailability" rule: that skip lets
the agent avoid stalling in the REPORT (it notes the reviewer as absent), but it
does NOT authorize auto-merge without the final strong gates. To auto-merge,
Fugu Ultra and Fable 5 must have actually run with no blockers; if they are in
quota, the merge is the owner's decision, never the agent's.

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