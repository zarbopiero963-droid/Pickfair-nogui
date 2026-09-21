# AGENTS.md

## GLOBAL EXECUTION POLICY

This repository uses strict, safe, SERIAL TASK EXECUTION with gated automation.

Project goal: maintain and improve **Pickfair (no-GUI)**, a headless Betfair
trading bot that:

- listens to selected Telegram signal chats/channels and parses supported
  signal formats;
- validates markets and prices via the Betfair API (`betfair_client`,
  `betfair_market_api`, `market_validator`);
- computes dutching stakes and places/cancels orders (`dutching*`,
  `order_manager`, `executor_manager`, cashout modules);
- enforces money management, safety layers, circuit breakers and safe mode
  (`safe_mode*`, `circuit_breaker`, `auto_throttle`, `safety_logger`);
- persists state in a local database (`database`, `database_schema`) and
  reconciles it with Betfair on startup/recovery;
- runs headless (`headless_main`) with an optional mini GUI (`mini_gui`,
  `ui_panels`).

The repository is safety-critical because a wrong order, a duplicated bet, a
stale signal replay, or a weakened safety gate can place **real money** on
Betfair. Runtime behavior around orders, stakes, dedupe, reconciliation and
teardown must be treated with the same care as production trading code.

The goal of this policy is to prevent parallel work, duplicate pull requests,
scope creep, unsafe changes, and accidental work on `main`, while still
allowing agents to:

1. create a new PR when no PR exists for the requested task;
2. continue fixing the currently open PR when checks, review comments, static
   analysis, or handoff files request follow-up fixes.

---

## Mandatory companion specs

This file is the entry point, not the whole rulebook. Before any task that
modifies code destined for a PR, touches `scripts/pr_*.py` or their tests, or
requires commit/push/resolve/merge-readiness decisions, the agent MUST also
read and follow:

- `CLAUDE.md` — non-negotiable rules (fail-closed, one PR, current-head only,
  gated external actions, check-completion gate, docs-in-same-PR).
- `docs/auto_pr_flow_spec.md` — the full PR state machine (INIT, preflight,
  Phase 0, patch, micro-audit, tests, gated push, check status, review triage,
  fix loop, evidence resolve, READY_TO_MERGE / NEEDS_MANUAL / FAILED).
- `docs/hard_verify_spec.md` — the layered final implementation verification
  (contract, static audit, PASS+BLOCK tests, wiring, scope, fail-closed,
  docs alignment §12-bis, final labels).
- `docs/ai_audit_workflows.md` — operational detail of the AI review
  workflows and their security posture.

If this file and a spec ever conflict, the stricter (more fail-closed) rule
wins, and the conflict must be reported.

---

## Core rules

- Only one active task is allowed at a time.
- Only one open pull request is allowed at a time.
- Never work directly on `main`.
- Never execute multiple tasks in parallel.
- Never create a second PR while another PR is open.
- Merge is gated: auto-merge is allowed ONLY under the conditions of the
  "Auto-merge (owner-authorized, gated)" section; outside them merge is
  manual and owner-only. Safety-critical PRs are never auto-merged without
  the explicit per-issue owner override.
- Never mark work complete while checks are pending, checks are failing, or
  blocking review comments remain unresolved.
- Never expand scope beyond the current task, current PR, or provided handoff.
- Every task that modifies code MUST add or update truthful hard tests that
  exercise the real behavior of the change — including, where relevant,
  resilience scenarios (crash/recovery, reconnect, concurrency/race,
  START/STOP teardown, order dedupe, reconciliation, write-failure with
  rollback). A code change without matching hard tests is an incomplete PR
  and cannot be declared DONE.
- Every code change updates the corresponding documentation in the SAME PR
  (see "Documentation maintenance"): docs must never drift from code.
- Never commit secrets: real Betfair app keys, session tokens, certificates,
  real Telegram bot tokens or chat IDs, a `config.json` containing real
  credentials, or `.env` files — never, with no exception (an explicit
  request cannot authorize committing secrets).
- Never commit database files, logs, caches, build artifacts, EXE/ZIP
  artifacts, or generated reports, unless explicitly requested.
- Never add direct real-money execution paths, browser automation,
  mouse/keyboard automation, or new live-trading capabilities beyond the
  existing gated runtime, unless explicitly requested by the owner and
  protected by a dedicated safety plan.
- Respect `files_allowed` / `files_forbidden`. By default NEVER touch:
  `.github/workflows/*`, `core/*`, `services/*`, secrets, runtime trading
  files, Betfair modules, Telegram live modules — UNLESS the task spec
  explicitly includes them in `files_allowed`. Violation => FAILED.
- New-task PRs MUST register the task key in `.guardrails/allowed_scope.json`
  (`tasks` object, with its allowed `files` and `max_files`) in the SAME PR.
  The `[TASK: <key>]` marker must match a registered key: the `guard` check
  validates presence AND registration — a present marker with an unregistered
  key => "Unknown TASK tag" => guard FAILED. The PR may include
  `.guardrails/allowed_scope.json` among its own files (it is not a critical
  file): it self-registers and the guard passes on the same head.

---

## Project-specific safety invariants

The following behavior must be preserved unless the task explicitly asks to
change it.

### Betfair / order safety

- Order placement must stay idempotent: the same signal must never produce
  duplicate orders. Dedupe state must survive restart.
- Never bypass, weaken, or default-disable `safety_layer` behavior,
  `circuit_breaker`, `auto_throttle`, `safe_mode` / `safe_mode_manager`, or
  reconciliation. Their fail-closed defaults are the product.
- Startup/recovery must reconcile local state with Betfair before any new
  order can be placed; stale pending orders must be handled, never silently
  dropped or blindly re-placed.
- Unmatched-order TTL, cancel paths and cashout paths
  (`direct_unmatched_ttl`, `cashout_*`) must not be weakened silently.
- API errors, timeouts and ambiguous responses must fail closed (no order /
  block) — never fail open into placing or repeating a bet.
- Never hardcode credentials, session tokens, or user-specific paths.

### Dutching / money management safety

- Never silently change stake calculation, dutching distribution, rounding,
  min/max price validation, liability caps, daily limits, or `stake_mode`
  behavior. Any change here is safety-critical by definition.
- Invalid or incomplete parsed signals must be skipped, blocked, or clearly
  logged — never turned into a partial or "best effort" betting instruction.
- Numeric handling (odds/stake conversion, comma vs dot, precision) must be
  covered by tests when touched.

### Telegram safety

- The Telegram bot token must never be printed in full, committed, or
  exposed in logs (`telegram_sanitizer` exists for a reason — keep using it).
- Chat/source filtering must remain strict. If chat_id filtering exists, do
  not weaken it. Never make the bot listen to every chat/channel unless the
  task explicitly requests it.
- Do not process old Telegram messages on startup unless the task explicitly
  requires replay behavior and deduplication exists.
- Do not retry the same message into an order without a deduplication rule.
- A new START epoch must invalidate old pollers/listeners: no stale listener
  may survive and write into the new session.

### Config & secrets safety

- Settings must survive app close/reopen. Corrupted config must be backed up,
  not silently overwritten; failed saves must not destroy the existing config.
- Defaults must be safe (live/real-money behavior never enabled by default).
- Never store real secrets in committed files; `config.json` in the repo must
  contain only safe placeholders/defaults.
- Keep backward compatibility with existing config keys where practical; a
  removed/renamed config key is a documented, deliberate change.

### Database safety

- `database_schema` changes are breaking changes: they require explicit task
  approval, a migration/compatibility note, and tests. Never silently drop or
  repurpose columns/tables.
- Writes that are part of order lifecycle (queue, dedupe, daily limits, PnL)
  must be consistent: a failed write must roll back related state so a signal
  can be retried safely, never half-applied.

### Runtime / GUI safety

- `headless_main` must remain runnable headless; the mini GUI is optional and
  must not become a hard dependency of the runtime.
- START failure must not leave the session active; STOP/shutdown
  (`shutdown_manager`) must tear down listeners, timers and executors cleanly.
- Expiry, manual clear, and processing of the same signal must be serialized
  (no race between timers and handlers).
- Do not remove or hide safety-relevant controls, logs or status indicators
  without explanation.

---

## Mandatory execution sequence

For any task that modifies code, tests, workflows, parser behavior, Betfair
behavior, dutching/money-management behavior, Telegram behavior, config or
database behavior, runtime behavior, or build behavior, the agent must follow
this exact sequence (detailed in `docs/auto_pr_flow_spec.md`):

1. Clean branch preflight.
2. Phase 0 read-only inspection (Matrix Phase 0 for safety-critical areas).
3. Patch plan.
4. Narrow patch.
5. Post-fix micro-audit.
6. Hard truthful local validation/tests.
7. Commit and push (gated: explicit flags or explicit owner request).
8. Wait until all GitHub checks finish on the new head.
9. Collect checks, annotations, review bodies, PR comments, inline comments,
   and unresolved threads.
10. Review triage.
11. If more patching is needed, repeat from Phase 0.
12. Final hard verify (`docs/hard_verify_spec.md`).
13. Report final status: READY_TO_MERGE, NEEDS_MANUAL, FAILED,
    CHECKS_PENDING or PATCH_REQUIRED_LOOP_STOPPED, with REASON.

Statuses that map to the automated PR flow (`docs/auto_pr_flow_spec.md`) are
always also emitted as machine-readable `AUTO_PR_FLOW_STATUS=<STATUS>`
tokens. Missing, ambiguous or contradictory evidence ALWAYS yields
`AUTO_PR_FLOW_STATUS=NEEDS_MANUAL` — never only a bare human-readable
status — while `BLOCKED` is reserved for the explicit "Stop conditions" of
this file.

The agent must not skip Phase 0, the post-fix micro-audit, hard truthful
tests, the check-completion gate, review triage, or final hard verify.

If any required step cannot be completed safely, stop and report
NEEDS_MANUAL, CHECKS_PENDING, or BLOCKED — and, whenever the outcome maps to
a flow status, also emit the machine-readable token (e.g.
`AUTO_PR_FLOW_STATUS=NEEDS_MANUAL`, `AUTO_PR_FLOW_STATUS=CHECKS_PENDING`).

The flow is NOT required for: questions, explanations, read-only analysis,
or work that does not touch PR code.

---

## Auto mode detection

Before starting work, determine the mode.

### Mode A — Current PR repair mode

Use this mode if any of these are true:

- The prompt mentions an existing PR number.
- The prompt mentions an existing PR branch.
- The current branch is already associated with an open PR.
- A handoff `.md` references an existing PR.
- The request is about failing checks, review comments, DeepSource,
  CodeRabbit, Sourcery, Gitar, GitHub Actions, or other feedback on an
  existing PR.
- The request says to continue on the same PR, same branch, or current PR.

Behavior:

- Continue on the existing PR branch.
- Do not create a new branch.
- Do not create a new PR.
- Do not merge outside the gated auto-merge policy.
- Do not work on `main`.
- Fix only the reported current-PR issues.
- Commit and push to the same PR branch.
- Report the commit SHA and new PR head SHA.

### Mode B — New task / new PR mode

Use this mode only if all are true:

- No open PR exists for the requested work.
- The request is a new task, not a fix for an existing PR.
- The task is explicitly provided by the repository owner, Codex Web, Codex
  CLI, Linear, Slack, Telegram, GitHub issue/comment, or another clear
  prompt/handoff.
- The work can be completed without violating scope and safety rules.

Behavior:

- Create a new branch from the correct base branch.
- Implement only the requested task.
- Register the task key in `.guardrails/allowed_scope.json` in the same PR.
- Create exactly one PR.
- Do not merge outside the gated auto-merge policy.
- Include a clear PR body with:
  - what changed
  - why it changed
  - safety impact (Betfair / dutching / money management / Telegram /
    config / database)
  - tests/checks run
  - limitations or follow-up notes
  - task identifier or source if provided

### Mode C — Blocked mode

Use this mode if:

- A different unrelated PR is already open and the request is trying to
  start a new task.
- The task requires unsafe files outside allowed scope.
- The requested work would require opening a second PR.
- The requested work would require working directly on `main`.
- The requested work would require merging outside the gated auto-merge
  policy without explicit owner instruction.
- The current PR branch cannot be determined safely.
- Git remote or credentials are missing and pushing/creating a PR is
  required.
- The task requires exposing secrets or credentials.
- The task requires real Betfair or Telegram credentials that are not
  available safely.
- The task would increase betting risk (stakes, limits, dedupe, safety
  gates) without explicit owner approval.

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
Do not create, delete, or reorganize task files unless the repository owner
explicitly requests it.

---

## Before editing

Always inspect first:

```bash
git status --short
git branch --show-current
git remote -v
git fetch origin main --quiet
```

Then identify:

- current branch;
- current task;
- whether this is a new task or current PR repair;
- files likely needed;
- files that must not be touched (`files_forbidden`, default forbidden list);
- safety-critical areas affected (Betfair, dutching, money management,
  Telegram, config/secrets, database, workflows).

If the current branch is `main`, create or switch to a proper task branch
before editing.

---

## Phase 0 read-only inspection

Before making any code change, the agent must perform a read-only Phase 0
(Matrix Phase 0 for safety-critical areas — see `docs/auto_pr_flow_spec.md`
§4). Phase 0 must not modify files.

Phase 0 must identify:

- requested task;
- detected mode: new task or current PR repair;
- current branch and whether it is `main`;
- open PR state, if any;
- files inspected;
- files likely to be changed;
- files that must not be changed;
- Betfair/dutching/money-management/Telegram/config/database/runtime
  behavior affected;
- safety risks;
- hard truthful test plan;
- stop conditions;
- last-5 merged PR sweep for unaddressed AI findings (see the AI review
  section), de-duplicated against existing Issues.

Required Phase 0 output:

```text
PICKFAIR_PHASE_0

Task:
- <requested task>

Detected mode:
- <New task / Current PR repair / Unknown>

Current branch:
- <branch>

Files inspected:
- <files>

Expected files to change:
- <files>

Forbidden files / artifacts:
- <files or patterns>

Safety risks:
- <duplicate order / stale signal replay / stake or limit change / weakened
  safety gate / token leak / config loss / schema break / race condition>

Patch plan:
- <smallest safe patch>

Hard truthful tests/checks:
- <commands>

Stop conditions:
- <conditions>

Last-5 merged PR sweep:
- <findings or none>
```

If Phase 0 cannot determine safe scope, the agent must stop with:

```text
AUTO_PR_FLOW_STATUS=NEEDS_MANUAL

Reason:
- Phase 0 could not determine safe scope.
```

---

## Implementation rules

- Make the smallest safe patch.
- Do not broad-refactor unless explicitly requested.
- Do not change business/trading behavior silently.
- Keep backward compatibility when possible.
- Prefer adding narrow helper functions over rewriting modules.
- Avoid adding heavy dependencies unless explicitly needed.
- Do not add external services unless explicitly requested.
- Do not add hidden network calls.
- Do not hide errors silently when they affect order, money, Telegram,
  config, or database safety.
- Use clear logging, but redact secrets (tokens, keys, session IDs).
- Avoid bare `except Exception: pass` in new code where the error affects
  safety; if a fail-safe except is genuinely needed, justify it.
- If changing parser behavior, include examples of accepted/rejected
  messages in tests or docs.
- If changing order/dutching behavior, include the expected computation and
  a worked example in tests or docs.
- Keep Linux compatibility (the runtime targets Linux; see
  `install_linux.sh`, `requirements-build-linux.*`).

---

## Documentation maintenance — required

Whenever you add, change, or remove code (function, class, module, behavior,
config key, database column, parser rule, gate, contract, roadmap entry),
you must update the corresponding documentation in the SAME PR. Docs must
never drift from code: a new function with no doc, or a removed one whose
doc still lingers, is an incomplete PR.

In the same PR, update — when applicable:

- `README.md` → user-visible changes or main flow;
- `CHANGELOG.md` → notable behavior changes;
- `docs/` domain docs (e.g. `docs/cashout_overview.md`) → changes to the
  documented domain;
- `docs/auto_pr_flow_spec.md` / `docs/hard_verify_spec.md` /
  `docs/ai_audit_workflows.md` → changes to the PR flow, verification, or
  review workflows;
- `ops/` runbooks → operational behavior changes;
- docstrings or technical comments → public functions, services, or
  non-trivial modules;
- new/removed config key, database column, gate → update the related docs;
- `docs/design/design_handoff.md` → any change touching the design/UI/UX
  aspect of the mini GUI (windows, tabs, controls, dynamic states/indicators,
  confirmation flows, color semantics, UI copy) — see the design handoff gate
  in CLAUDE.md; no design impact => N/A with a written reason.

Scope constraint (from CLAUDE.md and `hard_verify_spec.md` §12-bis): if the
doc to update is OUTSIDE `files_allowed`, do NOT force the scope (no commit
outside the allowlist) => report NEEDS_MANUAL or request an explicit
allowlist extension. The doc-update obligation applies only within
`files_allowed`. If no doc genuinely needs updating, state N/A with the
reason.

The post-fix micro-audit and final hard verify must include a
"docs updated: PASS/FAIL/N/A" check:

- PASS = documentation updated in the same PR;
- FAIL = code changed but documentation missing;
- N/A = purely internal change with no documentation impact — you must
  explain why.

---

## New task behavior

When no PR exists for the requested task:

1. Confirm the current branch is not `main` before editing.
2. Create a dedicated branch for the task.
3. Implement only the requested task requirements.
4. Register the task key in `.guardrails/allowed_scope.json` in the same PR.
5. Run relevant tests/checks.
6. Commit only relevant files.
7. Push the branch.
8. Create exactly one PR.
9. Do not merge outside the gated auto-merge policy.

The PR body must include:

```text
Summary:
- <what changed>

Reason:
- <why it changed>

Safety:
- <Betfair / dutching / money management / Telegram / config / database impact>

Tests:
- <commands run and results>

Scope:
- <files changed>

Notes:
- <limitations or follow-up>
```

If the task includes a task file path, task ID, Linear issue, GitHub issue,
Slack/Telegram handoff, or other identifier, include it in the PR body.

When there is already an open PR:

- If the requested work is unrelated to the open PR, stop and report
  `BLOCKED`.
- If the requested work is about fixing the open PR, continue on the same PR
  branch.
- If the request includes a handoff `.md` for the open PR, treat it as
  current-PR repair work.

---

## Current PR fix behavior

If the current request is about the currently open PR:

- Continue on the same PR branch.
- Push to the same PR branch.
- Do not create a new PR.
- Do not create a new branch unless explicitly required to recover from a
  broken local checkout.
- Do not merge outside the gated auto-merge policy.
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
- DeepSource findings.
- CodeRabbit comments.
- Sourcery comments.
- Gitar comments.
- Security/scanner feedback attached to the PR.
- Generated `.md` handoff files for the current PR.
- Telegram/Slack/Linear/GitHub handoff reports that clearly reference the
  current PR.

---

## Handoff file behavior

A handoff file may contain failing checks, logs, annotations, review
comments, and a section such as:

```text
FIX THESE ISSUES ONLY
```

When a handoff file is provided for the current PR:

- Treat it as the source of truth for the current PR fix cycle.
- Start from `FIX THESE ISSUES ONLY` if that section exists.
- Fix only the deduplicated issues listed there.
- Use failing checks, logs, annotations, and review comments as evidence.
- Ignore duplicated old monitor comments.
- Ignore PR Health Monitor / PR Telegram Notify / PR Codex Monitor
  self-check noise.
- Ignore Node.js/action deprecation warnings unless they are directly
  blocking the current PR.
- Do not chase unrelated style cleanup outside files touched by the PR.
- Do not refactor unrelated code.
- Do not modify unrelated tests.
- Do not change trading/business behavior unless the handoff/task explicitly
  requires it.

If the handoff conflicts with repository rules, follow repository rules and
report the conflict.

If the handoff asks to create a new PR while the current task already has an
open PR, do not create a new PR. Continue on the existing PR branch and
report that the handoff was interpreted as a current-PR fix request.

When a handoff file is provided and no PR exists:

- Treat it as a new task only if it clearly describes new work.
- Create one branch and one PR.
- Do not treat old PR logs as active work unless they clearly apply to the
  new task.

---

## PR behavior

- Create exactly one PR per task.
- If no PR exists for the task, create one PR after completing the requested
  work.
- If a PR already exists for the current task, continue working on that PR.
- Do not create a new PR for follow-up fixes.
- Do not create a new PR for review comment fixes.
- Do not create a new PR for failing check fixes.
- Do not create a new PR for DeepSource/static analysis fixes.
- Do not create a new PR from a handoff file that references the current PR.
- Merge only via the gated auto-merge policy; otherwise merge is manual and
  owner-only.

---

## Post-fix micro-audit

After patching and before running tests, committing, pushing, resolving
comments, or declaring completion, the agent must perform a post-fix
micro-audit.

The micro-audit must verify:

- only intended files were changed;
- no forbidden files were changed (`.github/workflows/*`, `core/*`,
  `services/*`, secrets, runtime trading, Betfair, Telegram live — unless
  explicitly allowed by the task spec);
- no real Betfair credentials, session tokens or certificates were added;
- no real Telegram token or chat ID was added;
- no `.env` or local `config.json` with real data was added;
- no database files, logs, caches, EXE/ZIP, build artifacts, or generated
  reports were added;
- no ungated auto-merge, default auto-push, or default auto-resolve was
  introduced;
- no live/real-money execution path was introduced or enabled by default;
- no broad unrelated refactor was introduced;
- parser behavior was not changed outside task scope;
- stake/dutching/money-management/limit behavior was not changed unless
  explicitly required;
- order dedupe / idempotency / reconciliation behavior was preserved unless
  explicitly changed;
- Telegram chat filtering was not weakened;
- config persistence and database schema compatibility were not broken;
- fail-closed behavior was preserved (no new fail-open branch);
- Linux runtime compatibility was preserved;
- tests were added/updated for changed behavior;
- documentation was updated for the change, or a note explains why none was
  needed;
- the design handoff (`docs/design/design_handoff.md`) was updated when the
  change touches the design/UI/UX aspect of the mini GUI, or a note explains
  why it has no design impact (see the design handoff gate in CLAUDE.md).

Required micro-audit output:

```text
POST_FIX_MICRO_AUDIT

Scope:
- PASS / FAIL

Forbidden files:
- PASS / FAIL

Secrets:
- PASS / FAIL

Betfair/order safety:
- PASS / FAIL

Dutching/money management safety:
- PASS / FAIL

Telegram safety:
- PASS / FAIL

Config/database safety:
- PASS / FAIL

Duplicate-order risk:
- PASS / FAIL

Fail-closed preserved:
- PASS / FAIL

Gated actions (push/resolve/merge):
- PASS / FAIL

Docs updated:
- PASS / FAIL / N/A
  (PASS = docs updated in the same PR · FAIL = code changed but docs missing ·
   N/A = purely internal change with no documentation impact, with a written
   reason)

Design handoff updated:
- PASS / FAIL / N/A
  (PASS = docs/design/design_handoff.md updated in the same PR when the
   design/UI/UX aspect changed · FAIL = design aspect changed but handoff
   stale · N/A = no design impact, with a written reason)

Result:
- PASS / FAIL

Notes:
- <evidence>
```

If the micro-audit fails:

```text
POST_FIX_AUDIT=FAIL

Reason:
- <why>

Action:
- Do not test.
- Do not commit.
- Do not push.
- Do not resolve review threads.
- Do not declare DONE.
```

The agent may only continue to tests/commit/push if:

```text
POST_FIX_AUDIT=PASS
```

---

## Hard truthful tests

Tests must be real, targeted, and verifiable.

The agent must never claim a test passed unless it actually executed the
command and observed a passing exit code.

Forbidden test behavior:

- Do not invent test results.
- Do not write tests that only assert `True`.
- Do not write tests that do not exercise real project functions.
- Do not mark tests as passed because they are "expected to pass".
- Do not hide failing tests with `|| true`.
- Do not skip tests without a written reason.
- Do not use fake coverage as proof.
- Do not claim live Betfair, live Telegram, or GUI behavior was tested
  unless it was actually tested.

Minimum local validation for Python changes:

```bash
python3 -m py_compile <changed_python_files>
```

Plus targeted tests:

```bash
python3 -m pytest -q <test_files> -k "<task selectors>"
```

If parser, order, dutching, or database behavior changes, add or update hard
targeted tests where practical. Hard tests should exercise real functions,
for example:

- parser functions with a valid signal message and with unsupported/empty
  input;
- odds/stake conversion and rounding;
- dutching stake distribution with real parsed data;
- order dedupe: repeated signals do not produce duplicate orders;
- invalid or incomplete signals do not create dangerous order instructions;
- reconciliation/recovery paths with stale state on disk;
- config load/save round-trip, corrupted-config backup, safe defaults.

Recommended test style:

- Use `tempfile` or pytest `tmp_path` for files and databases.
- Do not use real Betfair or Telegram credentials.
- Do not call live Betfair/Telegram APIs in normal unit tests (use the
  simulation broker / stubs: `simulation_broker`, `headless_ui_stubs`).
- Keep unit tests deterministic and offline.
- Cover PASS, BLOCK, MALFORMED and EDGE cases (see `hard_verify_spec.md`
  §6): for safety-critical tasks the BLOCK tests are the most important.
- If a live/manual test is needed, document it separately as manual
  verification.

Required hard test report:

```text
HARD_TEST_EVIDENCE

Commands run:
- <exact command>: PASS / FAIL

Exit codes:
- <command>: <exit code>

What was actually tested:
- <real behavior>

What was not tested:
- <live Betfair / live Telegram / GUI, with reason>

Test quality:
- REAL / PARTIAL / MANUAL_ONLY

Notes:
- <evidence>
```

If tests cannot be run:

```text
TESTS_SKIPPED

Reason:
- <exact reason>

Risk:
- <what remains unverified>

Required owner action:
- <manual command or environment needed>
```

A task cannot be DONE if the only evidence is unrun, fake, decorative, or
assumed tests.

---

## Mandatory hard safety tests for critical runtime behavior

For every change that touches runtime execution, START/STOP, the Telegram
listener, reconnect/backoff, order placement/cancel, cashout, dutching
computation, money management, signal queue, dedupe, daily limits,
reconciliation, config persistence, database schema, parser routing, or
shutdown behavior, the agent must automatically add or update serious
targeted tests before declaring the task complete.

The tests must exercise the real project functions/classes and must cover
the highest-risk failure modes that are practical to test offline:

- **crash / power-loss recovery**: stale local state (pending orders, queue
  entries) left on disk must be reconciled on next start before any new
  order can be placed;
- **connection loss**: reconnect/backoff policy, STOP during backoff, no
  retry on permanent errors, and stale Telegram messages older than the
  configured max age must not produce orders;
- **order lifecycle**: dedupe survives restart, rate/daily limits hold,
  queue timeouts remove expired signals, write failures roll back
  queue/dedupe/daily state so a signal can be retried safely, cancel/cashout
  paths do not leave orphaned state;
- **money management**: stake computation, liability caps and limits are
  enforced fail-closed; malformed inputs block instead of producing a
  partial bet;
- **config persistence**: existing config survives failed saves, corrupted
  config is backed up, defaults remain safe, no real credentials are
  committed;
- **database**: schema compatibility, consistent multi-step writes,
  rollback on failure;
- **runtime race conditions**: START failure must not leave the session
  active, STOP must tear down cleanly (`shutdown_manager`), expiry/manual
  clear/processing must be serialized, and no old Telegram poller may
  survive a new START epoch.

If a risk cannot be tested automatically because it requires live Betfair,
live Telegram, a real GUI session, or hardware reboot behavior, the agent
must add a deterministic offline unit/integration test for the pure logic
and also document an explicit manual smoke test with exact steps, expected
result, and what remains unverified. The agent must not claim the behavior
is covered unless the automated or manual test was actually run and reported
with real evidence.

---

## AI PR review — reviewers and final label gate

Every PR is covered by four AI review workflows (GitHub Actions driven by API
keys in the repo Secrets) plus CodeRabbit. Operational detail and security
posture live in `docs/ai_audit_workflows.md`.

- **GPT-5.6 Sol** and **Grok 4.6** run on every push. Output is TARGETED and short
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
  calling the model (zero cost); those are still covered by GPT-5.6 Sol/Grok.

### Reviewer cost: do NOT truncate, do NOT burn credits

Entry rule, because this is where nearly everyone gets it wrong:
**`max_tokens` / `MAX_OUTPUT_TOKENS` is a CEILING, not a charge.** You pay for
what the model actually GENERATES, never for the ceiling you allowed it.

- **Raising the ceiling is FREE** and removes truncation. A high ceiling on a
  review the prompt caps at 150 words costs not one extra token.
- **Lowering the ceiling saves NOTHING.** It does not reduce what the model
  generates — it cuts it in half. The result is a truncated review: full price,
  zero value. That is not a saving lever, it is a way of paying for nothing.

So when a review comes back truncated, **raise the ceiling**; do not shorten the
prompt and hope.

**Levers that actually save** (by yield): (1) never pay twice for the same range
— the per-range `done_marker` is already wired into all 4 workflows, do not
remove it and do not force a re-fire; (2) fewer pushes, not smaller ones — each
push pays TWO calls (GPT-5.6 Sol + Grok 4.6), so batch the fixes; (3) owner
authorization on the two labels (below) — the biggest lever, since Fugu and Fable
are the expensive pair; (4) low `reasoning_effort` where the model reasons —
reasoning tokens are billed as output, so Grok 4.6 (default `high`) is set to
`low` and GPT-5.6 Sol to effort `low`.

**Forbidden fake savings:** lowering output ceilings (above); tightening
`MAX_TOTAL_PATCH_CHARS` until the reviewer stops seeing the code (a reviewer that
cannot see is a false green, not a saving); disabling a reviewer to go faster. If
budget is the problem, cut the NUMBER of calls, never the quality of one.

### The two labels are ALWAYS applied — standing owner authorization

**Owner decision, 2026-09-16, REPLACING the earlier "only on owner
authorization, never unprompted".** `final-fugu-review` and `final-fable-review`
are applied to EVERY PR, always, without asking.

The reason is not cost, it is **gate legibility**. A PR whose labels are missing
is indistinguishable, looking at it on GitHub, from one where the gate was
skipped: the gate must be VISIBLE, not merely satisfied in fact. It is the same
ambiguity PRs #465-#468 removed elsewhere.

How: with the GitHub MCP tools, **one at a time**, and if a label is already
present **remove and re-add** it (GitHub emits no new `labeled` event for a
label already there). Keep the `manual-review-required` label the workflows add
on their own.

**What it costs: almost always nothing.** The `done_marker` is per range: if the
two strong reviewers already published for that range — which happens by itself
when the push touches critical files — the runs fired by the label event finish
`success` without calling the model. Measured on #468: labels applied, **exactly
one published review per reviewer**, zero extra spend. When the range is new,
the cost is the label round's, and it is budgeted: you pay for the gate, not for
waste.

It remains true that **fewer pushes cost less**: every push pays the per-push
reviewers, and one extra push on critical files also pays the two strong ones.
Batching fixes is still the real lever. Measured in one session: `$0.42` with a
single push (#468), `$1.35` with three (#467), `$1.81` with five (#466) — the
same order of work.

**The automatic critical-files fire stays.** When a push touches `core/`,
`services/`, `controllers/`, the root modules, dependencies, workflows,
config/secrets or the safety areas, Fugu and Fable fire on their own: that is
not agent initiative, it is the safety net. Do not disable it, do not work
around it.

**Repeat the fire until Fugu/Fable come back with NO blockers (owner decision).**
Every time the head changes (a fix, an alignment) another round is needed: declare
it, **get authorization**, re-fire the two labels on the new stable head and wait
for their full-range outcome. The gate is satisfied ONLY
when BOTH come back with no real blockers. A persistent false positive is NOT a
real blocker (see the diff-only note): answer it with evidence, do not loop
forever — if after the full-range fire only a structural false positive remains,
declare ready and document it. If Fugu/Fable are in usage-quota (the workflow
starts but the model does not answer), the OUT OF CREDITS rule applies: do not
merge, tell the owner credits are needed, stop the queue until they say
"prosegui".

**Diff-only / push-range vs full-range note (learned on #393).** The reviewers
are diff-only (no checkout, no execution). The **per-push** reviews (auto on every
push: GPT/Grok always; Fugu/Fable on core files) see ONLY the latest commit of the
range, so they can produce false positives on imports/consistency/"missing code"
when the cited code lives in earlier commits. The **label** reviews instead run
over the WHOLE PR range (`base…head`) and see the full diff, so they resolve those
false positives. For the final verdict what counts is Fugu/Fable's **full-range
label** review, not the per-push ones. Do not chase a push-range false positive
with a commit: re-fire the labels and read the full-range.

**Timing: the strong gates are the LAST pre-merge step.** Fire the two labels
when the PR is stable and in theory ready to merge: the per-push reviewers
(GPT-5.6 Sol, Grok 4.6) have COMPLETED and their real findings are handled (patched
or answered in-thread with evidence). CodeRabbit is NOT a waiting gate: if it has
completed handle its real findings; if it is in rate-limit/usage-quota it is
absent and is NOT awaited; if it is "processing" it is still reviewing — not
awaited as a binding gate, but its real findings (if they arrive before you
finalize) are handled, else deferred to post-merge. This way Fugu Ultra and
Fable 5 review a STABLE head and are not wasted on versions that will still
change (each push to the strong reviewers costs). Sequence: work complete →
push → GPT/Grok done and findings handled (CodeRabbit only if available) →
stable head → **deliver the merge-readiness verdict to the owner and WAIT for
authorization** → only then fire
`final-fugu-review` + `final-fable-review` → wait for the **full-range**
outcome → if real blockers remain: fix, re-push, **re-deliver the verdict and
request a NEW authorization** before re-firing the labels, repeat until
both come back clean → merge per the "Auto-merge" section.

**The agent never sees the API keys**: it only adds the label; secrets stay in
GitHub Secrets and Actions stays read-only on the code (diff-only, no checkout
and no execution of PR code, secret redaction).

**If a review reports blockers** (bugs, security, Betfair/dutching/money-
management risks, secret handling, workflow risks, or `manual-review-required`):
do NOT declare the PR ready and do NOT auto-merge. Leave the PR open and state
which blocker is still open and why. With blockers, auto-merge is forbidden
(fail-closed); otherwise auto-merge follows the gated policy in "Auto-merge
(owner-authorized, gated)" below.

**Who to wait for / not wait for.** Default coverage on every PR is the four API
workflows (GPT-5.6 Sol, Grok 4.6, Fugu Ultra, Fable 5) plus CodeRabbit. Codex,
Sourcery **and CodeRabbit** are NOT a waiting gate: if they post usage-limit /
rate-limit / usage-quota messages, treat them as ABSENT (not pending) — do not
wait, do not count them in the check-completion gate, do not block DONE on them.
Owner decision: an **advisory** reviewer (CodeRabbit, Codex, Sourcery) in
rate-limit/usage-quota is absent immediately, no wait and no cap-timer (see
"Skip on unavailability"). For the **four paid reviewers** "do not wait" does
NOT mean "merge anyway": you do not wait on a timer, but if one of them is dry
**on the current head** the PR is not merged — OUT OF CREDITS applies. The only
binding final gate is the two strong label reviewers (Fugu Ultra + Fable 5): see
"Final label gate".

**Fail-closed preserved (anti-regression note).** Downgrading unavailable ADVISORY
reviewers (CodeRabbit/Codex/Sourcery) to "absent" does NOT weaken fail-closed: they
are NOT required CI checks and do NOT replace the binding gates. The BINDING gates
are ALWAYS active and never skipped — (a) settled current-head CI checks and
(b) the two strong label reviewers Fugu Ultra + Fable 5 (full-range, repeated
until a clean outcome). Late findings from reviewers marked absent are covered by
post-merge tracking (Issue + fix PR). If a PAID reviewer is in usage-quota on the current
head — any of the four, not just Fugu/Fable via label — DONE is NOT declared by
skipping it: the OUT OF CREDITS rule applies (do not merge, tell the owner
credits are needed, stop the queue until they say "prosegui"). The owner can also always merge manually (human override).

**Event-driven review window (no fixed timer).** The four synchronous reviewers
answer in ~1 min. **CodeRabbit is NOT a waiting gate**: if it has already
COMPLETED, read and handle its real findings (inline + review body); if it is in
rate-limit / usage-quota, treat it as ABSENT (no wait, no cap-timer) and defer
to post-merge tracking. If it is "processing" it is still reviewing: not
awaited as a binding gate, but not "absent" either — if it completes before
you finalize handle its findings, else post-merge; do not stall on it. The
AGENT's verdict (ready / DONE) does NOT depend on CodeRabbit: it depends on
settled CI checks and the strong
label gates (Fugu/Fable). The owner may merge manually at
any time.

**Be frugal with pushes (API + CI cost).** Every push that updates the head pays
the models (GPT/Grok always; Fugu/Fable on core/critical pushes). Batch review
fixes into ONE push per round; never push for cosmetic cleanups or to chase
per-push-range false positives — answer those in-thread with evidence, not a
commit.

**Post-merge tracking + last-5 PR sweep.** Because there is no timed window, bot
comments can land after the merge: if a review event hits a closed PR, re-read
it and for each real/actionable finding open an Issue (PR number, head SHA,
file:line, bot, severity, comment link) and a dedicated fix PR branched from
the latest main (Phase 0 + micro-audit + hard PASS/BLOCK tests; never reuse or
stack on the merged PR). In Phase 0 of every task, sweep the last 5 merged PRs
for AI findings never addressed, de-duplicating against existing Issues (open
and closed). Fix-PR creation is deferred while another task/PR is active: the
one-active-task / one-open-PR rule wins — the Issue holds the findings until no
other PR/task is active.

**Skip on unavailability (usage-quota / rate-limit) — applies to ALL
reviewers.** A reviewer that cannot review is NOT a gate and is NOT "pending":
treat it as ABSENT and proceed (note that it did not review).

- **Codex**: usage-limit => absent, skipped.
- **Sourcery**: rate-limit => absent, skipped.
- **CodeRabbit**: rate-limit / usage-quota => absent, skipped (like Codex/
  Sourcery); do not wait, no cap-timer, defer to post-merge tracking. "processing"
  = still reviewing: NOT "absent" but NOT a binding waiting gate (DONE rests on
  settled CI checks + Fugu/Fable label); if it completes in time handle its real
  findings, else post-merge. If it has already completed, handle its real findings.
- **The 4 API workflows** (GPT-5.6 Sol, Grok 4.6, Fugu Ultra, Fable 5): these
  can NOT be downgraded to absent. If a round reports provider usage-quota /
  rate-limit, do not wait for it on a timer and do not count it in the
  check-completion gate (the check settles anyway), but **DONE stays blocked**:
  OUT OF CREDITS applies. The criterion is the **current head**, not the
  individual push: merge only if all four produced a real review of the head
  being merged (for Fugu and Fable, the full-range label round). Quota on an
  intermediate push blocks **nothing** if that reviewer then reviewed the final
  head; quota on the final head blocks, even if earlier pushes were clean.

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

## Check completion gate — required before final decisions

The agent must not perform final PR review, evidence resolve, thread resolve,
or final READY/DONE judgment while GitHub checks are still running.

Before any final decision, the agent must inspect all current-head checks and
statuses, including:

- GitHub Actions check runs;
- commit statuses;
- statusCheckRollup;
- DeepSource;
- CodeRabbit/Sourcery/Gitar if present (subject to the absence rules above);
- guard / merge readiness / PR flow guardrails;
- build and test workflows.

The agent must wait until ALL current-head checks are settled. Settled means
there are no checks/statuses in any of these states:

- PENDING
- QUEUED
- IN_PROGRESS
- WAITING
- REQUESTED
- EXPECTED
- UNKNOWN
- null / empty / unknown running state

This settlement requirement applies to the BINDING set: CI check runs and
commit statuses registered on the current head. Advisory reviewers that
publish only comments (CodeRabbit, Codex, Sourcery) are governed by the
reviewer-availability rules in the AI review section: usage-quota /
rate-limit means ABSENT (skip, no wait); CodeRabbit "processing" is a
comment-level state, not a check state — it never blocks settlement and
never authorizes skipping a real check run. Unknown or missing states of
binding checks remain fail-closed (not settled).

Read review findings, inline comments and review bodies **after** checks
finish — the bots (CodeRabbit/DeepSource/Sourcery/Gitar) often publish
only when their check completes. After EVERY push repeat the cycle: push =>
wait for checks => re-read checks + annotations + comments + inline + threads
=> triage => patch if needed. An intermediate monitoring status is allowed,
but it never counts as a final judgment.

If any check is still pending or in progress, the agent must stop the final
control phase and report:

```text
CHECKS_PENDING
AUTO_PR_FLOW_STATUS=CHECKS_PENDING

Reason:
- Some PR checks are still running.

Pending checks:
- <check name>
```

When checks are pending:

- do not mark the PR ready;
- do not resolve review threads;
- do not reply that findings are fully covered;
- do not declare final DONE;
- do not declare READY_TO_MERGE;
- do not merge;
- do not start a second PR;
- do not make random extra patches while waiting.

The final review pass must happen in this order:

1. confirm current PR head SHA;
2. wait for all checks to finish;
3. collect check results and annotations;
4. collect PR conversation comments;
5. collect review bodies;
6. collect inline review comments and unresolved threads;
7. classify findings (review triage);
8. patch only real current-head blockers;
9. run hard truthful local validation;
10. push if needed;
11. wait again for all checks to finish;
12. only then decide DONE, PARTIAL, NOT DONE, CHECKS_PENDING, or
    NEEDS_MANUAL.

A PR is not ready while checks are pending, even if local tests pass.

---

## CI minutes / push-churn — do not waste CI

Beyond reviewer API cost, every push and every re-run consume GitHub Actions
minutes. The repo is private → minutes are metered and capped by the spending
limit; exhausting it blocks ALL CI (jobs fail instantly, `runner_id: 0`, no
logs — not a code bug, it's billing).

Rules:

- batch fixes into a single push per round (no rapid successive pushes);
- never spam empty commits / re-runs — if a check is red, first diagnose
  (transient vs real) from the logs, re-trigger once, then verify before
  retrying;
- do not churn labels (remove+re-add) right after a push — this does not
  forbid firing the final-review gate: do that once, deliberately, when the
  head is stable; the rule bans repeated/reflexive remove+add while checks
  are still moving;
- if every check fails in ~2s with no logs and `runner_id: 0`, stop — that
  is the spending limit / billing, an owner action, do not keep pushing;
- prefer waiting on an in-progress check over re-triggering it.

---

## Review triage rules

For every review comment or inline thread, classify it as one of:

- **PATCH_REQUIRED** — active, non-outdated, valid issue on the current head
  that needs a code/test/docs fix (narrow patch).
- **TEST_REQUIRED** — code may already exist, but coverage is missing or
  unclear.
- **EVIDENCE_RESOLVE** — already fixed or outdated, but needs evidence.
- **SKIP_OUTDATED** — outdated and not applicable to the current head.
- **SKIP_DUPLICATE** — duplicate of another handled finding.
- **NEEDS_MANUAL** — unclear, risky, product decision, or outside safe scope.

Blockers = unresolved `PATCH_REQUIRED`, `TEST_REQUIRED` and `NEEDS_MANUAL`:
do NOT declare the work complete while any of them remain.

The agent must fix only active, non-outdated, non-resolved, current-head
issues. Do not chase stale, duplicate, resolved, or unrelated comments.

DeepSource is advisory by default: patch only if it is a required failing
check on the current head or demonstrates a real bug / safety issue /
fail-open branch.

### Inline comment handling

For inline review comments:

- Read the exact file and line referenced by the comment.
- Check whether the comment still applies to the current PR head.
- If the line changed, inspect the current equivalent code.
- If the issue still exists, patch the smallest safe fix.
- If the issue is already fixed, provide evidence instead of patching again.
- If the comment is outside the current diff or cannot be mapped safely,
  report NEEDS_MANUAL.

### Evidence before resolving

Before replying that a comment is fixed or already covered, the agent must
provide evidence:

- commit SHA;
- file path changed or inspected;
- relevant test command;
- real test result;
- explanation of why the issue is fixed, outdated, duplicate, or covered.

Never resolve or mark a review comment handled only because it "seems fixed".

### Resolving threads

Marking a GitHub review thread as resolved is a GATED external action. The
agent may resolve a thread only if ALL are true:

- `AUTO_RESOLVE_ENABLED=true` or the owner's explicit mandate covers it;
- all checks are settled for the current PR head;
- the thread is active and non-outdated;
- the related issue is fixed or demonstrably already covered;
- relevant tests/checks pass;
- the current PR head SHA matches the commit being reported.

If resolve permission is unavailable, reply with evidence but do not claim
the thread was resolved.

Never resolve a thread when: checks are still pending; tests are failing;
evidence is missing; the fix is only assumed; the comment is unclear; the fix
would require unsafe scope expansion; or the thread requires an owner/product
decision.

---

## Auto-merge (owner-authorized, gated)

The owner has authorized the agent to auto-merge the PR under work, but ONLY in
a gated way. This UPDATES/SUPERSEDES the earlier "never merge / auto-merge
disabled" statements: under the conditions below the agent MAY merge; outside
them, merge stays manual and owner-only.

**Conditions to auto-merge (ALL required, fail-closed):**

1. All current-head checks SETTLED and green (check-completion gate passed).
2. Zero blockers from the 4 AI reviewers (GPT-5.6 Sol, Grok 4.6, Fugu Ultra,
   Fable 5) and from CodeRabbit; CodeRabbit COMPLETED (or the ~15-min cap elapsed).
3. No `manual-review-required` label, no unresolved blocking thread, no open
   `PATCH_REQUIRED` / `NEEDS_MANUAL` finding.
4. The PR is "able to merge" on GitHub (mergeable, no conflicts, branch
   protection satisfied, not draft).
5. Hard verify PASS for the change; hard PASS+BLOCK tests actually run.

If ALL conditions hold, the agent merges and reports the merge SHA — **with no
distinction between safety-critical and other PRs** (owner decision,
2026-09-16). Right after the merge it moves on to the next PR in the queue,
re-establishing the designated branch from the updated `main` (the ONE OPEN PR
AT A TIME rule still holds).

What changed and what did NOT, stated precisely because the whole difference is
here: the **exclusion by file category** is gone; **no quality gate** is. All
five conditions above remain mandatory and fail-closed. The merge becomes
automatic because the gates are verified, not because less is checked.

**Safety-critical PRs are no longer excluded, but stay identifiable.** A PR is
safety-critical if it touches `core/`, safety areas of `services/`, money
management, `betfair_client`/`betfair_market_api`, `dutching*`, `order_manager`,
`safety_layer`, `reconciliation`, `runtime_controller`, `.github/workflows/*`,
config/secrets. On these the agent:

- DECLARES it in the verdict (`PR safety-critical: yes — <files/areas>`), so the
  owner always knows what was merged autonomously;
- applies the same gates with no discount: if even one does not hold, it does
  not merge and goes through the need-manual cycle;
- NEVER uses urgency or "it's green anyway" as a substitute for a missing gate.

**THE ONE EXCLUSION: whatever defines or enforces the gates themselves.** PRs
touching any of these files stay MANUAL owner merge. They are two layers of the
same thing — the prose that declares the gates, and the code that runs them.

The documents that define what the agent may merge:

- `CLAUDE.md`
- `AGENTS.md`
- `docs/auto_pr_flow_spec.md`
- `docs/hard_verify_spec.md`

The workflows that ARE the gates the auto-merge decision rests on:

- `.github/workflows/pr-review-openrouter-gpt56-sol.yml`
- `.github/workflows/pr-review-xai-grok46.yml`
- `.github/workflows/pr-review-openrouter-fugu-ultra.yml`
- `.github/workflows/pr-review-claude-fable5.yml`
- `.github/workflows/ci-quarantine-guard.yml`
- `.github/workflows/pr-guard.yml`
- `.github/workflows/pr-merge-readiness.yml`
- `scripts/guardrail_check.py`

`scripts/guardrail_check.py` is on the list for the same reason as the
workflows, and GPT-5.6 Sol is who put it there: "a gate that is independent and
**not modifiable within the delegated scope**". The `pr-guard` workflow was
already excluded, but it runs the script from the PR's own checkout: excluding
the container and leaving the content out would have closed nothing.

The reason is structural, not a risk category. If the agent could merge a change
to these files on its own, it could **progressively widen its own authority**:
each step individually gated, the cumulative effect unbounded. An authorization
that can rewrite itself is no longer the owner's authorization.

For the workflow gates the mechanism is the same, and how they run makes it
worse — by two opposite routes that land in the same place.

The four reviewers and `ci-quarantine-guard` run on `pull_request_target`, i.e.
from the **base** branch: a PR that weakens a reviewer is reviewed by the
**old** version of the workflow — the one it is removing. The control that
should stop it is exactly the one the PR takes out, and that only shows up on
the next push, once it is already on `main`.

`pr-guard` and `pr-merge-readiness` run on plain `pull_request`, i.e. with the
**PR's own** version: a PR that weakens them runs the already-weakened guard
against itself, and goes green because the control is no longer there.

The first case cannot see the change, the second one is subject to it. In
neither can the gate notice its own weakening — and that is what sets these
apart from any other safety-critical area.

The other workflows — build, packaging, tests, chaos, lockfile — are NOT
excluded: they do not decide whether a PR may be merged, they only verify it.
The agent merges those on its own under the gates above.

Flagged on #469 independently by GPT-5.6 Sol and Claude Fable 5 (the policy
files) and by Fugu Ultra (the workflow gates). Owner decision: exclude the
policy files and the seven workflow gates; everything else — `core/`, Betfair,
dutching, `order_manager`, config/secrets, and the rest of
`.github/workflows/` — the agent merges on its own under the gates above.

On these PRs the agent gets everything green and able-to-merge, declares it, and
leaves the merge to the owner.

**`.guardrails/allowed_scope.json` is NOT excluded — and the reason has to be
written down, because three reviewers out of four asked for it to be.** Sol,
Fugu and Fable, independently, on #469. The risk they name is real: the agent
registers its own scope there, so in principle it can widen it.

The exclusion is not the way to close it, for two measured reasons. First, on
its own it would buy nothing, because the caps that matter live **in code** and
not in the JSON — `_path_conflict_safety_gate` limits to `scripts/` +
`tests/scripts/` and denies `.github/workflows/`, `core/`, `secrets/`,
`config/` regardless. Second, **14 PRs out of 14** touch it, because this same
policy requires registering the task key in the same PR. Excluding it would mean
no PR is ever auto-merged — a delegation that looks granted and never applies is
worse than the risk it would close, because it stops being verifiable.

It is closed instead with an invariant **enforced by the `guard` check** — not
with a rule the agent applies to itself. The distinction is not formal: the
first version of this section wrote it as prose, and all four paid reviewers
blocked on the same point ("it is only prose", Grok; "leaving these constraints
to the agent's own declaration does not close the hole", Sol). They were right,
and not in the abstract: #463 had touched
`.github/workflows/pr-merge-readiness.yml` and `docs/auto_pr_flow_spec.md`
outside its declared scope, and was merged with nobody noticing.

The invariant:

- the task key's `files` **match the files the PR actually touches**, no more and
  no less. A declaration wider than the diff IS a scope widening: the PR becomes
  need-manual, however green everything else is;
- extending the `files` of your **own** task key mid-PR is allowed only if
  declared **explicitly** — in the verdict to the owner AND in the key's
  `description`. Never silently;
- modifying or removing **another** task key's entry, or the `default` section,
  rewrites the record of what was granted elsewhere: always need-manual, never
  auto-merge.

With the invariant, widening the declaration without widening the diff buys
nothing, and widening the diff is visible in the diff — which every gate already
reads.

The three points are verified by `scripts/guardrail_check.py`
(`validate_declared_scope` and `validate_registry_untouched_elsewhere`), which
`pr-guard.yml` runs on every push: the workflow also hands it
`allowed_scope_base.json`, the registry as of the base branch, so the comparison
does not depend on what the PR says about itself. Registry touched but base copy
missing => FAIL: a check you can silently skip is not a check. `max_files` stays
documentation and is not enforced: with `files` == diff imposed, the number of
files is already the number in the diff.


The per-issue override that used to lift the exclusion case by case is no longer
needed and is withdrawn: the authorization is global and lives here.

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

**OUT OF CREDITS => DO NOT MERGE, STOP AND TELL THE OWNER.** This is the only
exception to the autonomy, and it applies to the **four reviewers the owner pays
for**: GPT-5.6 Sol, Grok 4.6, Fugu Ultra, Fable 5. If any of them cannot review
because the provider answers usage-quota / rate-limit / out of credits — the
workflow starts but the model does not answer — then that PR **has not been
reviewed**, and a merge without review is not an authorized merge. The agent:

1. does NOT merge, even if everything else is green and able-to-merge;
2. tells the owner plainly: **which** reviewer is out, **which** PR is stuck, and
   that credits need topping up;
3. RECORDS in the dedicated issue that the reviewer did not review due to quota;
4. WAITS. The owner tops up and says "prosegui": only then does the agent re-run
   the review round on the current head and, if it comes back clean, merge.

Meanwhile the agent does **not open the next PR**: the queue stops, because going
on would mean stacking unreviewed work behind a stuck PR.

**A distinction NOT to blur.** Codex and CodeRabbit are not the owner's credits:
they are third parties on limited availability (Codex permanently usage-limited,
CodeRabbit stopped at `<10 stars`). They stay **absent immediately**, block
nothing and do not stop the queue — otherwise the queue would never restart. The
stop rule above concerns **only** the four API workflows.

---

## Final hard verify

Before declaring DONE, READY, or PARTIAL, the agent must perform the final
hard verify defined in `docs/hard_verify_spec.md`: task contract,
current-head, static audit in the authoritative files, PASS and BLOCK tests,
`py_compile`, targeted `pytest`, wiring into the final flow, clean scope,
fail-closed behavior, docs aligned with the change (§12-bis).

Final hard verify requires:

- Phase 0 completed;
- post-fix micro-audit passed;
- hard truthful local validation completed;
- all current-head GitHub checks completed (settled);
- failing checks triaged;
- review bodies, PR comments, inline comments and unresolved threads read;
- real findings patched or answered with evidence;
- design handoff (`docs/design/design_handoff.md`) updated when the
  design/UI/UX aspect changed, or N/A with reason;
- no blocking review comments left unevidenced;
- final labels fired and the strong reviewers' full-range outcome read;
- last-5 merged PR sweep done;
- no ungated merge.

Required final hard verify output:

```text
FINAL_HARD_VERIFY

Phase 0:
- PASS / FAIL

Post-fix micro-audit:
- PASS / FAIL

Hard truthful tests:
- PASS / FAIL / SKIPPED with reason

Hard tests created/updated for the change:
- PASS / FAIL / N/A with reason

Docs updated for the change:
- PASS / FAIL / N/A with reason

Design handoff updated for the change:
- PASS / FAIL / N/A with reason

GitHub checks completed (settled):
- YES / NO

GitHub checks result:
- PASS / FAIL / PENDING

PR comments checked:
- YES / NO

Review bodies checked:
- YES / NO

Inline comments checked:
- YES / NO

Unresolved threads checked:
- YES / NO

Final labels fired + strong reviewers (Fugu/Fable) full-range outcome read:
- YES / NO

Last-5 PR post-merge sweep:
- YES / NO

Safety invariants:
- PASS / FAIL

Merge:
- AUTO-MERGE (gated conditions met, non-safety-critical) / MANUAL OWNER

Implementation label:
- MISSING / PARTIAL / IMPLEMENTED_WITH_NOTE / FULLY_IMPLEMENTED /
  MERGED_BUT_NOT_FULLY_AUTOMATED

Final status:
- DONE / PARTIAL / NOT DONE / CHECKS_PENDING / NEEDS_MANUAL
```

"PR merged" alone is NOT proof of implementation. If any required final hard
verify item is missing, do not declare DONE.

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
- Do not merge while blockers remain.
- Address active, non-outdated, non-resolved comments (see "Review triage
  rules").
- If a comment is already outdated or already fixed by the current code,
  explain that clearly with evidence.
- For each comment actually fixed, reply in the related GitHub thread with:

```text
Fatto in commit <SHA>
```

If branch conflicts with base:

- Resolve conflicts in the same PR.
- Do not create a new PR.
- Do not merge outside the gated auto-merge policy.
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
- Do not include generated temporary files, logs, secrets, local caches,
  database files, CSV/report output, build artifacts, EXE/ZIP files, or
  unrelated artifacts.
- Never include real Betfair credentials, session tokens, certificates,
  Telegram tokens, chat IDs, `.env`, or a `config.json` with real data.
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
- Phase 0 has passed.
- The post-fix micro-audit has passed.
- Hard truthful local tests have passed or are explicitly skipped with a
  real reason.
- All current-head checks have completed (settled).
- Relevant checks have passed or are clearly outside scope.
- Betfair/dutching/money-management/Telegram/config/database safety impact
  is explained.
- Blocking review comments are resolved, outdated, or explicitly handled
  with evidence.
- The final label gate has been fired and the strong reviewers' full-range
  outcome read.
- The final hard verify has been performed.
- The PR is ready for owner review (or auto-merged under the gated policy).

Do not mark work complete while checks are failing.
Do not mark work complete while checks are pending.
Do not mark work complete while active review comments remain unresolved.
Do not mark work complete after only editing files without running at least
`py_compile` for Python changes.
Do not mark work complete with fake, assumed, or decorative tests.
Do not move files to `ops/tasks_done/` unless explicitly instructed by the
repository owner.

---

## Scope control

- Modify only files required by the task, checks, review comments, or
  handoff file.
- Do not refactor unrelated code.
- Do not expand scope.
- Do not change trading/business logic unless explicitly required.
- Do not change parser/order/dutching/money-management behavior unless
  explicitly required.
- Do not make broad cleanup changes unless necessary for the current task.
- Do not modify unrelated tests.
- Do not modify CI configuration unless the task or failing check
  specifically requires it.
- Do not silence tests or checks just to make the PR green.
- Do not delete tests unless the task explicitly requires it and the reason
  is documented.
- Do not remove guardrails or safety gates.
- Do not bypass security/static analysis findings by ignoring them without
  justification.
- Do not add real secrets or sample secrets.

---

## Stop conditions

Stop immediately and report `BLOCKED` if:

- A different unrelated PR is already open and the current request is trying
  to start a new task.
- The task requires files outside the allowed scope.
- The conflict cannot be resolved safely.
- Tests cannot be fixed without violating scope rules.
- The requested work would require opening a second PR.
- The requested work would require working directly on `main`.
- The requested work would require merging outside the gated auto-merge
  policy without explicit owner instruction.
- The requested work would require disabling project guardrails or safety
  gates (safety_layer, circuit_breaker, safe_mode, reconciliation).
- The requested work would require exposing secrets or credentials.
- The task requires real Betfair or Telegram credentials that are not
  available safely.
- The task would increase betting risk (stakes, limits, dedupe, price
  validation) without explicit owner approval.
- The task would produce malformed or partial order instructions.
- The requested mode cannot be determined safely.

Do not stop if:

- The task is to fix the currently open PR.
- The task is triggered by GitHub review comments on the currently open PR.
- The task is triggered by failing checks on the currently open PR.
- The task is triggered by static-analysis feedback on the currently open PR.
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
- Do not merge outside the gated auto-merge policy.
- Report what changed and provide the commit SHA.

---

## Required response format after creating a new PR

After completing a new task and creating a PR, respond with:

```text
DONE / PARTIAL / NOT DONE / CHECKS_PENDING / NEEDS_MANUAL
AUTO_PR_FLOW_STATUS=<STATUS — mandatory when the outcome maps to a flow
status, e.g. NEEDS_MANUAL / CHECKS_PENDING>

Summary:
- <what was changed>

Branch:
- <branch name>

PR:
- <PR URL or number>

Commit:
- <commit SHA>

Safety:
- <Betfair / dutching / money management / Telegram / config / database impact>

Phase 0:
- PASS / FAIL

Post-fix micro-audit:
- PASS / FAIL

Hard truthful tests:
- <command run>: pass/fail/skipped with reason

GitHub checks:
- complete/pass/fail/pending with reason

Review comments handled:
- <thread/comment URL or summary>: fixed/skipped/needs manual with evidence

Files changed:
- <file path>

Files created:
- <file path>

Final hard verify:
- DONE / PARTIAL / NOT DONE / CHECKS_PENDING / NEEDS_MANUAL

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
DONE / PARTIAL / NOT DONE / CHECKS_PENDING / NEEDS_MANUAL
AUTO_PR_FLOW_STATUS=<STATUS — mandatory when the outcome maps to a flow
status, e.g. NEEDS_MANUAL / CHECKS_PENDING>

Summary:
- <what was changed>

Commit:
- <commit SHA>

New PR head SHA:
- <new PR head SHA>

Safety:
- <Betfair / dutching / money management / Telegram / config / database impact>

Phase 0:
- PASS / FAIL

Post-fix micro-audit:
- PASS / FAIL

Hard truthful tests:
- <command run>: pass/fail/skipped with reason

GitHub checks:
- complete/pass/fail/pending with reason

Review comments handled:
- <comment/thread URL or summary>: fixed in commit <SHA>; evidence: <test
  command PASS>
- <comment/thread URL or summary>: skipped because <reason>; evidence: <file/test>
- <comment/thread URL or summary>: needs manual because <reason>

Files changed:
- <file path>

Final hard verify:
- DONE / PARTIAL / NOT DONE / CHECKS_PENDING / NEEDS_MANUAL

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

## Required response format when checks are pending

```text
CHECKS_PENDING
AUTO_PR_FLOW_STATUS=CHECKS_PENDING

Reason:
- Current-head PR checks are not all finished yet.

Current head:
- <SHA>

Pending checks:
- <check name>

Next allowed action:
- Wait for checks to complete, then re-read checks, annotations, review
  bodies, inline comments, unresolved threads, and only then decide final
  status.
```

---

## Automation-specific rules

Automated agents, Codex CLI, Codex Web, self-hosted runners, Slack, Telegram,
Linear, and GitHub Actions must all follow this file.

For new-task automation:

- Run only when no unrelated PR is open.
- Create one branch.
- Create one PR.
- Do not merge outside the gated auto-merge policy.
- Stop after PR creation and notify the owner.

For automated PR repair loops:

- Use the handoff file as input.
- Apply only current PR fixes.
- Make at most one fix commit per automation attempt.
- Push only to the current PR branch.
- Let GitHub checks run after push.
- Wait until checks complete before final review/comment/inline triage.
- If checks remain red, generate or wait for a new handoff and run another
  controlled attempt.
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
