## Review of `f62f0e3..a8dbbd2` (PR #512, ciclo 7)

Repo tests `tests/test_pr512_codex_f62f0e3.py`: **7 passed**. Related suites (`tests/f026`, `tests/f001`, `tests/test_pr512_*`): **277 passed, 1 xfail (pre-existing)**. Findings in severity order.

### 1. P1 — The P1-3 fix rejects the *legitimate* baseline written by the very reset that set it (CONFIRMED by running code)

`core/runtime_controller.py:2668-2669` (and auto-reset `:4029-4032`) run `_start_expansion_cycle(...)` **before** `_persist_recovery_tables(cycle_reset=True)`. The baseline gets `set_at_ts = time.time()` (`:3911`), then the marker gets a *later* `at_ts = time.time()` (`:3991`, measured delta ≈ 0.4 ms). On restart `_load_expansion_baseline` (`:3941-3946`) evaluates `set_at_ts < reset_ts` → **true** → baseline discarded, `_expansion_baseline = None`, and since the method returns `True`, `start()` (`:2487-2488`) does not open a new cycle either. Net effect: after any `reset_cycle`/auto-reset followed by a restart, expansion is dead until the next explicit transition — and that one dies on the next restart again. Fail-closed for money (no expansion), but a functional regression that disables an owner-decided feature (F032) and makes the CHANGELOG claim (3) misleading. Throwaway proof: `/tmp/claude-0/-home-user/72973979-7da3-59f6-82b8-954d718e6349/scratchpad/review_c7/test_review_c7.py` — `test_real_reset_cycle_then_restart_keeps_the_fresh_baseline` and `test_auto_reset_sequence_then_restart_keeps_the_fresh_baseline` both FAIL (`Obtained: None`, log: `baseline (1792318849) precedente all'ultimo reset durevole (1792318849)`). Fix shape (not applied): write the marker before the baseline, or use one shared timestamp/cycle token for both writes in the same `reset_cycle` call.

### 2. P1 — Test gap that let #1 through (CONFIRMED)

`tests/test_pr512_codex_f62f0e3.py:89-95` (`test_baseline_newer_than_the_last_reset_survives_restart`) calls `_persist_recovery_tables(cycle_reset=True)` **first** and `_start_expansion_cycle` **second** — the opposite of the production order in `reset_cycle`/`_maybe_auto_reset`. It therefore pins an implementation detail (ordering of two private calls), not the invariant ("a reset's own baseline survives restart"), and passes against the buggy code. No existing test in `tests/f026/test_f032_rsr12_units.py` does `reset_cycle()` → new controller → `_load_expansion_baseline()` either. The other 6 tests do pin their behaviour (verified by reading: revert of P1-1 would rebuild `r1` as BACK with exposure 3.0; revert of P1-2 gives `resets == 1`/no key; the "stale" test fails on old code because `set_at_ts` was never written).

### 3. P2 — Corrupt/unparseable reset marker is treated as "no reset" (CONFIRMED)

`_last_durable_cycle_reset_ts` (`:3952-3959`) returns `None` on any exception (e.g. `{"at_ts": "garbage"}`), so the staleness check is skipped and the old baseline is accepted (throwaway `test_corrupt_marker_is_ignored_fail_open`: baseline 50.0 loaded). Inconsistent with the same function's posture for a corrupt *baseline* state ("corrotto: nessuna espansione"). Low likelihood, but it is the one fail-open edge in the new path; should return a sentinel that disables expansion.

### 4. P2 — Docs not updated for new persisted state (PLAUSIBLE, by grep)

New settings key `roserpina.cycle_reset_state`, new field `set_at_ts`, and new `reset_cycle()` result key `sim_reset_skipped` appear only in `CHANGELOG.md`; `docs/roserpina_bankroll_equity.md:82-83` describes `roserpina.expansion_baseline_state` ("Sopravvive al riavvio") without the staleness rule. CLAUDE.md requires docs in the same PR (within files_allowed — `docs/` is in the task key's description scope).

### 5. NIT — P1-1 scope check: no legitimate caller broken (CONFIRMED by grep)

Order rows are persisted only after `order_manager.py:437/460/520` normalises `bet_type` to exactly `BACK`/`LAY` (raises otherwise), so `.upper()` + membership check at `:3608/:3617` cannot reject a real row. Lowercase handled (parametrised test). Note the pre-existing sibling default at `core/runtime_controller.py:4177` (`meta.get("bet_type", "BACK")` in the stop-loss estimate) is outside this finding and untouched — worth a follow-up issue, not this PR.

### Verified OK (no finding)
- **P1-2**: `retained` is computed after `_reset_reservations_keeping_uncertain`, so it is exactly the set of tables kept with a live/uncertain position; skip is SIM-only (`simulation_mode and hasattr(...)`), LIVE path unchanged; counters (`_reset_recovery_cycle_keeping_losses`), expansion baseline (reads the un-reset SIM funds, hence consistent with retained exposure), persistence and `RUNTIME_CYCLE_RESET` publish all still run; `sim_reset_skipped` is `[]` in LIVE. Only caller `mini_gui.py:2213` ignores the result dict. 
- Marker is written in the same `save_settings` transaction as `_RECOVERY_KEY` (`database.py:416-419` uses one `transaction()`), in both `reset_cycle` and `_maybe_auto_reset`; the third `_persist_recovery_tables()` caller (`:4380`) correctly does not write the marker.
- Edge cases: equal timestamps accepted (`<`); clock stepping backwards between the two writes yields acceptance (safe); legacy baselines without `set_at_ts` → 0.0 → rejected only once a marker exists (acceptable, superseded by the reset that wrote the marker — modulo #1).
- Scope: 4 files, all three findings only; `.guardrails/allowed_scope.json` adds the new test file and the declared extension; `max_files` 77→78 consistent.

**Verdict:** not mergeable as-is — #1 is a real current-head defect (`CURRENT_DEFECT`, fail-closed but feature-breaking) introduced by this commit and masked by the reversed-order test (#2); #3 is a small fail-open gap in the new code.