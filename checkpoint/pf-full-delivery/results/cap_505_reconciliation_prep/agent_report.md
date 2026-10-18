Done. Everything stayed local in the worktree `/tmp/claude-0/-home-user/72973979-7da3-59f6-82b8-954d718e6349/scratchpad/wt_cap505` (branch `lots/pf-cap-505-reconciliation`, 3 commits on top of `618884e`); no push, no GitHub, `/home/user/Pickfair-nogui` untouched, no `.guardrails`/AGENTS/CLAUDE/.github changes.

## Patches
`/tmp/claude-0/-home-user/72973979-7da3-59f6-82b8-954d718e6349/scratchpad/wt_cap505_patches/`
- `0001-…` = `550bc16` PF-CAP-01..04 + PF-CAP-10 code + new test file
- `0002-…` = `cb6388e` PF-CAP-10 adaptation of the existing pr28d test
- `0003-…` = `6eb8e43` PF-CAP-11 docs

(CAP-10 code is in commit 1 together with CAP-01..04 because it is the same loader call; splitting would have left commit 1 with failing tests.)

## RED → fix → GREEN
New file `tests/acceptance/test_issue437_pr28d_rectification.py` (59 tests, real chain: `SettingsService` + SQLite → `RuntimeController.reload_config()` → real MM → `TradingEngine` → `SimulationBroker`).

**RED on `618884e`: 32 failed, 27 passed.** Key lines:
- `test_fresh_config_without_cap_keys_loads_all_four_as_none` → `assert 1.0 is None`
- `test_sim_bankroll_100_price_2_mm_stake_3_is_submitted_without_any_cap` → rejected `max_order_exposure_exceeded:limit=1.0€`
- `test_absent_drawdown_cap_does_not_lockdown…` (peak 100, bankroll 88 = 12 € dd, < 20 % lockdown) → head forced LOCKDOWN from the 10 € default
- `test_blank_or_null_persisted_value_means_not_active[""/"   "/None]` → `assert nan is None`
- `test_gui_initial_cap_fields_are_blank` → `'1.0' == ''`
- `test_legacy_prefixless_key_is_read_and_logged` → `10.0 == 7.0` (legacy key ignored, default used)
- `test_invalid_present_persisted_value_blocks_new_orders[*-max_market/event]` → head returned `max_order_exposure_exceeded` (default 1 € fired first) instead of `<name>_non_valido`

**Fix** (minimal, no runtime_controller/validators change — verified None ⇒ `absolute_cap_reason` returns "" and `_configured_optional` None ⇒ drawdown branch skipped, both in `_on_signal_received` `core/runtime_controller.py:3325-3368` and auto-next `:4697-4726`; NaN/≤0 ⇒ `<name>_non_valido`, confirmed by tests on both paths):
- `services/setting_service.py:327-351` the four keys → `_optional_hard_stop_value(..., fallback_key=<prefix-less>)`; `_required_absolute_cap_value` removed (no other callers, grep-verified); `:447-485` docstring, legacy-key `logger.info`, plus `bool ⇒ NaN` (previously `float(True)=1.0` would have read a bool as a 1 € cap — note this branch also applies to the other six optional hard-stops; all suites green, strictly more fail-closed); `logging` import + module logger.
- `core/system_state.py:69-74` comment only (fields were already `Optional[float] = None` on the #505 head).
- `mini_gui.py:704-709` initial vars `""`; `:1130-1133` labels `"… (€, vuoto = non attivo)"`; parsing unchanged via `_parse_hard_stop`.

**GREEN:** new file 59 passed; `tests/acceptance/test_issue437_pr28d.py` 69 passed.

## Adapted existing tests (only one)
`tests/acceptance/test_issue437_pr28d.py` — `test_blank_persisted_absolute_cap_stays_configured_and_fails_closed` → renamed `test_blank_persisted_absolute_cap_is_not_active_but_unreadable_fails_closed`: `""` ⇒ `None` (owner rule: blank = not active), and a present unreadable text ⇒ NaN (fail-closed kept). All tests that configure caps explicitly (1.0 / 2.0 / 10.0) are unchanged. No test on this head asserted the 1/2/2/10 defaults; `tests/integration/test_runtime_controller_auto_trade_mm.py` sets caps explicitly, unchanged.

## Suite counts (after patches)
- tests/acceptance: 570 passed, 1 skipped
- tests/core + tests/unit: 1853 passed, 1 xfailed
- tests/integration (DISPLAY=:99): 671 passed, 7 skipped
- tests/services, guardrails, failure, safety, controllers, parsers, smoke, invariant, recovery, reconciliation: 1669 passed, 7 skipped
- `py_compile` on all changed .py files OK.

## Items not applicable on this branch (stopped, not forced)
- `tests/f001` (incl. `child_runtime.py:23-30` neutralisation), `tests/f026` and `docs/roserpina41_parameter_dossier.md` (RSR-38..41) **do not exist on #505 head `618884e` / base `028cee9`** — they live on #512. Nothing to remove/adapt here; the dossier item is flagged in commit 3's message for the branch where it exists.
- Protections untouched as required: `_market_current_exposure`, `_event_current_exposure` canonical identity, `_release_if_terminal`, admission lock, cashout barrier, LOCKDOWN semantics of `max_drawdown_abs` when configured, MM percentages.