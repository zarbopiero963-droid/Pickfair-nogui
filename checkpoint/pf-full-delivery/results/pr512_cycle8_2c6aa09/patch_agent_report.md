All work is complete; nothing further to request.

## Report — PR #512, 10 Codex findings on 034af59 (branch `lots/pf-codex-034af59-c8`, local only, never pushed)

Worktree: `/tmp/claude-0/-home-user/72973979-7da3-59f6-82b8-954d718e6349/scratchpad/wt_c8` (clean, 10 commits on top of 034af59).
Patches: `/tmp/claude-0/-home-user/72973979-7da3-59f6-82b8-954d718e6349/scratchpad/wt_c8_patches/0001…0010-*.patch`.
New test files: `tests/test_pr512_codex_034af59.py` (findings 1–7, 10), `tests/traceability/test_pf_trace_codex_034af59.py` (8), `tests/agent_checkpoint/test_pfckpt_codex_034af59.py` (9).

### Per finding

**1. betfair_service `_well_formed_current_orders`** — RED: `7 failed, 1 passed` (e.g. `_rebuild_open_positions` → `{'ok': True, ... 'closed': ['r1']}`). Fix `services/betfair_service.py:736-744`: each row must carry non-empty `betId`/`bet_id` AND `marketId`/`market_id`, else `CURRENT_ORDERS_MALFORMED` (→ `EXCHANGE_ORDERS_UNAVAILABLE` → PAUSED). GREEN: 8 passed; unit/f018/recovery/reconciliation suites 333 passed. No existing test adapted.

**2. microstake_sim_port snapshot finale** — RED: `assert 'SUCCESS' == 'AMBIGUOUS'`, `recover_pending() == {}`. Fix `core/microstake_sim_port.py`: `port.last_snapshot_persisted` (`_snapshot` returns bool, :218-229), `hold_reservation` (:212), `_demote_unpersisted_active` (:363) persists AMBIGUOUS + `reservation_held`, returns AMBIGUOUS/`order_unknown` without `instructionReports`; `recover_pending` (:395) re-attempts the snapshot and promotes nothing while it fails. GREEN: 4 new tests; microstake+pr512 suites 253 passed. **Adapted tests (declared):** `test_pr512_recovery_fixes.py::test_snapshot_failure_is_declared_in_the_executor_result` encoded the old ACTIVE result (now asserts AMBIGUOUS); fixtures `_engine()`/`_DB` in `test_microstake_engine_native.py`, `_engine_real_db` in `test_pr512_codex_findings.py`, `new_broker` in `test_pr512_review_cycle4.py` built a `SimulationBroker` without DB, where `persist_snapshot()` returns False (no reload source) — a configuration production cannot reach the executor with (`DbSagaStore` needs the same DB); they now pass the DB. Doc: `docs/microstake_native.md`.

**3. dutching `_leg_liquidity_shortfall`** — RED: `TypeError: unexpected keyword 'warning_only'` + `_evaluate_liquidity_guard` `assert False is True`. Fix `controllers/dutching_controller.py:544-580, 602-608`: `warning_only` kw (default True); block mode + unknown liquidity → `{"reason": "LIQUIDITY_UNKNOWN", "available": None, ...}`; warning-only unchanged; cold book cache still fail-open. GREEN: 9 passed; dutching suites 350 passed. **Adapted:** `tests/integration/test_dutching_liquidity_guard.py::test_fail_open_when_selection_absent_block_mode` encoded the fail-open on absent selection in block mode (exactly the finding) → replaced by two explicit tests (block vs observe).

**4. microstake_safe `recover()` ROLLBACK_PENDING** — RED: `Phase.ACTIVE is Phase.ROLLED_BACK` (recover re-replaced). Fix `core/microstake_safe.py:580-593`: `ROLLBACK_PENDING` → `cancel_all_remaining` → ROLLED_BACK (reservation released), failure → AMBIGUOUS `rollback_unknown` held; placed after the matched-stub check (no widening). GREEN: 219 passed in microstake suites. No adaptation. Doc: `docs/microstake_native.md`.

**5. runtime_controller reset persist failure** — RED: `KeyError: 'persisted'`, `_recovery_persist_failed False`. Fix `core/runtime_controller.py`: `_declare_recovery_persist_failure(source)` (:4026) sets flag, logs, publishes `RECOVERY_STATE_PERSIST_FAILED {"source": ...}`; used in `reset_cycle` (:2674, adds `persisted: bool`) and `_maybe_auto_reset` (:4082). GREEN: 504 passed (f026 units + tests/core); `test_f026_financial.py` 38 passed, 1 xfailed. Doc: `docs/roserpina_bankroll_equity.md`.

**6. runtime_controller unreadable recovery state** — RED: persist overwrote (`True is False`), resume → ACTIVE, harness start → `'ACTIVE' == 'PAUSED'`. Fix: `_gate_rebuild_on_recovery_state` (:4024) forces rebuild `ok=False`, reason `RECOVERY_STATE_UNREADABLE` in `start()` (:2530) and `resume()` (:2634); `_persist_recovery_tables` (:4042) refuses with False while the flag is set. GREEN: 576 passed; harness test in the F6 group passed; `tests/f026` fully green in the broad run. Doc: `docs/roserpina_bankroll_equity.md`.

**7. microstake_safe docstring** — RED: docstring contains "no import or call site". Fix: module docstring states the real role (imported by `core/microstake_sim_port.py`, reached from `TradingEngine` for SIM runtime submissions, LIVE fail-closed). Doc-only + guard test. GREEN.

**8. pf_trace latest evidence** — RED: `11 failed, 5 passed` (BLOCKED/NOT RUN/NOT VERIFIED after PASS accepted). Fix `tools/traceability/pf_trace.py:131-135`: latest evidence must be PASS. GREEN: `tests/traceability` 33 passed (real registry still validates). No adaptation. (My fixture avoids FAIL for VERIFIED/CERTIFIED because the pre-existing "FAIL senza finding" rule is a separate error.)

**9. pfckpt verify test sha** — RED: `DID NOT RAISE CheckpointError` for fabricated shas. Fix `tools/agent_checkpoint/pfckpt.py:240-246`: every PASS/FAIL `tests_executed[].sha` resolved via `git cat-file -e <sha>^{commit}` with the `code.sha` lookup; unresolved → `test sha unresolved: <sha>`. GREEN: `tests/agent_checkpoint` 46 passed. No adaptation. Doc: `docs/agent_checkpoint_protocol.md`.

**10. money_management unknown growth** — RED: thresholds 0/-1 and NaN/inf → `DeskMode.EXPANSION`, stake 2.5. Fix `core/money_management.py:138-145`: growth None → return `DeskMode.NORMAL` before any threshold comparison. GREEN: 539 passed (tests/core + f026 units). No adaptation. Doc: `docs/roserpina_bankroll_equity.md`; CHANGELOG entry added in this commit.

### Broader suites
`tests/f026 tests/core tests/unit tests/test_pr512_*.py tests/test_microstake_*.py tests/agent_checkpoint tests/traceability tests/safety tests/acceptance`: **2888 passed, 1 skipped (Tk/X11), 2 xfailed (pre-existing), 0 failed** in 7:48 (log: `/tmp/claude-0/-home-user/72973979-7da3-59f6-82b8-954d718e6349/scratchpad/wt_c8_broad.log`).

### Commits (034af59..HEAD)
- d8cb2a4 prep c8 #1: currentOrders senza betId/marketId => CURRENT_ORDERS_MALFORMED
- 7faf7fa prep c8 #2: snapshot finale SIM non persistito => saga AMBIGUOUS, mai SUCCESS/ACTIVE
- 2082fe7 prep c8 #3: dutching, liquidità ignota in modalità blocco => LIQUIDITY_UNKNOWN
- 39870de prep c8 #4: recover() su ROLLBACK_PENDING cancella il residuo, mai replace
- 820db61 prep c8 #5: reset_cycle/auto-reset con persistenza fallita => fail-closed dichiarato
- 9828c27 prep c8 #6: stato di recovery illeggibile al riavvio => PAUSED, mai sovrascritto
- ad3209f prep c8 #7: docstring di core/microstake_safe.py veritiera
- 027138f prep c8 #8: pf_trace, l'ultima evidenza deve essere PASS
- 69f73cb prep c8 #9: pfckpt.verify risolve ogni tests_executed[].sha come code.sha
- 8b77404 prep c8 #10: crescita ignota => NORMAL prima del confronto con la soglia; CHANGELOG

Not touched: `.guardrails/allowed_scope.json`, `AGENTS.md`, `CLAUDE.md`, `.github/`, `/home/user/Pickfair-nogui`. Note for the scope registry: the task key's `files` will need to cover the 20 touched files when the PR is actually updated (deliberately left to the owner-authorised cycle-8 step).