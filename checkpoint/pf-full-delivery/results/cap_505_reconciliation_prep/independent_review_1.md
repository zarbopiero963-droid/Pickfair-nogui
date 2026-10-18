# Adversarial review — lots/pf-cap-505-reconciliation (550bc16, cb6388e, 6eb8e43 on 618884e)

Scope read: full `git diff 618884e HEAD`; runtime consumers in `core/runtime_controller.py`
(`_on_signal_received` 3309-3382, auto-next 4690-4731, `_configured_optional` 2658,
`_validate_live_hard_stop_config` 1477-1520, `reload_config` 1932), `core/validators.py`
(`absolute_cap_reason`, `finite_number`), `services/setting_service.py` load/save,
`mini_gui.py` `_parse_hard_stop`/`_hard_stop_to_str`/save/post-save reload, other
hard-stop consumers (`core/risk_stop.py`, `core/loss_limits.py`, `core/money_management.py`).
Tests run: 193 passed (rectification 59 + pr28d + auto_trade_mm integration + mm_gate +
live hard-stop safety). Probes: 35 passed
(`/tmp/claude-0/-home-user/72973979-7da3-59f6-82b8-954d718e6349/scratchpad/review_cap505/test_probe_cap505.py`).

## ERRORE 1 — CAP NASCOSTO

**E1-a (MEDIUM, PLAUSIBLE — contract/label mismatch, not a hidden default).**
`mini_gui.py:1132-1135` labels read "(€, vuoto = non attivo)", but the save path
(`mini_gui._parse_hard_stop` → None → `services/setting_service.py:446-449` skips None)
never clears a previously persisted cap. Scenario: user sets 1 € once, later blanks the
field expecting "non attivo" → cap stays 1 € and rejects every MM stake > 1 €
(`max_order_exposure_exceeded:limit=1.0`). Probe `test_gui_blank_save_cannot_deactivate_a_persisted_cap`
confirms the behaviour. The GUI re-shows the persisted value after save (`mini_gui.py:2088-2106`)
and `docs/design/design_handoff.md` admits "il salvataggio non azzera", so it is not silent,
but there is no GUI way to deactivate one of the four optional caps — contrary to the label.
Fix shape: for the four optional caps only, write `""` (loader → None) when the GUI field
is blank, or change the label to "vuoto = non impostato (conserva il valore salvato)" and
provide a clear action. Note: no shipped user holds the old 1/2/2/10 defaults —
`_required_absolute_cap_value` and the "1.0" vars exist only on the unmerged #505 branch
(`git grep` on origin/main: absent), so no migration debt.

**E1-b — verified NOT present.** With all four keys absent: `RoserpinaConfig` defaults None
(`core/system_state.py:74-77`); loader returns None (`setting_service.py:335-354`);
`_configured_optional` → None → `absolute_cap_reason` returns "" (`validators.py:232`);
`max_drawdown_abs` branch skipped on both paths (`runtime_controller.py:3326`, `4710`).
Bankroll 100 / MM 3 € BACK submitted (new test); LAY at 3.0 (liability 3 €, > old 1/2/2 €)
submitted (probe). `reload_config` goes through the same loader. No fixture/conftest/
`config.example.json`/template injects the four keys (grep). `_market_current_exposure`,
`_event_current_exposure`, `money_management._max_event_exposure_abs` (percentage, line 93)
unchanged. Remaining fixed-euro gates (`min_stake`, `max_stake_abs`, `max_win`) are
pre-existing, owner-accepted configured minimums/caps, outside this rectification.

## ERRORE 2 — SAFETY RIMOSSA

**(a) Present-but-invalid blocks on both entry paths — OK.** 0/-1/NaN/True/"x" via real DB
loader → signal path `<name>_non_valido` (new test, 20 cases); auto-next covered for 0.0
by the new test and extended by probe to NaN/True/"x"/-1/inf/-0.0 (24 cases, all
`<name>_non_valido`). "" vs unreadable text: `setting_service.py:492-500` returns None only
for None/blank string, NaN for bool and `float()` failure; probe confirms legacy prefix-less
"" → None and "dieci" → NaN → block. An unreadable value cannot become None.

**(b) bool ⇒ NaN for all optional hard-stops.** No production caller passes a bool
(grep: only tests asserting invalidity). Consumers: `max_open_exposure` NaN →
`max_open_exposure_non_valido` (`runtime_controller.py:3375-3378`, `validators.absolute_cap_reason`);
`max_session_loss` / `max_exposure_stop` NaN → `_non_valido` (`loss_limits.py:46`,
`risk_stop.py:42`); `max_recovery_chase_abs` NaN → chase 0 (`money_management.py:246-252`).
**LOW, pre-existing, note only:** `max_daily_loss` and `max_drawdown_hard_stop_pct` NaN are
NOT fail-closed in SIM — `_safe_daily_loss_threshold` (`runtime_controller.py:1089-1097`)
maps NaN to None = "not configured" and `determine_desk_mode` (`money_management.py:115`)
`daily_loss >= nan` is False; `max_drawdown_hard_stop_pct` check is LIVE-only (`3312`) and
`current_dd >= nan` is False. LIVE is protected by the deploy gate
(`_validate_live_hard_stop_config` → `LIVE_HARD_STOP_CONFIG_INVALID`, probe confirms).
Behaviour for text values was identical before the patch; the patch only moves bool from
the accidental "1 €" coercion to NaN. `tests/safety/test_live_key_source_hard_stop_required.py:94-101`
still documents True→1.0 on the *config object* — now inconsistent with the loader's
semantics but not weakened. Fix shape (follow-up, out of scope): treat NaN `max_daily_loss`
as `max_daily_loss_non_valido` in SIM too.

**(c) `max_drawdown_abs`** — configured → LOCKDOWN identical to 618884e (code untouched,
`test_configured_drawdown_cap_keeps_lockdown_semantics`, integration test latch); absent →
no lockdown, `lockdown_drawdown_pct` path (`runtime_controller.py:4130`) untouched.

**(d) Adapted test** `test_issue437_pr28d.py:239-263` — old asserted "" ⇒ NaN (configured,
fail-closed); new asserts "" ⇒ None AND "dieci euro" ⇒ NaN non-None. Correctly encodes the
owner rule; does not mask the fail-closed branch.

**(e) `_required_absolute_cap_value` removal** — it only did `float(value)`; inf/huge/-0.0
pass both helpers as floats and are rejected downstream by `finite_number`/`cap <= 0`
(probe: inf and -0.0 → `_non_valido` on auto-next; acceptance test covers signal path).
No validation lost.

## Verified OK
- Loader: absent/None/"" → None; bool/text → NaN; legacy key read and logged; prefixed wins.
- Both admission paths read caps only via `_configured_optional` (no catch-all leak).
- GUI cannot write invalid values (`_parse_hard_stop` raises on non-numeric/≤0/non-finite).
- Save payload never writes a default for the four keys.
- `core/system_state.py` change is comment-only; docs/CHANGELOG consistent with code
  (including the honest "non azzera" note).
- No `.github/workflows`, `scripts/pr_*`, policy files touched.

## Verdict: PASS WITH CORRECTIONS
Required before merge: E1-a — reconcile the "(€, vuoto = non attivo)" label with the
preserve-on-blank save (either persist a clear for the four optional caps, or reword the
label). Recommended follow-up (not blocking): SIM fail-closed for NaN `max_daily_loss`.
