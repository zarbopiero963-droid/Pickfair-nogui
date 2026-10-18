# Review 3369482 (PF-CAP-10b): blank GUI field deactivates the four optional caps

Scope: `git show 3369482` only, on top of 6eb8e43. Read-only. The throwaway tests are in
`/tmp/claude-0/-home-user/72973979-7da3-59f6-82b8-954d718e6349/scratchpad/review_cap505b/test_gui_cycle_throwaway.py`.

## Findings

### F1. MEDIUM (P2), CONFIRMED: a failed GUI load followed by a save silently clears the caps (ERRORE 2)
- Where: `mini_gui.py:1597-1650`. `_load_initial_settings` swallows any exception from
  `load_roserpina_config` (`except Exception: pass`), so the four cap vars keep their initial `""`
  (`mini_gui.py:706-709`). A later `_save_roserpina_settings` (`mini_gui.py:2023-2026`) passes None, and
  `services/setting_service.py:451-453` now writes `""`. Every persisted cap is deactivated and the user
  sees "Configurazione Roserpina salvata.", with no error shown.
- Scenario (throwaway test `test_gui_initial_load_failure_then_save_wipes_caps`, PASS = defect
  reproduced). The persisted values are `max_order_exposure=1.0`, `max_drawdown_abs=20` and
  `max_daily_loss=50`. The first `load_roserpina_config` raises ("database is locked"). The user then
  saves the Roserpina tab. Result: `max_order_exposure=None`, `max_drawdown_abs=None`,
  `max_daily_loss=50.0` (preserved), errors=[].
- In this path None does NOT mean "the user left it blank". Before 3369482 the cap keys were skipped,
  so they survived. The loss of the four caps is new with this commit.
- Already the case before this commit, on the same path: the non-optional fields (pct limits,
  commission and so on) are overwritten with the GUI defaults. This commit makes that existing lossy
  path cover opt-in money caps too.
- Likelihood: low. It needs a transient read failure at GUI start and then a successful write. The
  impact is a silent fail-open on a cap the user chose to set.
- Fix shape (small, GUI only):
  - Set `self._roserpina_loaded_ok = True` only when the load block finishes.
  - In `_save_roserpina_settings`, if the flag is False, refuse the save with an explicit error
    ("configurazione non caricata, ricarica prima di salvare"). An alternative is to re-run the load
    and abort if it fails.
  - Add a RED→GREEN test like the throwaway one.

### F2. LOW, CONFIRMED: two GUI comments still describe the old "preserve" rule for all hard-stop fields
- `mini_gui.py:1844`: the `_parse_hard_stop` docstring says "vuoto => None: il save preserva il valore
  persistito". This is no longer true for the four caps.
- `mini_gui.py:2065`: the `_save_roserpina_settings` refresh comment says "(semantica preserve)".
- `tests/integration/test_mini_gui_hard_stop_fields.py` docstring: correct as written, because it only
  talks about the three LIVE hard-stops.
- Fix shape: change the comment text only, splitting the four caps (deactivate) from the other
  hard-stops (preserve).

## ERRORE 1 (a hidden cap still applied after a blank save): none found
- The only production caller of `save_roserpina_config` is `mini_gui.py:2045`. Nothing in
  `headless_main`, the Control API, MCP or `tools/` builds a RoserpinaConfig and saves it.
- After the save, `runtime.reload_config()` is called synchronously (`mini_gui.py:2055`).
  `RuntimeController.reload_config` (`core/runtime_controller.py:1932-1943`) replaces `self.config`
  and `self.mm` under the risk lock. No other component holds a copy of the old config.
- If the reload fails, the GUI shows an explicit "nuovi limiti NON attivi" error. The old, stricter cap
  stays in force, which is the safe direction.
- `core/risk_gate.py:126` takes a fresh `load_roserpina_config()` snapshot on every check, so the cleared
  cap is picked up on the next check.
- DB round trip:
  - `Database._set_setting` stores `str("")` as `""`, and `get_settings` returns `""`.
  - `_optional_hard_stop_value` reads `""`, whitespace and None as None.
  - When the prefixed key is present (even as `""`), the prefix-less legacy key is ignored.
- Runtime readers (`_configured_optional`, `core/runtime_controller.py:3325-3360` and `4698-4724`) read
  only `self.config`. No other reader parses the raw `roserpina.max_*` keys.

## Verified OK
- Full GUI cycle with the real `SettingsService` and `Database` (throwaway tests):
  - Persisted cap 1.0 shows as "1.0". The user clears the field and saves. On reload the cap is None,
    the field refreshes to "", and `max_daily_loss` blank keeps 50.0 (preserve unaffected).
  - Saving some other field leaves an untouched persisted cap at its exact value (2.5).
  - A persisted invalid value ("dieci") shows as "nan", the save fails closed with an error, and the
    stored value is left unchanged. It is never cleared silently.
- Order 3 € accepted after the blank save and reload: covered by the commit's own test
  `test_blank_save_of_order_cap_lets_the_mm_stake_through` (real runtime chain, stake 3.0 exact).
- The other hard-stops (`max_daily_loss`, `max_open_exposure`, `max_session_loss`, `max_exposure_stop`,
  `max_drawdown_hard_stop_pct`, `max_recovery_chase_abs`) are untouched in the diff and still preserve
  on blank.
- Valid values are written exactly. Present-but-invalid values stay NaN, so new orders are rejected
  with `*_non_valido`.
- Test runs:
  - Requested set (both pr28d files, hard_stop_fields, mini_gui_integration, mini_gui_failures,
    live_control_plane_settings): 182 passed.
  - Every test file that mentions `save_roserpina_config`, `max_order_exposure` or `max_drawdown_abs`:
    570 passed.
- Docs: CHANGELOG and design_handoff match the new behaviour.

## Verdict: PASS WITH CORRECTIONS
The change does what the owner rule asks on the normal path, and no hidden cap path exists.
- F1: fix before merge, or record it explicitly as a tracked follow-up, because it is a fail-open on a
  failure path this commit introduced.
- F2: comment-only change.
