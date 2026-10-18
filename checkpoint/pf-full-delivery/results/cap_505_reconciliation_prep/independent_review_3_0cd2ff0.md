# Review 0cd2ff0 (PF-CAP-10b/F1) on top of 3369482

Verdict: **PASS WITH CORRECTIONS** (one LOW copy fix, no safety defect)

Tests: the requested 4 files + tests/acceptance/test_issue461_pr04_quater.py + throwaway probe
`/tmp/claude-0/-home-user/72973979-7da3-59f6-82b8-954d718e6349/scratchpad/review_cap505c/test_probe_throwaway.py`
-> 61 passed. Worktree left clean.

## (a) F1 closed: YES
- New test `test_failed_initial_load_blocks_the_save_and_keeps_every_user_limit` passes.
- My probe: the first load raises, then a save. Result: the explicit error is shown and the whole `get_settings()` dict is byte-identical before and after (caps and pct unchanged).
- The guard `getattr(self, "_roserpina_loaded_ok", True) is False` sits at the top of `_save_roserpina_settings` (mini_gui.py:1980), before any parse or write.
- The flag is set True only as the last statement of the load block (1648). A failure halfway through the load leaves it False even though some vars were already set. This is correct.

## (b) No regression: YES
- Load succeeds -> save works (new tests 1-2, probe). Fake service with no `load_roserpina_config` -> flag initialised True (1601) -> save not blocked (probe). Existing fakes-based suites are green.
- Recovery without a restart: **none**. `_load_initial_settings` has exactly one production caller, `__init__` (mini_gui.py:342). No reload button or menu exists, and no refresh path calls it. The post-save hard-stop refresh is never reached when the save is blocked, and it doesn't touch the flag anyway.
- **F3 (LOW, copy): the "o ricarica le impostazioni" part of the error text (mini_gui.py:1986) is not truthful.** Change it to "Riavvia l'applicazione." only, or add a real reload path. The design_handoff text doesn't make the claim, so the copy is the only fix needed. The failure mode itself is fail-closed: the user only loses the ability to save until a restart.

## (c) Re-call flips the flag: YES
- The flag is reset at the start of each call (1601), set True on success and False in `except`.
- Probe: fail -> re-call succeeds -> True, the fields show the persisted values and the save works. Then success -> re-call fails -> False.
- Today this path is reachable only programmatically (tests).

## (d) F2 comments: accurate
- The `_parse_hard_stop` docstring (1851-1856) and the refresh comment (2084-2085) now split the cases correctly: the four optional caps are deactivated by a blank field, the other hard-stops keep "vuoto preserva".
- design_handoff was updated consistently.

Side note: the load failure is now logged with `_LOGGER.exception` instead of being swallowed silently. This is an improvement.
