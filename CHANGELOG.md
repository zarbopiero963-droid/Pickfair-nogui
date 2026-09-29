# Changelog

All notable changes to Pickfair are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [Unreleased]

### Security
- Secrets (`password`, `api_hash`, `session_string`, etc.) now encrypted at
  rest in SQLite using a stdlib-only XOF stream cipher (`hashlib.shake_256`).
  Wire format: `enc:v1:<base64(nonce_16 + ciphertext)>`.
- Sensitive fields are redacted in all Telegram alert text and log output via
  `observability/sanitizers.py` with dot-notation key support
  (`telegram.api_hash`, `betfair.password`, …).
- `.gitignore` now covers `*.pem`, `*.key`, `.pickfair/`, `.env.*` and common
  secret file patterns.
- AI review workflows (the five `pr-review-*.yml`): when a secret key starts
  the line (`.env`, YAML, INI, properties, `export`/`set`, list items, comments
  with `#`, `//`, `;`, `!`, `--`, `/*` or `<!--`), an
  unquoted value is now redacted up to the end of the line, spaces included,
  before the diff reaches the provider and before the review is published. It
  used to stop at the first space, so the rest of the value leaked. Mid-line
  matches (arguments, comparisons, signatures) and code-shaped values at the
  start of a statement (calls, subscripts, built-in type annotations,
  arguments) keep the previous rule, so the reviewer still sees the code.
  Arithmetic operators do not count as code, since encoded secrets and
  passphrases contain them; `==`, `:=` and `=>` are not separators, so
  comparisons stay visible.
  Across the whole repo, 18 of 219,588 lines are now redacted more than before
  (comments, examples, test paths joined with `/`), none of them production
  code. Quoted values and GitHub Actions expressions are unchanged.
  (DECISIONE-426 P14, #479)

### Added
- `core/duplication_guard.py`: concrete `is_duplicate()` / `register()` two-phase
  interface (non-atomic check + register) alongside existing atomic `acquire()`.
- `core/runtime_controller.py`: `emergency_stop()` fail-closed implementation —
  cancels all live orders grouped by market, blocks further signals, publishes
  `EMERGENCY_STOP_TRIGGERED`; `reset_emergency()` unblocks.
- `services/betfair_service.py`: `handle_session_expiry()` with bounded re-auth
  (max 1 attempt); `is_live_usable()` gate; `LIVE_BLOCKED_SESSION_INVALID`
  error when session invalid and live requested.
- `core/reconciliation_engine.py`: concrete fencing token implementation —
  `_next_fencing_token()`, `get_active_fencing_token()`,
  `assert_fencing_ownership()`.
- `circuit_breaker.py`: `record_success()` public method.
- `betfair_client.py`: `_api_breaker` (CircuitBreaker, 5 failures / 60 s)
  protecting all JSON-RPC calls; SESSION_EXPIRED does not trip the breaker.
- `core/trading_engine.py`: `_order_submission_breaker` (CircuitBreaker,
  3 failures / 120 s) protecting the live order submission path.
- `core/type_helpers.py`: canonical `safe_float()`, `safe_int()`, `safe_side()`
  — replaces copy-pasted helpers across 7 modules.
- `core/reconciliation_types.py`: extracted public types/enums from
  `reconciliation_engine.py` for focused module concerns.
- `database_schema.py`: extracted DDL from `Database._init_db()`.
- `requirements-dev.txt`, expanded `requirements-test.txt`.
- `pyproject.toml` packaging metadata.

### Changed
- AI review workflow for Grok 4.7 (`pr-review-xai-grok46.yml`): each request to
  the model may now wait 240 s instead of 100 s, and the job timeout rises from
  10 to 20 minutes. The request is not streamed, so the answer arrives only
  after the reasoning; at reasoning `high` Grok often needed more than 100 s
  and the review failed (never completed on #478, 1 attempt out of 8 on #483).
  Waiting longer adds no provider cost, since billing is per generated token.
  A new test checks that, for all five reviewers, exactly three attempts plus
  backoff, the GitHub calls at their timeout and runner start-up fit in the
  job timeout. (DECISIONE-426 P24)
- `requirements.txt`: removed `pytest` / `pytest-asyncio` (test deps only).
- CI `unit.yml`, `failure.yml`, `net.yml`: install via `requirements-test.txt`
  instead of `requirements.txt + pip install pytest`.
- `pytest.ini`: registered missing markers (`reconciliation`, `safety`,
  `observability`, `mutation`).
- GitHub Actions: removed `|| true` from `pip install` steps (fail-closed CI).
- `core/reconciliation_engine.py`: -325 lines (-15%) after type extraction.
- `database.py`: -262 lines (-19%) after schema extraction.

### Fixed
- Cashout in the GUI (#461 PR04): the GUI process now builds the same cashout
  execution chain as the headless app (request bridge with duplicate filter,
  executor with the real SafetyLayer, OrderRouter, residual handler), through
  one shared builder, `cashout_wiring.cabla_catena_cashout`, used by both
  entrypoints. Before, a `CASHOUT` / `CASHOUT ALL` message received by the
  GUI was rejected with `cashout_chain_not_wired`, and the cashout request of
  the auto-close fell on a bus with no listener. Now it goes through the same
  gates as headless (emergency stop, runtime active, session and deploy gate
  in LIVE) and, in LIVE, places a real hedge. The GUI `Log` shows
  `CASHOUT_SUCCESS` and `CASHOUT_FAILED`, and `Storico Bet` and `Risk Desk`
  refresh on both. The GUI still has no manual cashout control. The Telegram
  notification of a cashout residual moved from `headless_main` to
  `cashout_wiring` unchanged, and both entrypoints use it.
  Closing the GUI window now stops the runtime first, as the headless app
  does, and `RuntimeController.stop` marks the runtime stopped before tearing
  down Telegram and Betfair, in both entrypoints. A command or a delayed
  cashout (auto-green grace) arriving during that teardown is rejected
  instead of being placed while the Betfair service is still connected.
  If stopping Telegram fails, `stop` now still disconnects Betfair before
  raising the error; before, the Betfair disconnect was skipped.
- Simulation settlement (#461 PR03): a SIM market is now settled when its
  CLOSED market book reaches the simulation broker. Before, nothing settled
  a SIM market: stakes and liabilities stayed locked forever and the runtime
  never saw a SIM result. Each matched bet is settled with exchange
  semantics (`core.pnl_engine.exchange_settled_gross_pnl`), selections are
  aggregated per market before the 4.5% policy commission, which applies
  once to the market net. The balance is credited with the net minus what
  the fills already moved (stakes, liabilities, realized close-outs), so a
  stake is no longer counted twice, and the market's position ledgers are
  closed. Duplicate CLOSED books, replays and restarts never settle twice,
  and an order placed on an already settled SIM market lapses unmatched.
  With `settlement.poll_enabled` on (default still off) the settlement
  poller now runs in SIM too and delivers each settled market once to the
  runtime cycle (realized PnL, daily loss, bankroll sync, checkpoints),
  also after a restart between the SIM state save and the cycle update.
  Runners without a terminal status, removed runners with bets on other
  runners, possible dead heats (several winners without a declared number
  of winners, or more than declared) and ledgers inconsistent with the
  orders leave the positions open. If handing the settlement to the event
  bus fails, the next poll round delivers it again without re-applying it;
  a delivery in progress is never repeated by a concurrent call. In production SIM still receives no market books until
  the SIM feed arrives (#461 PR11).
- `SimulationBroker.place_bet` now requires BACK or LAY and normalizes case
  and surrounding spaces before recording a PAPER order. Missing, malformed,
  or non-string sides raise `INVALID_SIDE` without changing simulated orders
  or funds. `place_orders` rejects just the invalid instruction without an
  implicit BACK; empty `side` permits a valid `bet_type`, while conflicting
  non-empty aliases and non-string aliases fail. Independent valid batch
  instructions still run. A regression test exercises the actual PAPER
  OrderRouter, OrderManager and CashoutExecutor against the broker.
  This aligns the simulation boundary with the live client (#426 P15).
  The engine's PAPER path now checks the two side aliases (`bet_type` and
  `side`) the same way as the LIVE branch before calling the simulation
  broker: discordant aliases, such as `bet_type=BACK` with `side=LAY`, fail
  with `INVALID_SIDE` instead of recording a `bet_type` order (#426 P17).
  A non-string side alias now fails with `INVALID_SIDE` on both paths, even
  when `str()` would turn it into BACK or LAY; on the LIVE branch such a
  value had been converted and sent since #478 (#426 P20).
- `betfair_client.py`: `BetfairClient.place_bet` now rejects a side outside
  BACK/LAY (checked after strip/upper; empty, `None` and non-string values
  included) with `RuntimeError("INVALID_SIDE")` before building the request,
  so nothing is sent. It used to turn any such value into BACK and report
  `ok=True`: a typo or a missing side became a real BACK bet. Callers that
  relied on the implicit BACK now get the error instead. `TradingEngine`
  treats `INVALID_SIDE` from the real client as a pre-send error (FAILED, not
  AMBIGUOUS). `_safe_side` stays permissive for `calculate_cashout`.
  (DECISIONE-426 P13, #480)
