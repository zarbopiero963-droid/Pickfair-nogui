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
- AI review workflow for Sol (`pr-review-openrouter-gpt56-sol.yml`, file name
  kept): the per-push reviewer moves from `openai/gpt-5.6-sol` to
  `openai/gpt-6.1-sol` on OpenRouter, with reasoning effort `max` instead of
  `high` — the highest level the model offers, checked against its OpenRouter
  metadata. The effort whitelist of this workflow now admits `xhigh` and `max`
  (without it `max` would silently fall back to `high`); the output ceiling
  rises from 25,000 to 64,000 tokens (model maximum 128,000), each request may
  wait 1800 s instead of 100 s and the job timeout rises from 12 to 105 minutes.
  The cost estimate now uses the OpenRouter list price, $2/M input and $10/M
  output: the previous 5.00/30.00 overstated the reported cost about threefold.
  New tests pin the model id, tie the widened whitelist to that id, require a
  higher ceiling and a longer wait at `xhigh`/`max`, and check that the
  failed-call heading is recognised by the done-marker guard. (Owner decision,
  07-10-2026)
- Merge readiness gate (`pr-merge-readiness.yml`): the wait for the head's
  checks rises from 900 s to 6600 s and the job timeout from 25 to 120 minutes.
  The budget must cover the slowest review job (Sol, 105 minutes) plus its
  start-up delay: with 900 s a valid but slow review left the gate red, which
  could already happen with Grok's 20-minute job. The gate still exits as soon
  as the checks are settled, and still fails when the budget runs out. A test
  ties the budget to the five review workflows' timeouts. The worst-case test
  of the review workflows now also counts a GitHub call written over several
  lines: there are seven calls per workflow, not six, and every call must be
  recognised or the test fails.
- AI review workflow for Grok 4.7 (`pr-review-xai-grok46.yml`): follow-up owner
  decision on 07-10-2026 raises the per-attempt wait from 240 s to **400 s**
  and the job timeout from 20 to **30 minutes**, giving large diffs more time
  to finish instead of losing paid attempts. The request is not streamed, so
  the answer arrives only after the reasoning. The earlier 100→240 s change
  remains historical context; waiting longer adds no provider cost because
  billing is per generated token.
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
- RISK_STOP on owner stop limits, separate from EMERGENCY (#461 PR28-c,
  PKG-P24-B: P38 and the P37 exposure stop).
  - Before, reaching the session-loss limit only blocked new entries: the bot's
    unmatched orders stayed in the market and its open positions were not
    closed. There was no total-exposure stop at all.
  - **RISK_STOP** (`RuntimeController.risk_stop`, rules in `core/risk_stop.py`):
    blocks new entries (signals, auto-next, resume) but not cashout; cancels the
    bot's unmatched orders and attempts to close its positions by routing the
    existing `CASHOUT_ALL` through the CashoutRouter (bot orders only, the
    cashout's own gates, blocked or uncertain outcomes reported by the router).
    It publishes `RISK_STOP_TRIGGERED` and is idempotent. If EMERGENCY is
    already active (the strongest barrier, cancel-all + LOCKDOWN) no cashout is
    routed. PAUSE is unchanged (blocks new entries only).
  - The stop is persisted (`risk_stop_state` setting) together with the state
    change, under the same lock: a restart does not clear it, and an unreadable
    saved state keeps it active. `reset_risk_stop()` clears it only after a full
    recheck: no emergency, daily loss not breached, limits valid, session loss
    below its limit and exposure below the exposure stop (positions actually
    closed). `start()` reopens on its own only an exposure stop, checked on the
    exposure held before the tables are rebuilt; a session-loss, unreadable or
    unknown stop cannot be verified by the new session and needs an explicit
    `reset_risk_stop()`. While the stop stays on, `start()` re-attempts the
    `CASHOUT_ALL` (for example after a restart).
  - **Triggers:** session loss reached (`max_session_loss`, PR28-b) and the new
    `max_exposure_stop` (EUR, owner limit with no default, GUI/loader/save). It
    is checked before submission on the total exposure including the order; a
    misconfigured value (unreadable or <= 0) blocks entries without closing
    anything. No fixed amount belongs to STOP/cashout (#426, 06/10).
- Durable daily loss and session-loss stop (#461 PR28-b, PKG-P24-B: F4, P37).
  - **F4:** the day's realized loss is saved (`daily_loss_state` setting) and
    rebuilt at restart on the same UTC day, before any new risk. Before, a
    restart zeroed it: EUR 8 lost, restart, another EUR 8 = 16 against
    `max_daily_loss` 10 with no stop. An unreadable saved state, or an invalid
    or future saved day, counts as a breach (fail-closed); a past UTC day
    starts from zero. If a save fails, new risk is refused until a save
    succeeds and the state goes to a marker file next to the db
    (`<db>.daily_loss_pending.json`). Every record (db and marker) carries
    the UTC day and a monotonic sequence number `seq`. At restart both are
    read: the higher `seq` wins; with an equal or unknown `seq` the larger
    loss wins, and a breach in either always counts. A leftover marker
    (failed remove, crash between save and remove) has a lower `seq` and can
    never lower the loss. Known limitation (declared): if neither the db nor
    the marker can be written, the block holds for the session only; a
    restart in that condition cannot rebuild the unsaved loss.
    Persistence lives in the new `core/daily_loss_store.py` (`db_path` may
    be a str or an `os.PathLike`).
  - `reset_cycle()` and the drawdown auto-reset no longer wipe the day's or the
    session's loss (they zero the desk `realized_pnl`; the baselines now shift
    with it).
  - **P37:** new owner limit `max_session_loss` (EUR, realized loss since the
    last `start()`), in GUI, loader and save. No default: empty = not set.
    Reached (with float tolerance) => new entries, auto-next and `resume()`
    are refused and `SESSION_LOSS_BREACH_TRIGGERED` is published once.
    Unreadable or <= 0 => entries refused. It is a risk limit, not a cashout
    parameter: exits/cashout keep their own gates (#426 decision 06/10).
- Owner limits measured on the order's risk (#461 PR28, PKG-P24-B, first slice).
  - **Single formula:** `core/validators.py` `order_exposure` gives BACK = stake
    and LAY = liability `stake * (price - 1)`. `exceeds_cap` holds at the exact
    threshold with float tolerance.
  - **LAY consumers:** money management (single, total and event caps), the A2
    `max_open_exposure` cap, the table exposure and auto-next all used the stake.
    A LAY of 1.5 at 21 (liability 30) counted as 1.5, and a LAY of 10 at 21
    (liability 200) passed the single cap of 180. A LAY over a cap is now
    rejected, with no resizing. RiskGate uses the same function.
  - **Exact threshold:** 0.1 + 0.2 against a cap of 0.3, or a LAY of 0.1 at 4
    against MAX_WIN 0.3, are no longer denied. A one-cent excess still is.
  - **`reload_config`** (GUI save while the bot is running): it no longer
    rebuilds the TableManager and the ReconciliationEngine. Before, tables,
    exposure and recovery memory went back to zero, so the aggregate caps
    restarted from 0. The table count adapts without dropping busy tables.
    `start()` keeps its fresh session.
  - **Auto-next:** it now applies the A2 cap and the real event exposure, and
    activates the table with the liability.
  - **Best price DIRECT:** if the price changes after the caps, a LAY whose
    liability grows is rejected.
- Fail-closed numeric validation for orders and risk (#461 PR27, PKG-P24-A).
  - A new shared source, `core/validators.py` (`order_market_id`,
    `order_selection_id`, `order_price`, `order_stake`, `finite_number`), uses
    the same `INVALID_*` codes as the client. The #488 client check is unchanged.
  - Engine: a present bool, NaN/inf, non-numeric value or non-integer selection
    id is now rejected at normalization (FAILED/INVALID_REQUEST). There is no
    `orders` row, no intent mark, no transport, and the key of an in-flight
    original is not released. Before, `selection_id=True` was placed on
    runner 1, `stake=True` was placed as 1 EUR, and 5678.9 was truncated.
  - Runtime signal: market/selection/price are checked before anti-duplication,
    tables and money management. A non-numeric selection used to raise with the
    event key still locked.
  - A2 cap: `max_open_exposure` that is configured but unreadable
    (NaN/inf/bool/text) now denies. NaN used to disable the cap.
  - Money management:
    - an invalid fixed stake from a signal, including `False`, is rejected
      (`stake_segnale_non_valido`). `0`/`None`/`""` stay the producer's
      "let MM decide" sentinel;
    - a non-finite exposure (`total_exposure()` = inf on a corrupted state,
      #449) is rejected instead of being counted as 0;
    - an unreadable `max_stake_abs` fails closed;
    - a bool recovery cap counts as a corrupted config (chase 0, #450).
  - OrderManager: bool and truncated ids or amounts are rejected before
    conversion.
- Single order authority and durable intent de-dup (#461 PR26, gate
  `LIVE_BLOCKED_UNTIL_PR26_DURABLE_DEDUPE`): the engine marks a
  `customer_ref` as consumed in the new additive SQLite table
  `consumed_order_intents`, committed right before every transport call
  (LIVE branch, declared SIM broker, `OrderManager`, fallback). The mark does
  not follow the order status: the same intent is `DUPLICATE_BLOCKED` after
  COMPLETED, FAILED after send (broker rejection), AMBIGUOUS, and after a
  restart, and two processes on the same file cannot both send (PRIMARY KEY).
  Before, de-dup only saw INFLIGHT/SUBMITTED rows and in-memory keys released
  on terminal outcomes. The mark is removed only with proof that nothing was
  sent: an engine pre-send error, or an argument error raised by the real
  `BetfairClient` or by the identity-proven `SimulationBroker`. Timeout and
  unknown outcomes stay AMBIGUOUS and go to reconciliation, never to a replay.
  STOP and every gate before the transport consume nothing. In declared LIVE a
  DB without the capability sends nothing (`DURABLE_DEDUPE_UNAVAILABLE`). A
  `DUPLICATE_BLOCKED` no longer releases the original's in-memory key.
- `TradingEngine` with `simulation_broker` set and a getter that does not
  return it: places through the broker's `place_bet` with the same side and
  signature adapter instead of failing with `NO_VALID_EXECUTION_PATH`
  (`SimulationBroker` has no `execute()`).
- `SimulationBroker` (F8, F10 of #453): an accepted unmatched order reports
  `SUCCESS` with `orderStatus` `EXECUTABLE`, not `FAILURE`; an invalid market,
  selection, price or size (NaN/Inf included) raises the same
  `INVALID_*` code as `BetfairClient.place_bet` before matching, storage or
  writes, and `place_orders` reports that leg as `FAILURE` with nothing stored.
- `OrderManager` (F14 of #453 and adapter signatures): the client pre-send
  codes are certain rejections (PERMANENT), not AMBIGUOUS; non-finite price or
  stake and an invalid or conflicting side are rejected before saga and client
  (a typo no longer becomes BACK); the `BetfairClient` envelope is read
  (`ok`/`result`/`order_unknown`), so a placed order is no longer FAILED and a
  lost response is AMBIGUOUS; cancel uses each broker's signature for one bet
  id and refuses an empty bet id (never cancel-all) and `size_reduction`.
- `SafetyLayer.validate_cashout_request` (F15 of #453) rejects NaN/Inf price
  and stake.
- Order identity for existing producers (#461 PR26-a): the runtime signal
  path, the post-settlement auto-trade, every dutching leg and
  `DutchingController.manual_bet` now publish `CMD_QUICK_BET` with a
  `customer_ref`. Before, the engine rejected all of them with
  `CUSTOMER_REF_REQUIRED`. `core/order_identity.py` gives every ref the
  Betfair-compliant shape `<prefix>-<28 hex>` (32 chars). Where each ref
  comes from:
  - an upstream ref is preserved, also when the mini-GUI nests it in
    `raw_signal`; a non-compliant one is mapped
    deterministically instead of being silently dropped by `BetfairClient`;
  - signal: derived from the message and its chat, without `received_at`
    or `event_key` (at any depth) and without the MM stake. The message has
    the same canonical form on the headless and mini-GUI paths (the innermost
    `raw_signal`), so the resolved price and the GUI stake are not part of the
    identity, while the effective SIM/LIVE mode is. A redelivery after
    reconnect or restart, even on the other entrypoint, keeps the same ref,
    and the engine blocks it;
  - auto-trade: derived from the triggering settlement;
  - dutching leg: derived from the request inputs (market, total stake,
    sorted selections) plus the leg itself, not from the computed stakes, so
    neither the order of the selections nor the order-sensitive rounding of
    equal odds matters. The ref is also stored in
    `dutching_batch_legs`;
  - `manual_bet` without an upstream ref: a fresh operation ref per call.
  The `RiskMiddleware` REQ_QUICK_BET forward keeps the caller's ref and never
  invents one. The engine gate is unchanged, and the Telegram REQ_QUICK_BET
  compat fallback (no runtime gate) stays rejected.
- Betfair credentials (#461 PR04-quater, DEC-426-P28–P32): separate encrypted
  Delayed and Live App Keys; the legacy key migrates only to Delayed. LIVE
  requires the explicit Live key, and the real client never uses Delayed.
  Settings include certificate/private-key file pickers and validate readable
  PEM files, permissions and the matching TLS pair before an atomic save.
  An empty password preserves the saved password; a replacement clears the
  input after saving. App Keys are masked in the registry and redacted from
  client errors and diagnostics. SIM remains offline; its Delayed feed belongs
  to PR11, and the authenticated encryption upgrade remains in PR35.
  LIVE readiness reads canonical settings without triggering migration and
  reports actionable missing/unavailable-key remedies in registry/headless.
  The legacy credentials writer updates Delayed after migration while keeping
  Live/password unchanged. Colliding redaction masks use a neutral ASCII
  character so strict Windows CP1252 log sinks retain login diagnostics.
  A read-only Live-key status distinguishes absent values from undecodable or
  unsupported ciphertext, retaining the stored value and directing master-key
  recovery. The existing unauthenticated enc:v1 format is unchanged; integrity
  verification remains in PR35.
  Empty GUI saves preserve unreadable Delayed/Live or unmigrated legacy App
  Keys; explicit replacements and clearing readable keys remain supported.
  Registry configuration reports never migrate, including on read-only legacy
  storage, and unsupported legacy enc formats remain retryable after recovery.
  Volatile unreadable-key provenance also protects an old empty form when
  another writer restores or replaces the credential before that form saves.
  Registry entries and presence checklist direct unreadable credentials to
  recovery; expired matching certificates are rejected before persistence.
  Load values and unreadable provenance share a transaction snapshot;
  certificate validation also rejects a future notBefore date.
  The client also blocks a future certificate swapped in after save with the
  trusted CERT_NOT_YET_VALID diagnostic before any login HTTP request.
  Every credential echo is redacted, including short/concatenated values and
  login passwords in client diagnostics. Local login codes are separate from
  provider text; session expiry is classified before redaction.
  Certificate diagnostics remain actionable; unreadable legacy credentials
  do not finalize migration, and original ciphertext remains recoverable.
  LIVE readiness/deploy also require the Live key; SIM remains independent.
  Short session tokens and colliding redaction markers are covered.
- Live client, non-finite price or size (F11 in #453, DECISIONE-426 P27):
  `BetfairClient.place_bet` now rejects a price or a size that is NaN or
  infinite with `INVALID_PRICE` / `INVALID_SIZE` before building the request,
  as `replace_orders` already did. Before, `float()` accepted them and
  `nan <= 1.0` is false, so the `placeOrders` body went out with
  `"price": NaN` or `"size": Infinity`; Betfair would have refused a body
  that is not valid JSON, but the check belongs to the client. In production
  the cashout chain reaches the client through `OrderRouter.place`, where the
  SafetyLayer only checks `price <= 1` and `stake <= 0`: the client was the
  only barrier. `BetfairService.place_order` and `OrderManager.place_order`
  (which only checks `price <= 1.0`) had the same hole, but today nothing in
  production calls them. For the `TradingEngine` the rejection is a certain
  pre-send failure, not an ambiguous outcome.
- Book sides (F6 in #453, DECISIONE-426 P25): the simulation broker, the
  cashout router and the Telegram resolver now read the two ladders as Betfair
  defines them, the convention the owner chose for dutching in #383:
  `availableToBack` holds the prices you can back at now, `availableToLay` the
  prices you can lay at now.
  - SIM: a BACK matches on the best `availableToBack` when its price is at or
    below it, a LAY on the best `availableToLay` when its price is at or above
    it, both at the book price. Before, each side matched against the opposite
    ladder: paper results earned the spread instead of paying it, and a BACK at
    the really executable price stayed unmatched.
  - Cashout: a BACK position is closed with a LAY at the best `availableToLay`
    and a LAY position with a BACK at the best `availableToBack`; the L95 depth
    gate reads that same level. Before, in LIVE the hedge would have rested
    unmatched and the residual handler would have cancelled it.
  - Cashout in LIVE: the router now asks for the market book with prices
    (`include_prices=True`, `EX_BEST_OFFERS`). Without them Betfair returns no
    ladders, so every position was skipped with only a log warning and a
    `CASHOUT` from the chat closed nothing.
  - The "aggressive" BACK of the Telegram resolver (old Telegram tab of the
    GUI, reachable in SIM only) now takes the best `availableToBack`, the
    price that matches at once.
  - The SIM no longer matches an order with an invalid price (malformed,
    non-finite or at most 1.0), as the live client refuses it. Before F6 a LAY
    with such a price matched; the side fix alone would have moved the hole to
    the BACK side.
  - `SimulationMatchingEngine` and `SimulationOrderBook` (used by tests only)
    follow the same rules; `direct_best_price` docstrings no longer call the
    executable price "defensive" (its code was already right).
  May look like a regression: SIM results are less optimistic, since the
  spread is now paid, and a SIM BACK priced above the best back now rests
  unmatched instead of matching.
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
