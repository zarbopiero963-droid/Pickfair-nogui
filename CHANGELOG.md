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
- `requirements.txt`: removed `pytest` / `pytest-asyncio` (test deps only).
- CI `unit.yml`, `failure.yml`, `net.yml`: install via `requirements-test.txt`
  instead of `requirements.txt + pip install pytest`.
- `pytest.ini`: registered missing markers (`reconciliation`, `safety`,
  `observability`, `mutation`).
- GitHub Actions: removed `|| true` from `pip install` steps (fail-closed CI).
- `core/reconciliation_engine.py`: -325 lines (-15%) after type extraction.
- `database.py`: -262 lines (-19%) after schema extraction.
