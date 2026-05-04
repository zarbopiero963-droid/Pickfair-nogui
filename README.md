# Pickfair

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/2140d214f5344955beb7bb7cd7a03b9c)](https://app.codacy.com/gh/zarbopiero963-droid/Pickfair-nogui?utm_source=github.com&utm_medium=referral&utm_content=zarbopiero963-droid/Pickfair-nogui&utm_campaign=Badge_Grade)

Automated sports-betting trading system for the Betfair exchange.

## Architecture overview

```
betfair_client.py          HTTP/JSON-RPC client for the Betfair API
circuit_breaker.py         Fail-fast protection for external calls
core/
  trading_engine.py        Order lifecycle state machine
  reconciliation_engine.py Exchange ↔ local order reconciliation
  reconciliation_types.py  Types, enums, config for reconciliation
  runtime_controller.py    Live/simulation mode + emergency stop
  safety_layer.py          Pre-flight risk invariants
  money_management.py      Stake sizing and drawdown limits
  duplication_guard.py     Idempotency / duplicate-event guard
  type_helpers.py          Shared numeric conversion helpers
database.py                SQLite persistence layer (encrypted secrets)
database_schema.py         DDL declarations separated from DB logic
services/
  betfair_service.py       High-level Betfair service (auth, funds)
  telegram_alerts_service.py Push alerts to Telegram
observability/
  sanitizers.py            Redact sensitive fields in logs/alerts
```

## Quick start

```bash
# Install runtime dependencies
pip install -r requirements.txt

# Install for development / testing
pip install -r requirements-dev.txt

# Run unit tests
pytest tests/unit -q -m unit
```

## Configuration

Settings are stored in the SQLite database (`pickfair.db`).  Secret fields
(`password`, `api_hash`, `session_string`, etc.) are encrypted at rest using
a key resolved in this order:

1. `PICKFAIR_SECRET_KEY` environment variable (64-character hex string)
2. `~/.pickfair/db.key` (auto-generated on first run, `chmod 0600`)
3. Ephemeral in-memory key (logged as WARNING — do not use in production)

## Security notes

- Never commit `*.pem`, `*.key`, or `.env` files — these are covered by `.gitignore`
- The circuit breaker (`BetfairClient._api_breaker`) opens after 5 consecutive
  API failures and holds for 60 s before probing
- The order-submission breaker (`TradingEngine._order_submission_breaker`)
  opens after 3 failures and holds for 120 s
- Emergency stop (`RuntimeController.emergency_stop()`) is fail-closed:
  cancellation failures do **not** resume live trading

## Running tests

```bash
# All unit tests
pytest tests/unit -q -m unit

# Reconciliation engine tests
pytest tests/reconciliation -q -m reconciliation

# Safety / emergency stop tests
pytest tests/safety -q -m safety

# Observability / sanitizer tests
pytest tests/observability -q -m observability
```

## Branch policy

All development happens on feature branches.  See `AGENTS.md` for the
serial-task execution policy enforced by CI.
