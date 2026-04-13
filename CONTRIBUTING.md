# Contributing to Pickfair

## Serial task execution

This repository enforces **one open PR at a time**.  See `AGENTS.md` for the
full policy.  In short:

1. Work on a dedicated feature branch — never commit directly to `main`.
2. Open a PR only when the previous PR has been merged.
3. Task files live in `ops/tasks/`; mark them done by moving to
   `ops/tasks_done/` as part of the PR.

## Development setup

```bash
# Clone and install dev dependencies
git clone <repo>
cd Pickfair-nogui
pip install -r requirements-dev.txt
```

## Running tests

```bash
# Fast unit tests (< 30 s)
pytest tests/unit -q -m unit

# Reconciliation engine
pytest tests/reconciliation -q

# Safety / emergency stop
pytest tests/safety -q

# Observability / sanitizers
pytest tests/observability -q

# Full suite (slow — includes chaos / integration)
pytest tests/ -q
```

## Commit message style

```
[TASK N] Short imperative summary (≤ 72 chars)

Optional body explaining the why, not the what.
```

## Security checklist before opening a PR

- [ ] No secrets in committed files (`git diff --staged` scan)
- [ ] Sensitive fields covered by `observability/sanitizers.py`
- [ ] New external HTTP calls protected by `CircuitBreaker`
- [ ] Tests for fail-closed / error paths added

## Adding a new secret field

1. Add the field name to `_SECRET_FIELDS` in `database.py`
2. Add the field name to `SENSITIVE_KEYS` and `_SENSITIVE_KEY_SUFFIXES` in
   `observability/sanitizers.py`
3. Add a test to `tests/unit/test_secret_cipher_and_db.py` confirming the
   on-disk value is not plaintext
4. Add a test to `tests/observability/test_sanitizers_coverage.py` confirming
   the field is redacted in formatted output

## Code style

- Python 3.11+, `from __future__ import annotations` in every module
- No external type checkers required; `typing` stdlib only
- `ruff` for linting (`pyproject.toml` config)
- Prefer narrow exceptions over bare `except Exception`
- No `|| true` in CI install steps — fail-closed is the policy
