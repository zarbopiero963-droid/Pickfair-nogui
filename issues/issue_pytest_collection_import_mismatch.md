# Bug report: `pytest` collection fails for duplicate test module basenames

## Summary
Running the full test suite with `pytest -q` fails during collection with two `import file mismatch` errors.

## Reproduction
1. From repository root, run:
   ```bash
   pytest -q
   ```
2. Collection stops with errors similar to:
   - `tests/unit/test_betfair_client_failures.py` conflicts with `tests/failure/test_betfair_client_failures.py`
   - `tests/unit/test_trading_engine.py` conflicts with `tests/core/test_trading_engine.py`

## Actual behavior
`pytest` aborts at collection time and does not execute the suite.

## Expected behavior
`pytest -q` should complete collection and run tests across folders without module-name collisions.

## Root cause hypothesis (evidence-based)
The repository contains duplicate test file basenames in different directories:

- `test_betfair_client_failures.py`
  - `tests/unit/test_betfair_client_failures.py`
  - `tests/failure/test_betfair_client_failures.py`
- `test_trading_engine.py`
  - `tests/core/test_trading_engine.py`
  - `tests/unit/test_trading_engine.py`

With the current test layout/import mode, Python imports by module basename, so the second file with the same basename conflicts with the already-imported module path.

## Why this should be fixed
- Blocks CI/local validation because the full suite cannot be executed reliably.
- Hides real regressions by failing before test execution.
- Increases maintenance risk: adding more duplicate basenames can create more nondeterministic collection errors.

## Suggested fixes
Pick one consistent strategy:
1. Rename duplicate test files to unique basenames (recommended, smallest blast radius).
2. Convert test directories to proper packages and adopt explicit imports.
3. Configure `pytest` import behavior (e.g., `--import-mode=importlib`) only if compatible with existing fixtures/plugins.

## Acceptance criteria
- `pytest -q` no longer fails with `import file mismatch` collection errors.
- Both conflicting test groups still run and report independently.
