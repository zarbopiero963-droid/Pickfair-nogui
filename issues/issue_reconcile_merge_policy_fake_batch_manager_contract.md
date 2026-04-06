# Bug report: reconciliation merge-policy tests are broken by BatchManager contract drift

## Summary
The unit test module `tests/unit/test_reconcile_merge_policy.py` currently fails before executing merge-policy assertions because `FakeBatchManager` no longer satisfies `ReconciliationEngine`'s required BatchManager contract.

## Reproduction
Run:

```bash
pytest -q --import-mode=importlib tests/unit/test_reconcile_merge_policy.py
```

## Actual behavior
All seven tests in this module fail during engine construction with:

- `TypeError: BatchManager contract violation: missing methods ['get_open_batches']`

## Expected behavior
The tests should instantiate `ReconciliationEngine` successfully and validate merge-policy behavior (exchange/local precedence, UNKNOWN timeout handling, PARTIAL transitions, terminal no-op).

## Evidence
`ReconciliationEngine` now enforces `get_open_batches` in `_REQUIRED_BM_METHODS` and validates it at initialization.

`FakeBatchManager` inside `tests/unit/test_reconcile_merge_policy.py` implements several methods but does not define `get_open_batches`, causing immediate `TypeError`.

## Root cause hypothesis (evidence-based)
Contract drift between production component and test fake:

- production contract was tightened to include `get_open_batches`
- local fake in merge-policy tests was not updated accordingly

## Why this should be fixed
- Prevents execution of an important policy-focused test module (7 tests).
- Reduces trust in test coverage for reconciliation precedence rules.
- Can hide real regressions because failures are infrastructure-level, not behavior-level.

## Suggested fixes
1. Update `FakeBatchManager` in `tests/unit/test_reconcile_merge_policy.py` with a minimal `get_open_batches()` implementation compatible with test fixtures.
2. Optionally add a shared fake/stub for BatchManager contract to avoid future drift across test modules.

## Acceptance criteria
- `pytest -q --import-mode=importlib tests/unit/test_reconcile_merge_policy.py` passes.
- Tests fail only on real policy regressions, not on contract initialization errors.
