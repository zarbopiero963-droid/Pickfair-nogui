# Runtime / CTO / Telegram / EventBus / Contention — Phase 0 Audit

**Scope:** Read-only audit of 5 runtime observability areas before any further
implementation.
**Branch:** `claude/audit-runtime-cto-gap-prep-Exe7b`
**Date:** 2026-04-20
**Verdict:** All 5 areas FULLY IMPLEMENTED. No functional gaps. Audit closed.

## Section verdicts

| # | Area | Verdict |
|---|------|---------|
| 1 | Runtime chaos suite           | FULLY IMPLEMENTED |
| 2 | CTO reviewer + 8 CTO rules    | FULLY IMPLEMENTED |
| 3 | Rich Telegram alerts          | FULLY IMPLEMENTED |
| 4 | EventBus fault injection      | FULLY IMPLEMENTED |
| 5 | Contention / stall chaos      | FULLY IMPLEMENTED |

## 1. Runtime chaos suite
Authoritative tests:
- `tests/chaos/test_runtime_network_instability.py` (4 tests)
- `tests/chaos/test_runtime_partial_failure_paths.py` (7 tests)
- `tests/chaos/test_runtime_reconcile_under_stress.py` (5 tests)
- `tests/integration/test_betfair_timeout_and_ghost_orders.py` (7 tests)

Named scenarios covered with real assertions:
`SUBMIT_TIMEOUT_BECOMES_AMBIGUOUS`, `GHOST_ORDER_AFTER_TIMEOUT`,
`TIMEOUT_THEN_RETRY_NO_DOUBLE_EXPOSURE`, `PARTIAL_FAILURE_MISSING_CONFIRMATION`,
`RECONCILE_FETCH_TRANSIENT_FAILURE`, `CONCURRENT_SUBMIT_RACE`,
`FORENSIC_EVIDENCE_SURVIVES_CHAOS`.

## 2. CTO reviewer + rules
- `observability/cto_reviewer.py` — `history_window=6`, `cooldown_sec=60`,
  per-`(rule_name, component)` cooldown, severity bump when
  `evidence_count >= 3`, `current_rule_names()` for stale-resolve.
- `observability/cto_rules.py` — all 8 rules present:
  `RISK_ESCALATION_CHAIN`, `SILENT_FAILURE_DETECTED`,
  `STATE_INCONSISTENCY_CRITICAL`, `STALLED_SYSTEM_DETECTED`,
  `DATA_DRIFT_SUSPECTED`, `OBSERVABILITY_UNTRUSTED`,
  `CASCADE_FAILURE_RISK`, `MEMORY_GROWTH_TREND`.
  Every rule emits `severity / short_explanation / key_metrics /
  correlation_summary / suggested_action`.
- Watchdog integration via `_evaluate_cto_reviewer` → `CTO::<rule_name>`
  alert/incident lifecycle.

Tests: `tests/observability/test_cto_reviewer.py` (4),
`tests/observability/test_cto_rules.py` (2),
`tests/observability/test_cto_runtime_scenarios.py` (5).

## 3. Rich Telegram alerts
`services/telegram_alerts_service.py` provides:
severity threshold, cooldown, dedup via `_dedup_key`, aggregation via
`_maybe_aggregate` (window_start, count, severity_counter, code_counter,
summary_sent), safe-fail `deliverable` flag, governance fields
(`incident_class`, `normalized_severity`, `why_it_matters`,
`suggested_action`, `timestamp`).

Tests: `tests/services/test_telegram_alerts_rich.py` (3),
`tests/services/test_telegram_alert_pipeline.py` (11, including
CTO-originated payload through the same pipeline).

## 4. EventBus fault injection
`core/event_bus.py` exposes `subscriber_error_counts`,
`published_total_count`, `delivered_total_count`, `queue_depth`,
`pressure_snapshot`, `stop(drain=...)`, `stop_lossy`.

Tests: `tests/chaos/test_eventbus_fault_injection.py` (8) — poison pill,
slow subscriber, partial fanout, event-without-side-effect,
critical-vs-noncritical handler behavior, drain-true/false lossy
semantics — all with real assertions flowing into anomaly / CTO /
diagnostics.

## 5. Contention / stall chaos
Tests:
- `tests/chaos/test_db_contention_and_locked_paths.py` (4)
- `tests/chaos/test_runtime_stall_detection.py` (2)
- `tests/chaos/test_runtime_cto_alerts_under_chaos.py` (1)
- `tests/chaos/test_async_writer_stalls_and_backlog.py` (2)
- `tests/observability/test_contention_anomaly_signals.py` (1)

Scenarios: `SQLITE_LOCKED_TRANSIENT`, `WRITER_BACKLOG_GROWTH`,
`WRITER_STALL_WITH_RUNTIME_CONTINUING`, `SNAPSHOT_PERSISTENCE_DEGRADED`,
`STALLED_SYSTEM_WITH_HEARTBEAT_GAP`,
`DB_CONTENTION_PLUS_AMBIGUITY` → `CASCADE_FAILURE_RISK`,
`CONTENTION_WITH_DIAGNOSTIC_EXPORT`.

## Authoritative vs adapter modules

**Authoritative (do NOT refactor):**
`observability/watchdog_service.py`, `observability/cto_reviewer.py`,
`observability/cto_rules.py`, `observability/forensics_engine.py`,
`observability/forensics_rules.py`, `observability/anomaly_engine.py`,
`observability/runtime_probe.py`, `services/telegram_alerts_service.py`,
`core/event_bus.py`, `core/trading_engine.py`, `order_manager.py`,
`core/reconciliation_engine.py`, `database.py`.

**Adapters:** `services/telegram_service.py`, `telegram_sender.py`,
`headless_main.py` (wiring).

## CI blast radius

Changed-file routing via `scripts/ci_changed_modules.py`:
- Changes under `tests/chaos/`, `core/reconciliation_engine.py`,
  `core/trading_engine.py`, `order_manager.py` → `chaos-critical` rule.
- Changes under `observability/**` or `services/telegram_alerts_service.py`
  have no dedicated rule; they fall through to the `mutation-guardrails`
  umbrella AND are additionally covered by path-filtered
  `observability-runtime.yml` / `observability-tests.yml`.

Always-on gates (fire regardless of routing):
`pr-check.yml`, `merge-simulation-hard.yml`, `chaos-runtime.yml`,
`_module-ultra-check.yml`, `ci-dynamic-intelligent.yml`
(noise-free-pr-gate + repo-guardrails-extended + merge-simulation-hard),
`ci-master-gate.yml`.

Path-filtered high-risk gates: `observability-runtime.yml`,
`observability-tests.yml`, `stateful-integrity.yml`, `live-sim-parity.yml`,
`recovery-guardrails.yml`, `trading-engine-hard-tests.yml`,
`integration.yml`, `smoke.yml`.

## Files NOT to touch (unless a failing test proves necessity)

`core/trading_engine.py`, `order_manager.py`,
`core/reconciliation_engine.py`, `database.py`,
`observability/watchdog_service.py`, `observability/runtime_probe.py`,
`core/event_bus.py`, `services/telegram_alerts_service.py`,
`observability/cto_reviewer.py`.

## Safest extension order (future reference)

1. Add a failing scenario test in `tests/chaos/` or
   `tests/observability/`.
2. Append a new rule function to `observability/cto_rules.py` or
   `observability/forensics_rules.py` (append-only in
   `DEFAULT_*_RULES`).
3. Only if the new rule requires evidence not already emitted, extend a
   `runtime_probe` collector.
4. Never edit watchdog tick order, cooldown defaults, or
   `trading_engine` / `order_manager` in the same PR as a reviewer
   change.

## Residual non-blocking note

`scripts/ci_changed_modules.py` has no dedicated rule for
`observability/**` or `services/telegram_alerts_service.py`. Coverage is
guaranteed by `mutation-guardrails` + path-filtered
`observability-runtime.yml` + the always-on full-suite backstops
(`pr-check`, `merge-simulation-hard`, `_module-ultra-check`). Adding a
dedicated rule would be a CI routing optimization only, not a
functional gap. Deliberately left unchanged.

## Final call

Phase 0 is CLOSED. No implementation required. Repository is ready for
any subsequent phase. Gap surface across runtime chaos, CTO reviewer,
Telegram alerts, EventBus fault injection, and contention/stall is
effectively zero.
