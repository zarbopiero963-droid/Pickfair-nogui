# Observability Minimum (Operator-Facing, Phase 1)

This file is the **single authoritative minimum** for operator-visible observability before paper/live support.

Scope:
- Defines what an operator must be able to see.
- Does **not** redesign runtime internals.
- Fails closed in CI if required sections are missing.

## startup_shutdown_visibility
Minimum operator-visible signals:
- Runtime startup state is visible through health/readiness snapshots.
- Runtime shutdown state (intentional stop or degraded stop) is visible through health/alert state.
- No silent start/stop transitions for core runtime process.

Operational checks:
- Health snapshot includes runtime component state.
- Any non-ready startup state is visible as blocker/degraded signal.

## order_lifecycle_visibility
Minimum operator-visible signals:
- Visibility for submit/inflight/success/failure/ambiguous order lifecycle states.
- Contradictory local-vs-remote order state is surfaced as alert/incident.

Operational checks:
- Runtime state and diagnostics include recent order/audit evidence.
- Alert/incident stream shows lifecycle contradictions when present.

## live_readiness_deploy_gate_visibility
Minimum operator-visible signals:
- Readiness status is visible with explicit level (`READY`/`DEGRADED`/`NOT_READY`).
- Deploy/go-live allow/deny status is visible with explicit reason and blockers.

Operational checks:
- Readiness report includes blockers and component detail.
- Deploy gate status includes allowed flag and reason.

## risk_safety_deny_visibility
Minimum operator-visible signals:
- Safety/risk deny states are visible as blockers, degraded states, or explicit alerts/incidents.
- Operator can determine whether writes/live progression must remain blocked.

Operational checks:
- Runtime/reviewer outputs surface safety deny conditions.
- Deny conditions are not represented only by debug logs.

## reconcile_recovery_visibility
Minimum operator-visible signals:
- Reconcile chain visibility (submitted vs reconciled/finalized) is available.
- Recovery-critical mismatches are visible (e.g., unresolved submitted or finalized gaps).

Operational checks:
- Correlation/reconcile evidence appears in runtime context and/or diagnostics.
- Persistent mismatch signals are visible in alerts/incidents.

## anomaly_incident_visibility
Minimum operator-visible signals:
- Anomaly findings are visible as alerts with severity.
- Critical/high conditions can open incidents and are not silently suppressed.

Operational checks:
- Reviewer findings can be observed in alert snapshot.
- Incident lifecycle (open/close) is visible.

## alert_pipeline_deliverability_visibility
Minimum operator-visible signals:
- Alert transport state is explicit: enabled/disabled, deliverable/degraded, reason.
- Silent failure risk (enabled but undeliverable) is visible to operators.

Operational checks:
- Alert pipeline state includes deliverability and reason fields.
- Last delivery success/failure metadata is visible.

## diagnostics_export_bundle_minimum
Minimum operator-visible signals:
- Diagnostics export bundle includes at least:
  - `health.json`
  - `metrics.json`
  - `alerts.json`
  - `incidents.json`
  - `runtime_state.json`
  - `recent_orders.json`
  - `recent_audit.json`
  - `forensics_review.json`

Operational checks:
- Missing required sections is treated as observability evidence gap.

## kill_switch_lockdown_emergency_visibility
Minimum operator-visible signals:
- Emergency stop / lockdown / kill-switch states are visible as explicit operational state.
- Emergency-deny conditions are operator-observable before any live progression.

Operational checks:
- Runtime/safety state exposes emergency mode and blocking impact.
- Emergency states are not inferred only from internal logs.

## non_negotiable_rule
Non-negotiable:
- If any section in this document is missing from source control or removed from this document, the observability minimum check must fail closed.
