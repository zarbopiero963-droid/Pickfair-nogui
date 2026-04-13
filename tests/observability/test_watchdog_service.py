from observability.alerts_manager import AlertsManager
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry
from observability.watchdog_service import WatchdogService
from tests.helpers.watchdog_fakes import (
    FakeAnomalyEngineSequence,
    get_alert,
    normalize_alerts_snapshot,
)


class _ProbeStub:
    def collect_health(self):
        return {"runtime": {"status": "READY", "reason": "ok", "details": {}}}

    def collect_metrics(self):
        return {}

    def collect_runtime_state(self):
        return {}


class _SnapshotStub:
    def collect_and_store(self):
        return None


def _make_watchdog(**kwargs):
    defaults = dict(
        probe=_ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=AlertsManager(),
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        interval_sec=60.0,
    )
    defaults.update(kwargs)
    return WatchdogService(**defaults)


def test_watchdog_resolves_stale_anomaly_alert_without_touching_unrelated_alerts():
    alerts = AlertsManager()
    alerts.upsert_alert("SYSTEM_WARN", "warning", "keep me", source="system")

    watchdog = WatchdogService(
        probe=_ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        anomaly_engine=FakeAnomalyEngineSequence(),
        interval_sec=60.0,
    )

    watchdog._evaluate_anomalies()
    first_snapshot = normalize_alerts_snapshot(alerts.snapshot())

    first_stuck = get_alert(first_snapshot, "STUCK_INFLIGHT")
    assert first_stuck is not None
    assert first_stuck["active"] is True
    assert first_stuck["severity"] == "HIGH"

    first_system_warn = get_alert(first_snapshot, "SYSTEM_WARN")
    assert first_system_warn is not None
    assert first_system_warn["active"] is True
    assert first_system_warn.get("source") == "system"

    watchdog._evaluate_anomalies()
    second_snapshot = normalize_alerts_snapshot(alerts.snapshot())

    second_stuck = get_alert(second_snapshot, "STUCK_INFLIGHT")
    assert second_stuck is not None
    assert second_stuck["active"] is False

    second_system_warn = get_alert(second_snapshot, "SYSTEM_WARN")
    assert second_system_warn is not None
    assert second_system_warn["active"] is True
    assert second_system_warn.get("source") == "system"


# ---------------------------------------------------------------------------
# New tests: invariant, correlation, forensics integration
# ---------------------------------------------------------------------------

def test_watchdog_tick_calls_invariant_pass():
    """_evaluate_invariants() runs during tick and produces alerts for violations."""
    alerts = AlertsManager()
    health = HealthRegistry()
    metrics = MetricsRegistry()

    # Set up a broken runtime status to trigger the runtime_status_known invariant
    # via probe (collect_runtime_state returns the state used for invariants)
    class _BrokenProbe(_ProbeStub):
        def collect_runtime_state(self):
            return {"runtime": {"status": "BROKEN"}, "metrics": {"inflight_count": 0}}

    watchdog = WatchdogService(
        probe=_BrokenProbe(),
        health_registry=health,
        metrics_registry=metrics,
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        interval_sec=60.0,
    )

    watchdog._evaluate_invariants()

    active = alerts.active_alerts()
    invariant_alerts = [a for a in active if a.get("source") == "invariant_reviewer"]
    assert len(invariant_alerts) >= 1
    codes = {a["code"] for a in invariant_alerts}
    assert "runtime_status_known" in codes


def test_watchdog_tick_calls_correlation_pass():
    """_evaluate_correlations() runs during tick and produces alerts for findings."""
    alerts = AlertsManager()

    class _CorrelationProbe(_ProbeStub):
        def collect_runtime_state(self):
            return {
                "recent_orders": [
                    {"order_id": "o1", "status": "OPEN", "remote_status": "CANCELLED"},
                ]
            }

        def collect_health(self):
            return {"runtime": {"status": "READY", "reason": "ok", "details": {}}}

        def collect_metrics(self):
            return {}

    watchdog = WatchdogService(
        probe=_CorrelationProbe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        anomaly_context_provider=lambda: {
            "recent_orders": [
                {"order_id": "o1", "status": "OPEN", "remote_status": "CANCELLED"},
            ]
        },
        interval_sec=60.0,
    )

    watchdog._evaluate_correlations()

    active = alerts.active_alerts()
    correlation_alerts = [a for a in active if a.get("source") == "correlation_reviewer"]
    assert len(correlation_alerts) >= 1
    codes = {a["code"] for a in correlation_alerts}
    assert "LOCAL_VS_REMOTE_MISMATCH" in codes


def test_watchdog_tick_calls_forensics_pass():
    """_evaluate_forensics() runs during tick and produces alerts for findings."""
    alerts = AlertsManager()

    class _ForensicsProbe(_ProbeStub):
        def collect_forensics_evidence(self):
            return {
                "recent_orders": [
                    {"order_id": "o1", "status": "FAILED", "remote_bet_id": "ext-123"},
                ]
            }

    watchdog = WatchdogService(
        probe=_ForensicsProbe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        interval_sec=60.0,
    )

    watchdog._evaluate_forensics()

    active = alerts.active_alerts()
    forensics_alerts = [a for a in active if a.get("source") == "forensics_reviewer"]
    assert len(forensics_alerts) >= 1
    codes = {a["code"] for a in forensics_alerts}
    assert "FAILED_BUT_REMOTE_EXISTS" in codes


def test_stale_invariant_alert_resolves_cleanly():
    """When an invariant violation clears, its alert is resolved on the next evaluation."""
    alerts = AlertsManager()
    health = HealthRegistry()
    metrics = MetricsRegistry()

    broken_state = {"runtime": {"status": "BROKEN"}, "metrics": {"inflight_count": 0}}
    good_state = {"runtime": {"status": "READY"}, "metrics": {"inflight_count": 0}}
    probe_state = {"state": broken_state}

    class _DynamicProbe(_ProbeStub):
        def collect_runtime_state(self):
            return dict(probe_state["state"])

    watchdog = WatchdogService(
        probe=_DynamicProbe(),
        health_registry=health,
        metrics_registry=metrics,
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        interval_sec=60.0,
    )

    # First tick: violation present
    watchdog._evaluate_invariants()
    active_after_first = {a["code"] for a in alerts.active_alerts() if a.get("source") == "invariant_reviewer"}
    assert "runtime_status_known" in active_after_first

    # Fix the state
    probe_state["state"] = good_state

    # Second tick: violation cleared → alert should resolve
    watchdog._evaluate_invariants()
    active_after_second = {a["code"] for a in alerts.active_alerts() if a.get("source") == "invariant_reviewer"}
    assert "runtime_status_known" not in active_after_second


def test_stale_correlation_alert_resolves_cleanly():
    """When a correlation finding clears, its alert is resolved on the next evaluation."""
    alerts = AlertsManager()

    # Inject a mismatched order into the context provider, then clear it
    context_orders = [{"order_id": "o1", "status": "OPEN", "remote_status": "CANCELLED"}]

    watchdog = WatchdogService(
        probe=_ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        anomaly_context_provider=lambda: {"recent_orders": list(context_orders)},
        interval_sec=60.0,
    )

    watchdog._evaluate_correlations()
    active_first = {a["code"] for a in alerts.active_alerts() if a.get("source") == "correlation_reviewer"}
    assert "LOCAL_VS_REMOTE_MISMATCH" in active_first

    # Clear the mismatch
    context_orders.clear()

    watchdog._evaluate_correlations()
    active_second = {a["code"] for a in alerts.active_alerts() if a.get("source") == "correlation_reviewer"}
    assert "LOCAL_VS_REMOTE_MISMATCH" not in active_second


def test_invariant_violation_opens_incident():
    """A critical invariant violation opens an incident."""
    incidents = IncidentsManager()

    class _RegressionProbe(_ProbeStub):
        def collect_runtime_state(self):
            # terminal_to_nonterminal_regression contains "regression" in code
            return {
                "runtime": {"status": "READY"},
                "metrics": {"inflight_count": 0},
                "recent_orders": [
                    {
                        "order_id": "o1",
                        "prev_status": "COMPLETED",
                        "status": "PENDING",
                    }
                ],
            }

    watchdog = WatchdogService(
        probe=_RegressionProbe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=AlertsManager(),
        incidents_manager=incidents,
        snapshot_service=_SnapshotStub(),
        interval_sec=60.0,
    )

    watchdog._evaluate_invariants()

    snap = incidents.snapshot()
    open_incidents = {i["code"] for i in snap["incidents"] if i["status"] == "OPEN"}
    assert "terminal_to_nonterminal_regression" in open_incidents


def test_stale_anomaly_incident_closes_when_anomaly_clears():
    """Critical anomaly opens an incident; when the anomaly clears, the incident is closed."""
    alerts = AlertsManager()
    incidents = IncidentsManager()

    class _CriticalThenClearEngine:
        def __init__(self):
            self.step = 0

        def evaluate(self, _context):
            self.step += 1
            if self.step == 1:
                return [{"code": "FINANCIAL_DRIFT", "severity": "critical",
                         "description": "drift detected", "details": {}}]
            return []

    watchdog = WatchdogService(
        probe=_ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=_SnapshotStub(),
        anomaly_engine=_CriticalThenClearEngine(),
        anomaly_enabled=True,
        interval_sec=60.0,
    )

    # First evaluation: anomaly fires → alert active, incident OPEN
    watchdog._evaluate_anomalies()
    snap1 = incidents.snapshot()
    open1 = {i["code"] for i in snap1["incidents"] if i["status"] == "OPEN"}
    assert "FINANCIAL_DRIFT" in open1

    # Second evaluation: anomaly gone → alert resolved, incident CLOSED
    watchdog._evaluate_anomalies()
    snap2 = incidents.snapshot()
    open2 = {i["code"] for i in snap2["incidents"] if i["status"] == "OPEN"}
    assert "FINANCIAL_DRIFT" not in open2


def test_forensics_finding_in_default_tick():
    """Forensics findings flow through _evaluate_forensics in a full tick scenario."""
    alerts = AlertsManager()
    incidents = IncidentsManager()

    class _FullTickProbe(_ProbeStub):
        def collect_forensics_evidence(self):
            return {
                "recent_orders": [
                    {"order_id": "f1", "status": "FAILED", "remote_bet_id": "ext-999"},
                ]
            }

    watchdog = WatchdogService(
        probe=_FullTickProbe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=_SnapshotStub(),
        interval_sec=60.0,
    )

    watchdog._tick()

    active = alerts.active_alerts()
    forensics_alerts = [a for a in active if a.get("source") == "forensics_reviewer"]
    assert any(a["code"] == "FAILED_BUT_REMOTE_EXISTS" for a in forensics_alerts)

    snap = incidents.snapshot()
    open_codes = {i["code"] for i in snap["incidents"] if i["status"] == "OPEN"}
    assert "FAILED_BUT_REMOTE_EXISTS" in open_codes


# ---------------------------------------------------------------------------
# Task: reviewer_invariant_fail_loud
# ---------------------------------------------------------------------------

def test_invariant_misconfigured_zero_checks_emits_alert():
    """When invariant_checks=[] (zero checks), watchdog must emit a structured alert
    so the misconfiguration is visible instead of silently producing no findings."""
    alerts = AlertsManager()

    watchdog = WatchdogService(
        probe=_ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        invariant_checks=[],  # explicitly empty → misconfiguration
        interval_sec=60.0,
    )

    watchdog._evaluate_invariants()

    active = alerts.active_alerts()
    codes = {a["code"] for a in active}
    assert "INVARIANT_CHECKS_MISCONFIGURED" in codes, \
        "empty invariant_checks must emit INVARIANT_CHECKS_MISCONFIGURED alert"


def test_invariant_default_checks_none_does_not_emit_misconfigured_alert():
    """When invariant_checks=None (use defaults), no misconfigured alert is emitted."""
    alerts = AlertsManager()

    watchdog = WatchdogService(
        probe=_ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        invariant_checks=None,  # use defaults (9 checks)
        interval_sec=60.0,
    )

    watchdog._evaluate_invariants()

    active = alerts.active_alerts()
    codes = {a["code"] for a in active if a.get("source") == "invariant_reviewer"}
    assert "INVARIANT_CHECKS_MISCONFIGURED" not in codes


# ---------------------------------------------------------------------------
# Task: reviewer_forensics_rule_isolation (watchdog integration)
# ---------------------------------------------------------------------------

def test_watchdog_forensics_bad_rule_does_not_silence_other_findings():
    """A forensics rule that raises must not prevent other rules from emitting alerts
    via the watchdog tick. The tick itself must remain operational."""
    from observability.forensics_engine import ForensicsEngine

    alerts = AlertsManager()
    incidents = IncidentsManager()

    def _exploding_rule(context, state):
        raise RuntimeError("forensics rule crashed")

    def _finding_rule(context, state):
        return {"code": "FORENSIC_SENTINEL", "severity": "critical",
                "message": "sentinel finding", "details": {}}

    forensics_engine = ForensicsEngine([_exploding_rule, _finding_rule])

    watchdog = WatchdogService(
        probe=_ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=_SnapshotStub(),
        forensics_engine=forensics_engine,
        interval_sec=60.0,
    )

    watchdog._evaluate_forensics()

    active = alerts.active_alerts()
    codes = {a["code"] for a in active}
    assert "FORENSIC_SENTINEL" in codes, \
        "finding_rule must still fire even though exploding_rule raised"

    snap = incidents.snapshot()
    open_codes = {i["code"] for i in snap["incidents"] if i["status"] == "OPEN"}
    assert "FORENSIC_SENTINEL" in open_codes


# ---------------------------------------------------------------------------
# Task: reviewer_strong_collectors — correlation context enrichment via probe
# ---------------------------------------------------------------------------

def test_evaluate_correlations_enriches_context_from_probe_collect_correlation_context():
    """_evaluate_correlations must call probe.collect_correlation_context() and merge
    direct typed evidence into the correlation context so rules can fire on real data."""
    from observability.correlation_engine import CorrelationEvaluator

    alerts = AlertsManager()
    fired_contexts = []

    def _spy_rule(ctx, state):
        fired_contexts.append(dict(ctx))
        return None

    class _DirectEvidenceProbe(_ProbeStub):
        def collect_correlation_context(self):
            return {
                "event_bus": {"queue_depth": 7, "published_total": 99},
                "db_write_queue": {"queue_depth": 3, "failed": 1},
            }

    watchdog = WatchdogService(
        probe=_DirectEvidenceProbe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        interval_sec=60.0,
    )
    # Replace evaluator with one using our spy rule
    watchdog._correlation_evaluator = CorrelationEvaluator([_spy_rule])

    watchdog._evaluate_correlations()

    assert len(fired_contexts) == 1
    ctx = fired_contexts[0]
    assert ctx.get("event_bus", {}).get("queue_depth") == 7
    assert ctx.get("event_bus", {}).get("published_total") == 99
    assert ctx.get("db_write_queue", {}).get("queue_depth") == 3


def test_evaluate_correlations_direct_evidence_wins_over_loose_gauge():
    """When probe provides event_bus.queue_depth, it overrides the loose gauge value
    that was already present in the base context from _build_anomaly_context()."""
    from observability.correlation_engine import CorrelationEvaluator

    captured = []

    def _capture_rule(ctx, state):
        captured.append(dict(ctx))
        return None

    class _ConflictingProbe(_ProbeStub):
        def collect_correlation_context(self):
            # Direct evidence says queue_depth=10
            return {"event_bus": {"queue_depth": 10}}

    watchdog = WatchdogService(
        probe=_ConflictingProbe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=AlertsManager(),
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        anomaly_context_provider=lambda: {"event_bus": {"queue_depth": 0, "events_published": 5}},
        interval_sec=60.0,
    )
    watchdog._correlation_evaluator = CorrelationEvaluator([_capture_rule])

    watchdog._evaluate_correlations()

    assert len(captured) == 1
    # Direct evidence (10) wins over the loose injected value (0)
    assert captured[0]["event_bus"]["queue_depth"] == 10
    # Loose injected field not clobbered if not in direct evidence
    assert captured[0]["event_bus"]["events_published"] == 5
