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
