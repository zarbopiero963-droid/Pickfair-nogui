import time

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


def test_watchdog_handles_telegram_probe():
    class _TelegramProbe(_ProbeStub):
        def collect_runtime_state(self):
            return {
                "telegram_health": {
                    "state": "FAILED",
                    "failed": True,
                    "invariant_ok": True,
                    "intentional_stop": False,
                    "reconnect_in_progress": False,
                    "last_error": "boom",
                    "reconnect_attempts": 2,
                    "active_alert_codes": [],
                    "checked_at": "2026-04-15T00:00:30+00:00",
                }
            }

    alerts = AlertsManager()
    watchdog = _make_watchdog(probe=_TelegramProbe(), alerts_manager=alerts)

    watchdog._evaluate_alerts()

    codes = {a["code"] for a in alerts.active_alerts()}
    assert "TELEGRAM_FAILED" in codes


def test_watchdog_fallback_telegram_health_passes_checked_at_timestamp():
    class _TelegramServiceStub:
        def __init__(self):
            self.calls: list[str | None] = []

        def health_status(self, *, checked_at: str | None = None):
            self.calls.append(checked_at)
            return {
                "state": "CONNECTED",
                "failed": False,
                "invariant_ok": True,
                "intentional_stop": False,
                "reconnect_in_progress": False,
                "last_error": "",
                "reconnect_attempts": 0,
                "active_alert_codes": [],
                "checked_at": checked_at,
            }

    class _TelegramFallbackProbe(_ProbeStub):
        def __init__(self):
            self.telegram_service = _TelegramServiceStub()

    alerts = AlertsManager()
    probe = _TelegramFallbackProbe()
    watchdog = _make_watchdog(probe=probe, alerts_manager=alerts)

    watchdog._evaluate_alerts()

    assert len(probe.telegram_service.calls) == 1
    checked_at = probe.telegram_service.calls[0]
    assert isinstance(checked_at, str)
    assert checked_at
    assert "TELEGRAM_FAILED" not in {a["code"] for a in alerts.active_alerts()}


def test_watchdog_does_not_loop():
    class _SnapshotCountingStub(_SnapshotStub):
        def __init__(self):
            self.calls = 0

        def collect_and_store(self):
            self.calls += 1
            return None

    snapshot = _SnapshotCountingStub()
    watchdog = _make_watchdog(snapshot_service=snapshot)

    watchdog.tick()

    assert snapshot.calls == 1


def test_watchdog_tick_is_pure_function():
    class _PureProbe(_ProbeStub):
        def collect_runtime_state(self):
            return {
                "telegram_health": {
                    "state": "FAILED",
                    "failed": True,
                    "invariant_ok": True,
                    "intentional_stop": False,
                    "reconnect_in_progress": False,
                    "last_error": "boom",
                    "reconnect_attempts": 1,
                    "active_alert_codes": [],
                    "checked_at": "2026-04-15T00:00:35+00:00",
                }
            }

    alerts = AlertsManager()
    watchdog = _make_watchdog(probe=_PureProbe(), alerts_manager=alerts)
    watchdog._evaluate_alerts()
    first = {a["code"]: (a["severity"], a["message"]) for a in alerts.active_alerts()}

    watchdog._evaluate_alerts()
    second = {a["code"]: (a["severity"], a["message"]) for a in alerts.active_alerts()}

    assert first == second


def test_watchdog_does_not_block_execution():
    watchdog = _make_watchdog()

    started = time.perf_counter()
    watchdog.tick()
    elapsed = time.perf_counter() - started

    assert elapsed < 1.0


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


def test_watchdog_correlation_disabled_emits_structured_operational_signal():
    alerts = AlertsManager()

    watchdog = WatchdogService(
        probe=_ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        anomaly_context_provider=lambda: {"correlation_reviewer_enabled": False},
        interval_sec=60.0,
    )

    watchdog._evaluate_correlations()
    codes = {a["code"] for a in alerts.active_alerts() if a.get("source") == "correlation_reviewer"}
    assert "CORRELATION_REVIEWER_DISABLED" in codes


def test_watchdog_correlation_missing_evaluator_emits_structured_signal():
    alerts = AlertsManager()
    watchdog = _make_watchdog(alerts_manager=alerts)
    watchdog._correlation_evaluator = None

    watchdog._evaluate_correlations()
    codes = {a["code"] for a in alerts.active_alerts() if a.get("source") == "correlation_reviewer"}
    assert "CORRELATION_REVIEWER_MISSING" in codes


def test_watchdog_correlation_unavailable_evaluator_emits_structured_signal():
    alerts = AlertsManager()
    watchdog = _make_watchdog(alerts_manager=alerts)
    watchdog._correlation_evaluator = object()

    watchdog._evaluate_correlations()
    codes = {a["code"] for a in alerts.active_alerts() if a.get("source") == "correlation_reviewer"}
    assert "CORRELATION_REVIEWER_UNAVAILABLE" in codes


def test_watchdog_correlation_empty_noop_evaluator_emits_structured_signal():
    from observability.correlation_engine import CorrelationEvaluator

    alerts = AlertsManager()
    watchdog = _make_watchdog(alerts_manager=alerts)
    watchdog._correlation_evaluator = CorrelationEvaluator([])

    watchdog._evaluate_correlations()
    codes = {a["code"] for a in alerts.active_alerts() if a.get("source") == "correlation_reviewer"}
    assert "CORRELATION_REVIEWER_EMPTY" in codes


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


def test_watchdog_governance_groups_cross_rule_execution_consistency():
    class _Probe(_ProbeStub):
        def collect_runtime_state(self):
            return {
                "recent_orders": [
                    {"order_id": "O-1", "status": "AMBIGUOUS", "remote_status": "MATCHED", "remote_final_status": "SETTLED_WIN"},
                ],
                "reconcile": {"ghost_orders_count": 1, "event_key": "O-1"},
                "alert_pipeline": {"alerts_enabled": True, "sender_available": True, "deliverable": True, "last_delivery_ok": True},
            }

        def collect_correlation_context(self):
            return {
                "recent_orders": [
                    {"order_id": "O-1", "status": "AMBIGUOUS", "remote_status": "MATCHED"},
                ]
            }

    alerts = AlertsManager()
    incidents = IncidentsManager()
    watchdog = WatchdogService(
        probe=_Probe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=_SnapshotStub(),
        interval_sec=60.0,
    )

    watchdog.tick()
    gov_alerts = [a for a in alerts.active_alerts() if a.get("source") == "reviewer_governance"]
    execution = [a for a in gov_alerts if "execution_consistency_incident" in a["code"]]
    assert len(execution) == 1
    details = execution[0]["details"]
    assert details["incident_class"] == "execution_consistency_incident"
    assert details["normalized_severity"] == "critical"
    assert details["delivery_required"] is True
    assert details["delivery_policy_class"] == "mandatory_delivery_with_degraded_fallback_state"
    assert "LOCAL_VS_REMOTE_MISMATCH" in details["triggering_finding_codes"]
    assert "GHOST_ORDER_DETECTED" in details["triggering_finding_codes"]


def test_watchdog_governance_fail_closed_when_mandatory_delivery_degraded():
    class _Probe(_ProbeStub):
        def collect_runtime_state(self):
            return {
                "recent_orders": [{"order_id": "O-2", "status": "AMBIGUOUS", "remote_status": "MATCHED"}],
                "reconcile": {"ghost_orders_count": 1, "event_key": "O-2"},
                "alert_pipeline": {"alerts_enabled": False, "sender_available": False, "deliverable": False, "last_delivery_ok": False},
            }

        def collect_correlation_context(self):
            return {"recent_orders": [{"order_id": "O-2", "status": "AMBIGUOUS", "remote_status": "MATCHED"}]}

    alerts = AlertsManager()
    incidents = IncidentsManager()
    watchdog = WatchdogService(
        probe=_Probe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=_SnapshotStub(),
        interval_sec=60.0,
    )
    watchdog.tick()
    codes = {a["code"] for a in alerts.active_alerts() if a.get("source") == "reviewer_governance"}
    degraded = [c for c in codes if c.startswith("REVIEWER_DELIVERY_DEGRADED::execution_consistency_incident")]
    assert len(degraded) == 1
    delivery_alert = [a for a in alerts.active_alerts() if a["code"] == degraded[0]][0]
    assert delivery_alert["details"]["delivery_status"] == "degraded"
    assert delivery_alert["details"]["degraded_reason"] == "alerts_disabled"


def test_watchdog_governance_open_close_reopen_lifecycle_is_deterministic():
    state = {
        "recent_orders": [{"order_id": "O-3", "status": "AMBIGUOUS", "remote_status": "MATCHED"}],
        "reconcile": {"ghost_orders_count": 1, "event_key": "O-3"},
    }

    class _Probe(_ProbeStub):
        def collect_runtime_state(self):
            return {
                "recent_orders": list(state["recent_orders"]),
                "reconcile": dict(state["reconcile"]),
                "alert_pipeline": {"alerts_enabled": True, "sender_available": True, "deliverable": True, "last_delivery_ok": True},
            }

        def collect_correlation_context(self):
            return {"recent_orders": list(state["recent_orders"])}

    alerts = AlertsManager()
    incidents = IncidentsManager()
    watchdog = WatchdogService(
        probe=_Probe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=_SnapshotStub(),
        interval_sec=60.0,
    )
    watchdog.tick()
    code = next(a["code"] for a in alerts.active_alerts() if a.get("source") == "reviewer_governance" and "execution_consistency_incident" in a["code"])
    first_open = [i for i in incidents.snapshot()["incidents"] if i["code"] == code and i["status"] == "OPEN"]
    assert len(first_open) == 1

    state["recent_orders"] = [{"order_id": "O-3", "status": "COMPLETED", "remote_status": "COMPLETED"}]
    state["reconcile"] = {"ghost_orders_count": 0, "event_key": "O-3"}
    watchdog.tick()
    assert code not in {a["code"] for a in alerts.active_alerts()}

    state["recent_orders"] = [{"order_id": "O-3", "status": "AMBIGUOUS", "remote_status": "MATCHED"}]
    state["reconcile"] = {"ghost_orders_count": 1, "event_key": "O-3"}
    watchdog.tick()
    reopened = [a for a in alerts.active_alerts() if a["code"] == code]
    assert len(reopened) == 1


class _GovernanceProbe:
    def __init__(self, state):
        self.state = state

    def collect_health(self):
        return {"runtime": {"status": "READY", "reason": "ok", "details": {}}}

    def collect_metrics(self):
        return dict(self.state.get("metrics", {}))

    def collect_runtime_state(self):
        return dict(self.state.get("runtime_state", {}))

    def collect_correlation_context(self):
        return dict(self.state.get("correlation_context", {}))

    def collect_forensics_evidence(self):
        return dict(self.state.get("forensics_evidence", {}))

    def collect_reviewer_context(self):
        return dict(self.state.get("reviewer_context", {}))


def _active_governance_by_class(alerts: AlertsManager, incident_class: str):
    return [
        row
        for row in alerts.active_alerts()
        if row.get("source") == "reviewer_governance"
        and str(row.get("code", "")).startswith("REVIEWER_GOVERNANCE::")
        and (row.get("details") or {}).get("incident_class") == incident_class
    ]


def test_financial_integrity_governance_repeated_cycles_no_flap_no_orphan():
    state = {"runtime_state": {"alert_pipeline": {"alerts_enabled": True, "sender_available": True, "deliverable": True, "last_delivery_ok": True}}}
    alerts = AlertsManager()
    incidents = IncidentsManager()
    watchdog = WatchdogService(
        probe=_GovernanceProbe(state),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=_SnapshotStub(),
        interval_sec=60.0,
    )

    for source, code in (
        ("anomaly", "EXPOSURE_MISMATCH"),
        ("anomaly", "GHOST_ORDER_DETECTED"),
    ):
        alerts.upsert_alert(
            code,
            "critical",
            code,
            source=source,
            details={"grouping_key": "order:FIN-1"},
        )
    watchdog._evaluate_reviewer_governance()
    first = _active_governance_by_class(alerts, "financial_integrity_incident")
    assert len(first) == 1
    first_code = first[0]["code"]
    first_details = first[0]["details"]
    assert first_details["normalized_severity"] == "critical"
    assert first_details["delivery_required"] is True
    assert first_details["delivery_policy_class"] == "mandatory_delivery_with_degraded_fallback_state"
    assert "EXPOSURE_MISMATCH" in first_details["triggering_finding_codes"]
    assert "GHOST_ORDER_DETECTED" in first_details["triggering_finding_codes"]

    watchdog._evaluate_reviewer_governance()
    watchdog._evaluate_reviewer_governance()
    stable = _active_governance_by_class(alerts, "financial_integrity_incident")
    assert len(stable) == 1
    assert stable[0]["code"] == first_code

    for code in ("EXPOSURE_MISMATCH", "GHOST_ORDER_DETECTED"):
        alerts.resolve_alert(code)
    watchdog._evaluate_reviewer_governance()
    assert _active_governance_by_class(alerts, "financial_integrity_incident") == []

    for source, code in (
        ("anomaly", "EXPOSURE_MISMATCH"),
        ("anomaly", "GHOST_ORDER_DETECTED"),
    ):
        alerts.upsert_alert(
            code,
            "critical",
            code,
            source=source,
            details={"grouping_key": "order:FIN-1"},
        )
    watchdog._evaluate_reviewer_governance()
    reopened = _active_governance_by_class(alerts, "financial_integrity_incident")
    assert len(reopened) == 1
    assert reopened[0]["code"] == first_code

    for code in ("EXPOSURE_MISMATCH", "GHOST_ORDER_DETECTED"):
        alerts.resolve_alert(code)
    watchdog._evaluate_reviewer_governance()

    assert _active_governance_by_class(alerts, "financial_integrity_incident") == []
    assert all(
        not (
            row.get("status") == "OPEN"
            and (row.get("details") or {}).get("incident_class") == "financial_integrity_incident"
        )
        for row in incidents.snapshot()["incidents"]
    )


def test_liveness_governance_reopens_deterministically_without_duplicate_noise():
    state = {"runtime_state": {"alert_pipeline": {"alerts_enabled": True, "sender_available": True, "deliverable": True, "last_delivery_ok": True}}}
    alerts = AlertsManager()
    incidents = IncidentsManager()
    watchdog = WatchdogService(
        probe=_GovernanceProbe(state),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=_SnapshotStub(),
        interval_sec=60.0,
    )

    alerts.upsert_alert(
        "QUEUE_DEPTH_LIVENESS_CONTRADICTION",
        "critical",
        "q",
        source="correlation_reviewer",
        details={"grouping_key": "svc:dispatcher"},
    )
    alerts.upsert_alert(
        "HEARTBEAT_STALE",
        "critical",
        "h",
        source="anomaly",
        details={"grouping_key": "svc:dispatcher"},
    )
    watchdog._evaluate_reviewer_governance()
    first = _active_governance_by_class(alerts, "liveness_degradation_incident")
    assert len(first) == 1
    code = first[0]["code"]
    assert first[0]["details"]["normalized_severity"] in {"high", "critical"}

    watchdog._evaluate_reviewer_governance()
    watchdog._evaluate_reviewer_governance()
    stable = _active_governance_by_class(alerts, "liveness_degradation_incident")
    assert len(stable) == 1
    assert stable[0]["code"] == code

    alerts.resolve_alert("QUEUE_DEPTH_LIVENESS_CONTRADICTION")
    alerts.resolve_alert("HEARTBEAT_STALE")
    watchdog._evaluate_reviewer_governance()
    assert _active_governance_by_class(alerts, "liveness_degradation_incident") == []

    alerts.upsert_alert(
        "QUEUE_DEPTH_LIVENESS_CONTRADICTION",
        "critical",
        "q",
        source="correlation_reviewer",
        details={"grouping_key": "svc:dispatcher"},
    )
    alerts.upsert_alert(
        "HEARTBEAT_STALE",
        "critical",
        "h",
        source="anomaly",
        details={"grouping_key": "svc:dispatcher"},
    )
    watchdog._evaluate_reviewer_governance()
    reopened = _active_governance_by_class(alerts, "liveness_degradation_incident")
    assert len(reopened) == 1
    assert reopened[0]["code"] == code


def test_dispatch_pipeline_governance_repeated_cycles_and_cleanup():
    state = {"runtime_state": {"alert_pipeline": {"alerts_enabled": True, "sender_available": True, "deliverable": True, "last_delivery_ok": True}}}
    alerts = AlertsManager()
    incidents = IncidentsManager()
    watchdog = WatchdogService(
        probe=_GovernanceProbe(state),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=_SnapshotStub(),
        interval_sec=60.0,
    )

    alerts.upsert_alert(
        "EVENT_SIDE_EFFECT_GAP",
        "warning",
        "gap",
        source="correlation_reviewer",
        details={"grouping_key": "svc:eventbus"},
    )
    alerts.upsert_alert(
        "POISON_PILL_SUBSCRIBER",
        "error",
        "poison",
        source="anomaly",
        details={"grouping_key": "svc:eventbus"},
    )
    watchdog._evaluate_reviewer_governance()
    first = _active_governance_by_class(alerts, "dispatch_pipeline_incident")
    assert len(first) == 1
    first_code = first[0]["code"]
    assert "EVENT_SIDE_EFFECT_GAP" in first[0]["details"]["triggering_finding_codes"]
    assert "POISON_PILL_SUBSCRIBER" in first[0]["details"]["triggering_finding_codes"]

    for _ in range(2):
        watchdog._evaluate_reviewer_governance()
    stable = _active_governance_by_class(alerts, "dispatch_pipeline_incident")
    assert len(stable) == 1
    assert stable[0]["code"] == first_code

    alerts.resolve_alert("EVENT_SIDE_EFFECT_GAP")
    alerts.resolve_alert("POISON_PILL_SUBSCRIBER")
    watchdog._evaluate_reviewer_governance()
    assert _active_governance_by_class(alerts, "dispatch_pipeline_incident") == []

    alerts.upsert_alert(
        "EVENT_SIDE_EFFECT_GAP",
        "warning",
        "gap",
        source="correlation_reviewer",
        details={"grouping_key": "svc:eventbus"},
    )
    alerts.upsert_alert(
        "POISON_PILL_SUBSCRIBER",
        "error",
        "poison",
        source="anomaly",
        details={"grouping_key": "svc:eventbus"},
    )
    watchdog._evaluate_reviewer_governance()
    reopened = _active_governance_by_class(alerts, "dispatch_pipeline_incident")
    assert len(reopened) == 1
    assert reopened[0]["code"] == first_code


def test_delivery_degradation_matrix_mandatory_vs_optional_governance():
    mandatory_template = {"runtime_state": {}}
    modes = [
        {"alerts_enabled": False, "sender_available": False, "deliverable": False, "reason": "alerts_disabled"},
        {"alerts_enabled": True, "sender_available": False, "deliverable": False, "reason": "sender_unavailable"},
        {"alerts_enabled": True, "sender_available": True, "deliverable": False, "reason": "alerts_chat_id_missing"},
        {"alerts_enabled": True, "sender_available": True, "deliverable": True, "last_delivery_ok": False, "last_delivery_error": "send_failed"},
    ]

    for idx, pipeline in enumerate(modes):
        state = dict(mandatory_template)
        state["runtime_state"] = dict(mandatory_template["runtime_state"])
        state["runtime_state"]["alert_pipeline"] = dict(pipeline)
        alerts = AlertsManager()
        watchdog = WatchdogService(
            probe=_GovernanceProbe(state),
            health_registry=HealthRegistry(),
            metrics_registry=MetricsRegistry(),
            alerts_manager=alerts,
            incidents_manager=IncidentsManager(),
            snapshot_service=_SnapshotStub(),
            interval_sec=60.0,
        )
        watchdog.tick()
        alerts.upsert_alert(
            "EXPOSURE_MISMATCH",
            "critical",
            "x",
            source="anomaly",
            details={"grouping_key": "order:D-1"},
        )
        alerts.upsert_alert(
            "GHOST_ORDER_DETECTED",
            "critical",
            "g",
            source="anomaly",
            details={"grouping_key": "order:D-1"},
        )
        watchdog._evaluate_reviewer_governance()
        grouped = _active_governance_by_class(alerts, "financial_integrity_incident")
        assert len(grouped) == 1, f"mandatory grouped incident missing for mode {idx}"
        details = grouped[0]["details"]
        assert details["delivery_required"] is True
        assert details["delivery_policy_class"] == "mandatory_delivery_with_degraded_fallback_state"
        assert details["delivery_status"] == "degraded"
        assert bool(details["degraded_reason"])
        degraded_codes = {
            a["code"]
            for a in alerts.active_alerts()
            if a.get("source") == "reviewer_governance" and a["code"].startswith("REVIEWER_DELIVERY_DEGRADED::")
        }
        assert len(degraded_codes) >= 1

    optional_state = {"runtime_state": {"alert_pipeline": {"alerts_enabled": False, "sender_available": False, "deliverable": False, "reason": "alerts_disabled"}}}
    alerts = AlertsManager()
    watchdog = WatchdogService(
        probe=_GovernanceProbe(optional_state),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        interval_sec=60.0,
    )
    alerts.upsert_alert(
        "AMBIGUOUS_SPIKE",
        "warning",
        "warn",
        source="anomaly",
        details={"grouping_key": "order:O-WARN"},
    )
    watchdog._evaluate_reviewer_governance()
    generic = _active_governance_by_class(alerts, "reviewer_generic_incident")
    assert len(generic) >= 1
    assert all((row["details"] or {}).get("delivery_required") is False for row in generic)
    assert all(not row["code"].startswith("REVIEWER_DELIVERY_DEGRADED::") for row in alerts.active_alerts())


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


def test_watchdog_default_anomaly_path_consumes_ghost_suspected_and_poison_pill():
    alerts = AlertsManager()

    class _AnomalyProbe(_ProbeStub):
        def collect_runtime_state(self):
            return {"reconcile": {"suspected_ghost_count": 1}}

    watchdog = WatchdogService(
        probe=_AnomalyProbe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        anomaly_context_provider=lambda: {
            "event_bus": {"subscriber_errors": {"toxic_sub": 4}, "poison_pill_threshold": 3}
        },
        interval_sec=60.0,
    )

    watchdog._evaluate_anomalies()
    codes = {a.get("code") for a in watchdog.last_anomalies}
    assert "GHOST_ORDER_SUSPECTED" in codes
    assert "POISON_PILL_SUBSCRIBER" in codes


def test_watchdog_default_anomaly_path_consumes_duplicate_intelligence_findings():
    alerts = AlertsManager()

    class _DuplicateProbe(_ProbeStub):
        def collect_runtime_state(self):
            return {"duplicate_guard": {"blocked_submit_streak": 4}}

    watchdog = WatchdogService(
        probe=_DuplicateProbe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        anomaly_context_provider=lambda: {
            "metrics": {"counters": {"duplicate_blocked_total": 3}},
            "recent_orders": [
                {"status": "DUPLICATE_BLOCKED", "event_key": "evt-a"},
                {"status": "DUPLICATE_BLOCKED", "event_key": "evt-a"},
                {"status": "DUPLICATE_BLOCKED", "event_key": "evt-a"},
                {"status": "DUPLICATE_BLOCKED", "event_key": "evt-b"},
            ]
        },
        anomaly_enabled=True,
        interval_sec=60.0,
    )

    watchdog._tick()
    codes = {a.get("code") for a in watchdog.last_anomalies}
    assert "DUPLICATE_BLOCK_SPIKE" in codes
    assert "SUSPICIOUS_DUPLICATE_PATTERN" in codes


def test_watchdog_correlation_default_path_uses_strong_dispatcher_liveness_evidence():
    alerts = AlertsManager()

    class _DispatcherDownProbe(_ProbeStub):
        def collect_correlation_context(self):
            return {"event_bus": {"queue_depth": 3, "running": False, "worker_threads_alive": 0}}

    watchdog = WatchdogService(
        probe=_DispatcherDownProbe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        interval_sec=60.0,
    )

    watchdog._evaluate_correlations()
    active = alerts.active_alerts()
    codes = {a["code"] for a in active if a.get("source") == "correlation_reviewer"}
    assert "QUEUE_DEPTH_DISPATCHER_CONTRADICTION" in codes


def test_build_anomaly_context_includes_probe_canonical_reviewer_blocks():
    class _CanonicalProbe(_ProbeStub):
        def collect_reviewer_context(self):
            return {
                "risk": {"expected_exposure": 9.0, "actual_exposure": 9.0, "exposure_tolerance": 0.01},
                "db": {"lock_wait_ms": 0.0, "contention_events": 0, "db_writer_backlog": 4},
                "financials": {"ledger_balance": 10.0, "venue_balance": 10.0, "drift_threshold": 0.01},
                "event_bus": {"expected_fanout": 3, "delivered_fanout": 2},
                "recent_orders": [{"order_id": "o1", "status": "SUBMITTED"}],
                "recent_audit": [],
                "reconcile_chain": {"missing_count": 1, "sample_missing_ids": ["o1"]},
                "runtime_state": {"reconcile": {"suspected_ghost_count": 1, "event_key": "evt-1"}},
            }

    watchdog = WatchdogService(
        probe=_CanonicalProbe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=AlertsManager(),
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        interval_sec=60.0,
    )

    ctx = watchdog._build_anomaly_context()
    assert ctx["risk"]["expected_exposure"] == 9.0
    assert ctx["db"]["db_writer_backlog"] == 4
    assert ctx["financials"]["ledger_balance"] == 10.0
    assert ctx["event_bus"]["expected_fanout"] == 3
    assert ctx["reconcile_chain"]["missing_count"] == 1
    assert ctx["runtime_state"]["reconcile"]["suspected_ghost_count"] == 1


def test_runtime_probe_default_path_emits_canonical_ghost_evidence_for_anomaly_context():
    from observability.runtime_probe import RuntimeProbe

    class _Rec:
        def ghost_evidence_snapshot(self):
            return {"ghost_orders_count": 2, "event_key": "batch-42", "source": "reconciliation_engine"}

    class _Trading:
        reconciliation_engine = _Rec()

        def readiness(self):
            return {"state": "READY"}

    class _Db:
        def get_recent_orders_for_diagnostics(self, limit=200):
            del limit
            return [
                {
                    "order_id": "ord-1",
                    "status": "AMBIGUOUS",
                    "remote_bet_id": "bet-100",
                    "event_key": "evt-100",
                }
            ]

        def get_recent_audit_events_for_diagnostics(self, limit=300):
            del limit
            return []

    probe = RuntimeProbe(db=_Db(), trading_engine=_Trading())
    watchdog = WatchdogService(
        probe=probe,
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=AlertsManager(),
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        interval_sec=60.0,
    )

    ctx = watchdog._build_anomaly_context()
    reconcile = ctx["runtime_state"]["reconcile"]
    assert reconcile["ghost_orders_count"] == 2
    assert reconcile["suspected_ghost_count"] >= 1
    assert reconcile["event_key"] in {"batch-42", "evt-100"}


def test_watchdog_default_path_uses_runtime_probe_liveness_metrics():
    from observability.runtime_probe import RuntimeProbe

    class _DeadWorker:
        def is_alive(self):
            return False

    class _Bus:
        _workers = [_DeadWorker()]

        def queue_depth(self):
            return 4

        def delivered_total_count(self):
            return 10

    class _Runtime:
        last_signal_at = "2000-01-01T00:00:00+00:00"

    probe = RuntimeProbe(runtime_controller=_Runtime(), event_bus=_Bus())
    alerts = AlertsManager()
    watchdog = WatchdogService(
        probe=probe,
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        interval_sec=60.0,
    )

    # First tick seeds completed_total baseline; second tick evaluates no-progress delta.
    watchdog._tick()
    watchdog._tick()

    codes = {a["code"] for a in alerts.active_alerts() if a.get("source") == "anomaly"}
    assert "HEARTBEAT_STALE" in codes
    assert "QUEUE_DEPTH_LIVENESS_MISMATCH" in codes


# ---------------------------------------------------------------------------
# Micro-task 1: anomaly reviewer default-on
# ---------------------------------------------------------------------------

def test_anomaly_reviewer_runs_by_default_with_no_settings_service():
    """When no settings_service is provided, anomaly_enabled defaults to True
    and anomaly scans run on every tick without any explicit configuration."""
    alerts = AlertsManager()

    class _AnomalyProbe(_ProbeStub):
        def collect_runtime_state(self):
            # Trigger HEARTBEAT_STALE via gauges
            return {}

    # Inject a simple engine that always returns one anomaly
    class _AlwaysOnEngine:
        def evaluate(self, _ctx):
            return [{"code": "CANARY_ANOMALY", "severity": "warning",
                     "description": "canary", "details": {}}]

    watchdog = WatchdogService(
        probe=_AnomalyProbe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        anomaly_engine=_AlwaysOnEngine(),
        # No settings_service, no explicit anomaly_enabled — must default to ON
        interval_sec=60.0,
    )

    assert watchdog._is_anomaly_enabled() is True, (
        "anomaly reviewer must be enabled by default when no settings_service is wired"
    )

    watchdog._run_anomaly_hook()
    codes = {a.get("code") for a in watchdog.last_anomalies}
    assert "CANARY_ANOMALY" in codes, (
        "anomaly rule must fire in default-on mode without any explicit configuration"
    )


def test_anomaly_reviewer_explicit_false_disables_scanning():
    """An explicit anomaly_enabled=False (e.g., from operator settings) must still
    disable scanning, but the disabled state must be visible (fail-loud logging).
    The test verifies that no anomalies are collected when explicitly off."""
    alerts = AlertsManager()

    class _AlwaysOnEngine:
        def evaluate(self, _ctx):
            return [{"code": "CANARY_ANOMALY", "severity": "warning",
                     "description": "canary", "details": {}}]

    watchdog = WatchdogService(
        probe=_ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        anomaly_engine=_AlwaysOnEngine(),
        anomaly_enabled=False,  # explicit disable
        interval_sec=60.0,
    )

    # Must report disabled
    assert watchdog._is_anomaly_enabled() is False

    # Tick must not run anomaly hook
    watchdog._tick()
    assert watchdog.last_anomalies == []


def test_anomaly_settings_none_keeps_default_on():
    """If settings_service.load_anomaly_enabled() returns None (not configured),
    anomaly reviewer keeps the default-on=True — None must not override to False."""

    class _NoneSettingsService:
        def load_anomaly_enabled(self):
            return None  # not configured in settings

    class _AlwaysOnEngine:
        def evaluate(self, _ctx):
            return [{"code": "CANARY_ANOMALY", "severity": "warning",
                     "description": "canary", "details": {}}]

    watchdog = WatchdogService(
        probe=_ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=AlertsManager(),
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        anomaly_engine=_AlwaysOnEngine(),
        settings_service=_NoneSettingsService(),
        # anomaly_enabled defaults to True
        interval_sec=60.0,
    )

    assert watchdog._is_anomaly_enabled() is True, (
        "None from settings must not override the default-on=True for anomaly reviewer"
    )


# ---------------------------------------------------------------------------
# Micro-task 4: incident lifecycle maturity — resolution metadata
# ---------------------------------------------------------------------------

def test_incident_resolution_metadata_set_on_close():
    """close_incident must record resolution_reason, resolved_by, and a resolution
    event in the incident's events list."""
    incidents = IncidentsManager()
    incidents.open_incident("TEST_CODE", "Test Incident", "warning")

    incidents.close_incident(
        "TEST_CODE",
        reason="finding_cleared",
        resolved_by="correlation_reviewer",
    )

    snap = incidents.snapshot()
    incident = next(i for i in snap["incidents"] if i["code"] == "TEST_CODE")
    assert incident["status"] == "CLOSED"
    assert incident["resolution_reason"] == "finding_cleared"
    assert incident["resolved_by"] == "correlation_reviewer"
    assert incident["closed_at"] is not None

    # Resolution event must be recorded in the events list
    assert len(incident["events"]) >= 1
    resolution_event = incident["events"][-1]
    assert "closed" in resolution_event["message"].lower() or "resolved" in resolution_event["message"].lower()
    assert resolution_event["details"]["resolved_by"] == "correlation_reviewer"


def test_incident_resolution_defaults_are_safe():
    """close_incident with no keyword args must use 'resolved' / 'system' defaults."""
    incidents = IncidentsManager()
    incidents.open_incident("DEFAULT_CODE", "Default Incident", "warning")
    incidents.close_incident("DEFAULT_CODE")

    snap = incidents.snapshot()
    incident = next(i for i in snap["incidents"] if i["code"] == "DEFAULT_CODE")
    assert incident["status"] == "CLOSED"
    assert incident["resolution_reason"] == "resolved"
    assert incident["resolved_by"] == "system"


def test_stale_anomaly_incident_resolution_carries_reason():
    """When an anomaly clears, the incident is closed with reason='anomaly_cleared'
    and resolved_by='anomaly_reviewer' — proving the reviewer-sourced lifecycle metadata."""
    alerts = AlertsManager()
    incidents = IncidentsManager()

    class _CriticalThenClear:
        def __init__(self):
            self.step = 0

        def evaluate(self, _ctx):
            self.step += 1
            if self.step == 1:
                return [{"code": "DRIFT_SENTINEL", "severity": "critical",
                         "description": "drift detected", "details": {}}]
            return []

    watchdog = WatchdogService(
        probe=_ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=_SnapshotStub(),
        anomaly_engine=_CriticalThenClear(),
        anomaly_enabled=True,
        interval_sec=60.0,
    )

    # Tick 1: anomaly fires → incident OPEN
    watchdog._evaluate_anomalies()
    snap1 = incidents.snapshot()
    assert any(i["code"] == "DRIFT_SENTINEL" and i["status"] == "OPEN" for i in snap1["incidents"])

    # Tick 2: anomaly cleared → incident CLOSED with resolution metadata
    watchdog._evaluate_anomalies()
    snap2 = incidents.snapshot()
    closed = next(i for i in snap2["incidents"] if i["code"] == "DRIFT_SENTINEL")
    assert closed["status"] == "CLOSED"
    assert closed["resolution_reason"] == "anomaly_cleared"
    assert closed["resolved_by"] == "anomaly_reviewer"
    # Resolution event must be in the events list
    assert any("closed" in e["message"].lower() for e in closed["events"])


def test_tick_keeps_invariant_and_anomaly_exposure_signals_distinct():
    """Anomaly EXPOSURE_MISMATCH and invariant EXPOSURE_MISMATCH remain distinct in alerts."""
    alerts = AlertsManager()
    incidents = IncidentsManager()

    class _ExposureMismatchProbe(_ProbeStub):
        def collect_runtime_state(self):
            return {
                "runtime": {"status": "READY"},
                "metrics": {"inflight_count": 0},
                "risk": {
                    "local_exposure": 0.0,
                    "remote_exposure": 999.0,
                    "exposure_tolerance": 0.01,
                },
            }

    class _AnomalyThenClearEngine:
        def __init__(self):
            self.step = 0

        def evaluate(self, _ctx):
            self.step += 1
            if self.step == 1:
                # Tick 1: anomaly EXPOSURE_MISMATCH fires
                return [{"code": "EXPOSURE_MISMATCH", "severity": "warning",
                         "description": "anomaly exposure drift", "details": {}}]
            # Tick 2+: anomaly EXPOSURE_MISMATCH clears
            return []

    watchdog = WatchdogService(
        probe=_ExposureMismatchProbe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=_SnapshotStub(),
        anomaly_engine=_AnomalyThenClearEngine(),
        anomaly_enabled=True,
        interval_sec=60.0,
    )

    watchdog.tick()

    active_t1 = alerts.active_alerts()
    all_codes_t1 = {a["code"] for a in active_t1}
    assert "EXPOSURE_MISMATCH" in all_codes_t1
    assert "INVARIANT_EXPOSURE_MISMATCH" in all_codes_t1
    invariant_exposure = next(a for a in active_t1 if a["code"] == "INVARIANT_EXPOSURE_MISMATCH")
    assert invariant_exposure["source"] == "invariant_reviewer"
    assert invariant_exposure.get("details", {}).get("violation_code") == "EXPOSURE_MISMATCH"

    watchdog.tick()

    all_codes_t2 = {a["code"] for a in alerts.active_alerts()}
    assert "EXPOSURE_MISMATCH" not in all_codes_t2
    assert "INVARIANT_EXPOSURE_MISMATCH" in all_codes_t2


def test_tick_is_single_pass_and_loop_free():
    import inspect

    src = inspect.getsource(WatchdogService.tick)

    assert " for " not in src
    assert " while " not in src
    assert ".tick(" not in src


def test_invariant_reviewer_fails_closed_on_missing_runtime_input():
    class _NoRuntimeStateProbe(_ProbeStub):
        def collect_runtime_state(self):
            return {}

    alerts = AlertsManager()
    incidents = IncidentsManager()
    watchdog = WatchdogService(
        probe=_NoRuntimeStateProbe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=_SnapshotStub(),
        interval_sec=60.0,
    )

    watchdog._evaluate_invariants()

    active = {a["code"] for a in alerts.active_alerts() if a.get("source") == "invariant_reviewer"}
    assert "INVARIANT_INPUT_MISSING" in active
    open_incidents = {
        i["code"] for i in incidents.snapshot()["incidents"] if i["status"] == "OPEN"
    }
    assert "INVARIANT_INPUT_MISSING" in open_incidents


# ---------------------------------------------------------------------------
# Gap 3: anomaly reviewer disabled → structured operational fail-closed signal
# ---------------------------------------------------------------------------

def test_anomaly_reviewer_disabled_emits_structured_operational_signal():
    """When anomaly_enabled=False the watchdog must emit a structured operational
    alert AND open an incident — not just log a warning.

    Proves the disabled reviewer state is fail-closed / operationally escalated,
    not merely visible suppression via log output.
    """
    alerts = AlertsManager()
    incidents = IncidentsManager()

    watchdog = WatchdogService(
        probe=_ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=_SnapshotStub(),
        anomaly_enabled=False,
        interval_sec=60.0,
    )

    watchdog.tick()

    # Structured alert must be present (not just log output).
    active_codes = {a["code"] for a in alerts.active_alerts()}
    assert "ANOMALY_REVIEWER_DISABLED" in active_codes, (
        "anomaly reviewer disabled state must emit ANOMALY_REVIEWER_DISABLED "
        "structured alert — warning log alone is insufficient"
    )

    # Source must clearly identify the disabled reviewer.
    disabled_alert = next(
        a for a in alerts.active_alerts() if a["code"] == "ANOMALY_REVIEWER_DISABLED"
    )
    assert disabled_alert["source"] == "anomaly_reviewer_disabled"

    # Structured incident must be opened so the disabled state is operationally
    # escalated beyond alert-only visibility.
    open_incidents = {
        i["code"] for i in incidents.snapshot()["incidents"] if i["status"] == "OPEN"
    }
    assert "ANOMALY_REVIEWER_DISABLED" in open_incidents, (
        "anomaly reviewer disabled state must open a structured incident "
        "to satisfy fail-closed / operationally escalated requirement"
    )


def test_anomaly_reviewer_disabled_signal_is_idempotent_across_ticks():
    """ANOMALY_REVIEWER_DISABLED alert and incident are not duplicated across ticks —
    open_incident is idempotent, upsert_alert updates count but not a new alert."""
    alerts = AlertsManager()
    incidents = IncidentsManager()

    watchdog = WatchdogService(
        probe=_ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=_SnapshotStub(),
        anomaly_enabled=False,
        interval_sec=60.0,
    )

    watchdog.tick()
    watchdog.tick()
    watchdog.tick()

    # Alert must remain active (upserted, not duplicated).
    disabled_alerts = [a for a in alerts.active_alerts() if a["code"] == "ANOMALY_REVIEWER_DISABLED"]
    assert len(disabled_alerts) == 1, "ANOMALY_REVIEWER_DISABLED must not be duplicated"

    # Incident must remain a single OPEN incident.
    disabled_incidents = [
        i for i in incidents.snapshot()["incidents"]
        if i["code"] == "ANOMALY_REVIEWER_DISABLED" and i["status"] == "OPEN"
    ]
    assert len(disabled_incidents) == 1, "ANOMALY_REVIEWER_DISABLED incident must be idempotent"


def test_anomaly_reviewer_disabled_signal_clears_after_reenable():
    """ANOMALY_REVIEWER_DISABLED alert/incident must resolve when anomaly scan is re-enabled."""
    alerts = AlertsManager()
    incidents = IncidentsManager()

    watchdog = WatchdogService(
        probe=_ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=_SnapshotStub(),
        anomaly_enabled=False,
        interval_sec=60.0,
    )

    watchdog.tick()
    assert "ANOMALY_REVIEWER_DISABLED" in {a["code"] for a in alerts.active_alerts()}
    assert "ANOMALY_REVIEWER_DISABLED" in {
        i["code"] for i in incidents.snapshot()["incidents"] if i["status"] == "OPEN"
    }

    watchdog.anomaly_enabled = True
    watchdog.tick()

    assert "ANOMALY_REVIEWER_DISABLED" not in {a["code"] for a in alerts.active_alerts()}
    assert "ANOMALY_REVIEWER_DISABLED" not in {
        i["code"] for i in incidents.snapshot()["incidents"] if i["status"] == "OPEN"
    }


def test_anomaly_reviewer_unavailable_context_is_fail_loud() -> None:
    alerts = AlertsManager()
    incidents = IncidentsManager()
    watchdog = WatchdogService(
        probe=_ProbeStub(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=_SnapshotStub(),
        anomaly_enabled=True,
        interval_sec=60.0,
    )
    watchdog._build_anomaly_context = lambda: []  # type: ignore[assignment]

    watchdog._evaluate_anomalies()
    active = {a["code"] for a in alerts.active_alerts()}
    assert "ANOMALY_REVIEWER_UNAVAILABLE" in active


def test_anomaly_reviewer_missing_runtime_state_is_fail_closed_in_strict_mode() -> None:
    class _StrictProbe(_ProbeStub):
        def collect_runtime_state(self):
            return {}

        def collect_reviewer_context(self):
            return {"anomaly_fail_closed": True}

    alerts = AlertsManager()
    watchdog = WatchdogService(
        probe=_StrictProbe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        anomaly_enabled=True,
        interval_sec=60.0,
    )

    watchdog._evaluate_anomalies()
    active = {a["code"] for a in alerts.active_alerts()}
    assert "ANOMALY_REVIEWER_INPUT_MISSING" in active


def test_anomaly_reviewer_malformed_runtime_state_is_fail_closed_in_strict_mode() -> None:
    class _StrictProbe(_ProbeStub):
        def collect_runtime_state(self):
            return "broken-shape"

        def collect_reviewer_context(self):
            return {"anomaly_fail_closed": True}

    alerts = AlertsManager()
    watchdog = WatchdogService(
        probe=_StrictProbe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        anomaly_enabled=True,
        interval_sec=60.0,
    )

    watchdog._evaluate_anomalies()
    active = {a["code"] for a in alerts.active_alerts()}
    assert "ANOMALY_REVIEWER_MISCONFIGURED" in active


def test_watchdog_exposes_external_observability_snapshot_and_metrics_text():
    class _Probe(_ProbeStub):
        def collect_metrics(self):
            return {"queue-depth": 3, "worker_alive": 1, "9bad": 2}

        def collect_runtime_state(self):
            return {"mode": "runtime"}

    watchdog = _make_watchdog(probe=_Probe())

    watchdog.tick()

    snapshot = watchdog.get_external_observability_snapshot()
    assert snapshot["version"] == 1
    assert isinstance(snapshot.get("collected_at"), float)
    assert snapshot["health"]["runtime"]["status"] == "READY"
    assert snapshot["metrics"]["worker_alive"] == 1
    assert snapshot["runtime_state"]["mode"] == "runtime"

    metrics_text = watchdog.get_external_metrics_text()
    assert "queue_depth 3.0" in metrics_text
    assert "worker_alive 1.0" in metrics_text
    assert "metric_9bad 2.0" in metrics_text
