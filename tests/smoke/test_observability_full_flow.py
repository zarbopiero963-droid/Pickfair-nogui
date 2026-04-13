from pathlib import Path
import zipfile
import json

import pytest

from observability.alerts_manager import AlertsManager
from observability.diagnostic_bundle_builder import DiagnosticBundleBuilder
from observability.diagnostics_service import DiagnosticsService
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry
from observability.watchdog_service import WatchdogService


class FlowProbe:
    def collect_runtime_state(self):
        return {"mode": "smoke-flow"}

    def collect_health(self):
        return {"engine": {"status": "READY", "reason": "ok", "details": {}}}

    def collect_metrics(self):
        return {"memory_rss_mb": 20.0, "inflight_count": 2.0}


class DummyDb:
    def __init__(self):
        self.exports = []

    def register_diagnostics_export(self, path):
        self.exports.append(path)

    def get_recent_orders_for_diagnostics(self, limit=200):
        _ = limit
        return [{"id": "O1", "status": "ok"}]

    def get_recent_audit_events_for_diagnostics(self, limit=500):
        _ = limit
        return [{"id": "A1", "type": "evt"}]


class SnapshotCollector:
    def __init__(self, db, probe, health, metrics, alerts, incidents):
        self.db = db
        self.probe = probe
        self.health = health
        self.metrics = metrics
        self.alerts = alerts
        self.incidents = incidents
        self.calls = 0

    def collect_and_store(self):
        self.calls += 1


@pytest.mark.smoke
def test_observability_full_flow_smoke(tmp_path):
    db = DummyDb()
    probe = FlowProbe()
    health = HealthRegistry()
    metrics = MetricsRegistry()
    alerts = AlertsManager()
    incidents = IncidentsManager()

    snapshot_collector = SnapshotCollector(db, probe, health, metrics, alerts, incidents)

    watchdog = WatchdogService(
        probe=probe,
        health_registry=health,
        metrics_registry=metrics,
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=snapshot_collector,
        interval_sec=0.01,
    )
    watchdog._tick()

    service = DiagnosticsService(
        builder=DiagnosticBundleBuilder(export_dir=str(tmp_path / "exports")),
        probe=probe,
        health_registry=health,
        metrics_registry=metrics,
        alerts_manager=alerts,
        incidents_manager=incidents,
        db=db,
        safe_mode=None,
        log_paths=[],
    )

    bundle = service.export_bundle()

    assert snapshot_collector.calls >= 1
    assert Path(bundle).exists()
    assert db.exports and db.exports[-1] == bundle
    assert health.snapshot()["overall_status"] in {"READY", "DEGRADED", "NOT_READY"}
    with zipfile.ZipFile(bundle, "r") as zf:
        assert "forensics_review.json" in set(zf.namelist())
        review = json.loads(zf.read("forensics_review.json"))
    assert "degraded_or_not_ready" in review


@pytest.mark.smoke
def test_timeout_ambiguity_contradiction_lifecycle_canonical_proof():
    trace_id = "trace-timeout-ambiguity-1"
    order_id = "ORD-TIMEOUT-AMB-1"

    state = {
        "submit_phase": {"result": "TIMEOUT", "trace_id": trace_id},
        "recent_orders": [
            {
                "order_id": order_id,
                "correlation_id": trace_id,
                "status": "AMBIGUOUS",
                "remote_status": "MATCHED",
                "remote_final_status": "SETTLED_WIN",
            }
        ],
        "recent_audit": [{"type": "REQUEST_RECEIVED", "order_id": order_id, "correlation_id": trace_id}],
    }

    class _Probe:
        def collect_runtime_state(self):
            return {
                "submit_phase": dict(state["submit_phase"]),
                "recent_orders": [dict(item) for item in state["recent_orders"]],
                "recent_audit": [dict(item) for item in state["recent_audit"]],
            }

        def collect_correlation_context(self):
            return {
                "recent_orders": [dict(item) for item in state["recent_orders"]],
                "recent_audit": [dict(item) for item in state["recent_audit"]],
            }

        def collect_forensics_evidence(self):
            return {
                "recent_orders": [dict(item) for item in state["recent_orders"]],
                "recent_audit": [dict(item) for item in state["recent_audit"]],
            }

        def collect_health(self):
            return {"runtime": {"status": "READY", "reason": "ok", "details": {"trace_id": trace_id}}}

        def collect_metrics(self):
            return {"inflight_count": 1.0}

    class _Snapshot:
        def collect_and_store(self):
            return None

    alerts = AlertsManager()
    incidents = IncidentsManager()
    watchdog = WatchdogService(
        probe=_Probe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=_Snapshot(),
        interval_sec=60.0,
    )

    watchdog._tick()

    assert state["submit_phase"]["result"] == "TIMEOUT"
    assert state["recent_orders"][0]["status"] == "AMBIGUOUS"
    assert state["recent_orders"][0]["remote_status"] == "MATCHED"

    active_alerts = {a["code"]: a for a in alerts.active_alerts()}
    assert "ambiguous_local_remote_inconsistency" in active_alerts
    assert "LOCAL_VS_REMOTE_MISMATCH" in active_alerts
    assert "ALERT_WITHOUT_RUNTIME_CONTEXT" in active_alerts
    assert active_alerts["ambiguous_local_remote_inconsistency"]["source"] == "invariant_reviewer"
    assert active_alerts["LOCAL_VS_REMOTE_MISMATCH"]["source"] == "correlation_reviewer"
    assert active_alerts["ALERT_WITHOUT_RUNTIME_CONTEXT"]["source"] == "forensics_reviewer"
    assert active_alerts["LOCAL_VS_REMOTE_MISMATCH"]["details"]["mismatched_count"] == 1
    assert active_alerts["LOCAL_VS_REMOTE_MISMATCH"]["details"]["sample"][0]["id"] == order_id

    open_incidents = {row["code"]: row for row in incidents.snapshot()["incidents"] if row["status"] == "OPEN"}
    assert "LOCAL_VS_REMOTE_MISMATCH" in open_incidents
    assert open_incidents["LOCAL_VS_REMOTE_MISMATCH"]["severity"] == "critical"

    state["submit_phase"] = {"result": "RECONCILED", "trace_id": trace_id}
    state["recent_orders"] = [
        {
            "order_id": order_id,
            "correlation_id": trace_id,
            "status": "COMPLETED",
            "remote_status": "COMPLETED",
            "remote_final_status": "SETTLED_WIN",
        }
    ]
    # Provide runtime context once healed so forensics no longer flags context gap.
    state["runtime_context"] = {"mode": "smoke-flow"}

    class _HealedProbe(_Probe):
        def collect_runtime_state(self):
            base = super().collect_runtime_state()
            base.update(state.get("runtime_context", {}))
            return base

    watchdog.probe = _HealedProbe()
    watchdog._tick()

    healed_codes = {a["code"] for a in alerts.active_alerts()}
    assert "ambiguous_local_remote_inconsistency" not in healed_codes
    assert "LOCAL_VS_REMOTE_MISMATCH" not in healed_codes
    assert "ALERT_WITHOUT_RUNTIME_CONTEXT" not in healed_codes

    healed_open_incidents = {row["code"] for row in incidents.snapshot()["incidents"] if row["status"] == "OPEN"}
    assert "LOCAL_VS_REMOTE_MISMATCH" not in healed_open_incidents


# ---------------------------------------------------------------------------
# Micro-task 3: strong end-to-end proof — default headless collector wiring
# ---------------------------------------------------------------------------

@pytest.mark.smoke
def test_headless_direct_collector_queue_liveness_contradiction_e2e():
    """End-to-end proof: when RuntimeProbe is wired with a direct EventBus reference
    (as in the default headless path), queue depth + dispatcher-down evidence flows
    through collect_correlation_context → _evaluate_correlations → alert/incident lifecycle.

    Proves the correlation reviewer acts on real direct evidence rather than loose
    gauge-only heuristics.
    """
    from observability.runtime_probe import RuntimeProbe

    class _DirectEventBus:
        """Simulates a live EventBus that has pending work but a dead dispatcher."""
        def queue_depth(self) -> int:
            return 5  # pending items

        def published_total_count(self) -> int:
            return 20

        def delivered_total_count(self) -> int:
            return 17

        def subscriber_error_counts(self) -> dict:
            return {}

        # Dispatcher is not running — worker pool is down
        _running = False
        _workers: list = []

    class _Snapshot:
        def collect_and_store(self):
            return None

    probe = RuntimeProbe(event_bus=_DirectEventBus())
    alerts = AlertsManager()
    incidents = IncidentsManager()

    watchdog = WatchdogService(
        probe=probe,
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=_Snapshot(),
        interval_sec=60.0,
    )

    # Verify direct evidence is collected by the probe
    direct_ctx = probe.collect_correlation_context()
    assert direct_ctx["event_bus"]["queue_depth"] == 5
    assert direct_ctx["event_bus"]["running"] is False
    assert direct_ctx["event_bus"]["worker_threads_alive"] == 0

    # Run the full correlation evaluation path
    watchdog._evaluate_correlations()

    active = alerts.active_alerts()
    codes = {a["code"] for a in active if a.get("source") == "correlation_reviewer"}
    assert "QUEUE_DEPTH_DISPATCHER_CONTRADICTION" in codes, (
        "direct EventBus evidence must trigger queue/dispatcher contradiction "
        "through the default headless collector path"
    )

    # Critical finding must also open an incident
    open_codes = {
        i["code"] for i in incidents.snapshot()["incidents"] if i["status"] == "OPEN"
    }
    assert "QUEUE_DEPTH_DISPATCHER_CONTRADICTION" in open_codes


@pytest.mark.smoke
def test_headless_direct_collector_db_write_queue_evidence_e2e():
    """End-to-end proof: when RuntimeProbe is wired with a direct AsyncDBWriter
    reference (as in the default headless path), write-queue backlog + DB inflight
    evidence flows through collect_correlation_context into the correlation reviewer.

    Also verifies DB-vs-memory mismatch detection using the diagnostics_recent_orders
    direct source from RuntimeProbe.
    """
    from observability.runtime_probe import RuntimeProbe
    from queue import Queue

    class _FakeDBWriter:
        """Simulates AsyncDBWriter with a non-empty backlog queue."""
        def __init__(self):
            self.queue = Queue()
            for _ in range(8):  # 8 pending writes
                self.queue.put({"kind": "bet", "payload": {}, "retries": 0})
            self._written = 50
            self._failed = 3
            self._dropped = 1

    class _FakeDB:
        """Simulates DB with more inflight orders than in-memory gauge sees."""
        def get_recent_orders_for_diagnostics(self, limit=500):
            # 3 non-terminal orders → db_state.inflight_orders_count = 3
            return [
                {"status": "SUBMITTED"},
                {"status": "SUBMITTED"},
                {"status": "PENDING"},
                {"status": "FILLED"},  # terminal
            ]

        def get_recent_observability_snapshots(self, limit=1):
            return []

    class _Snapshot:
        def collect_and_store(self):
            return None

    probe = RuntimeProbe(db=_FakeDB(), async_db_writer=_FakeDBWriter())
    alerts = AlertsManager()
    incidents = IncidentsManager()

    watchdog = WatchdogService(
        probe=probe,
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=_Snapshot(),
        interval_sec=60.0,
    )

    # Verify direct evidence is collected by the probe
    direct_ctx = probe.collect_correlation_context()
    assert "db_write_queue" in direct_ctx
    assert direct_ctx["db_write_queue"]["queue_depth"] == 8
    assert direct_ctx["db_write_queue"]["failed"] == 3
    assert "db_state" in direct_ctx
    assert direct_ctx["db_state"]["inflight_orders_count"] == 3

    # Run correlations — db_vs_memory rule fires because DB sees 3 inflight
    # but in-memory gauge is 0 (no trading engine providing metric)
    watchdog._evaluate_correlations()

    active = alerts.active_alerts()
    codes = {a["code"] for a in active if a.get("source") == "correlation_reviewer"}
    assert "DB_VS_MEMORY_MISMATCH" in codes, (
        "direct DB writer + db_state evidence must flow through the default "
        "headless collector path and trigger DB/memory mismatch correlation"
    )
    mismatch_alert = next(a for a in active if a["code"] == "DB_VS_MEMORY_MISMATCH")
    # Direct db_write_queue evidence must be reflected in the finding details
    assert "db_write_queue_depth" in mismatch_alert["details"]
    assert mismatch_alert["details"]["db_write_queue_depth"] == 8
    assert mismatch_alert["details"]["db_source"] == "diagnostics_recent_orders"


@pytest.mark.smoke
def test_default_headless_reviewer_path_proves_critical_anomaly_and_chain_families_e2e():
    """End-to-end proof for default headless/runtime reviewer path coverage.

    Verifies the default RuntimeProbe -> WatchdogService path operationalizes:
    - EXPOSURE_MISMATCH
    - DB_CONTENTION_DETECTED
    - FINANCIAL_DRIFT (critical -> incident lifecycle)
    - EVENT_FANOUT_INCOMPLETE
    - SUBMIT_RECONCILE_CHAIN_BREAK

    Also verifies alert lifecycle (active -> resolved) and incident lifecycle where
    severity warrants it.
    """
    from queue import Queue

    from observability.runtime_probe import RuntimeProbe

    class _ReadyComponent:
        def is_ready(self):
            return True

    class _TradingEngine:
        _inflight_keys = []
        _seen_correlation_ids = []

        def readiness(self):
            return {"ok": True}

    class _Writer:
        def __init__(self):
            self.queue = Queue()
            for _ in range(4):
                self.queue.put({"kind": "write"})
            self._failed = 2
            self._dropped = 1

    class _Db:
        def __init__(self):
            self._submitted_missing = True

        def get_recent_orders_for_diagnostics(self, limit=200):
            del limit
            if self._submitted_missing:
                return [{"order_id": "o-chain-1", "status": "SUBMITTED"}]
            return [{"order_id": "o-chain-1", "status": "COMPLETED"}]

        def get_recent_audit_events_for_diagnostics(self, limit=300):
            del limit
            if self._submitted_missing:
                return []
            return [{"order_id": "o-chain-1"}]

        def get_recent_observability_snapshots(self, limit=1):
            del limit
            return []

    class _EventBus:
        def __init__(self):
            self._errors = {"fanout_handler": 2}

        def queue_depth(self):
            return 0

        def published_total_count(self):
            return 10

        def delivered_total_count(self):
            return 8

        def subscriber_error_counts(self):
            return dict(self._errors)

    class _RiskDesk:
        bankroll_current = 1000.0

    class _TableManager:
        def total_exposure(self):
            return 120.0

    class _RuntimeController:
        table_manager = _TableManager()
        risk_desk = _RiskDesk()

    class _Probe(RuntimeProbe):
        def __init__(self, *, db, writer, event_bus, runtime_controller):
            super().__init__(
                db=db,
                async_db_writer=writer,
                event_bus=event_bus,
                runtime_controller=runtime_controller,
                trading_engine=_TradingEngine(),
                betfair_service=_ReadyComponent(),
                safe_mode=_ReadyComponent(),
                shutdown_manager=_ReadyComponent(),
            )

        def collect_reviewer_context(self):
            ctx = super().collect_reviewer_context()
            # Force drift and exposure mismatch on the canonical default context block.
            ctx["risk"]["actual_exposure"] = 95.0
            ctx["financials"]["venue_balance"] = 900.0
            return ctx

    class _Snapshot:
        def collect_and_store(self):
            return None

    db = _Db()
    writer = _Writer()
    event_bus = _EventBus()
    probe = _Probe(db=db, writer=writer, event_bus=event_bus, runtime_controller=_RuntimeController())

    alerts = AlertsManager()
    incidents = IncidentsManager()
    watchdog = WatchdogService(
        probe=probe,
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=_Snapshot(),
        anomaly_enabled=True,
        interval_sec=60.0,
    )

    # Tick 1: degraded state with chain break/fanout/exposure/db contention/drift.
    watchdog._tick()

    active_alerts = {a["code"]: a for a in alerts.active_alerts()}
    assert "EXPOSURE_MISMATCH" in active_alerts
    assert "DB_CONTENTION_DETECTED" in active_alerts
    assert "FINANCIAL_DRIFT" in active_alerts
    assert "EVENT_FANOUT_INCOMPLETE" in active_alerts
    assert "SUBMIT_RECONCILE_CHAIN_BREAK" in active_alerts

    assert active_alerts["EXPOSURE_MISMATCH"]["source"] == "anomaly"
    assert active_alerts["DB_CONTENTION_DETECTED"]["source"] == "anomaly"
    assert active_alerts["FINANCIAL_DRIFT"]["source"] == "anomaly"
    assert active_alerts["EVENT_FANOUT_INCOMPLETE"]["source"] == "anomaly"
    assert active_alerts["SUBMIT_RECONCILE_CHAIN_BREAK"]["source"] == "correlation_reviewer"

    open_incidents = {
        row["code"]: row for row in incidents.snapshot()["incidents"] if row["status"] == "OPEN"
    }
    assert "FINANCIAL_DRIFT" in open_incidents
    assert open_incidents["FINANCIAL_DRIFT"]["severity"].lower() == "critical"
    # Warning-level families must not force incidents.
    assert "EXPOSURE_MISMATCH" not in open_incidents
    assert "DB_CONTENTION_DETECTED" not in open_incidents
    assert "EVENT_FANOUT_INCOMPLETE" not in open_incidents
    assert "SUBMIT_RECONCILE_CHAIN_BREAK" not in open_incidents

    # Tick 2: healed state; prove alert/incident lifecycle closes cleanly.
    db._submitted_missing = False
    event_bus._errors = {}
    writer._failed = 0
    writer._dropped = 0

    def _healed_collect_reviewer_context():
        healed = RuntimeProbe.collect_reviewer_context(probe)
        healed["risk"]["actual_exposure"] = healed["risk"]["expected_exposure"]
        healed["financials"]["venue_balance"] = healed["financials"]["ledger_balance"]
        return healed

    probe.collect_reviewer_context = _healed_collect_reviewer_context
    watchdog._tick()

    healed_codes = {a["code"] for a in alerts.active_alerts()}
    assert "EXPOSURE_MISMATCH" not in healed_codes
    assert "DB_CONTENTION_DETECTED" not in healed_codes
    assert "FINANCIAL_DRIFT" not in healed_codes
    assert "EVENT_FANOUT_INCOMPLETE" not in healed_codes
    assert "SUBMIT_RECONCILE_CHAIN_BREAK" not in healed_codes

    healed_open_incidents = {
        row["code"] for row in incidents.snapshot()["incidents"] if row["status"] == "OPEN"
    }
    assert "FINANCIAL_DRIFT" not in healed_open_incidents
