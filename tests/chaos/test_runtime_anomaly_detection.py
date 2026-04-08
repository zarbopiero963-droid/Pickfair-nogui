from observability.alerts_manager import AlertsManager
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry
from observability.watchdog_service import WatchdogService
from tests.helpers.fake_runtime_state import FakeRuntimeState


class _ProbeStub:
    def __init__(self, runtime_state=None):
        self.runtime_state = runtime_state

    def collect_health(self):
        return {"runtime": {"status": "READY", "reason": "ok", "details": {}}}

    def collect_metrics(self):
        return {}

    def collect_runtime_state(self):
        return self.runtime_state


class _SnapshotStub:
    def collect_and_store(self):
        return None


class _EngineStub:
    def __init__(self, response=None, raises=False):
        self.response = response if response is not None else []
        self.raises = raises
        self.calls = 0

    def evaluate(self, context):
        del context
        self.calls += 1
        if self.raises:
            raise RuntimeError("boom")
        return self.response


def _make_watchdog(*, anomaly_enabled: bool, runtime_state=None, anomaly_engine=None) -> WatchdogService:
    return WatchdogService(
        probe=_ProbeStub(runtime_state=runtime_state),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=AlertsManager(),
        incidents_manager=IncidentsManager(),
        snapshot_service=_SnapshotStub(),
        anomaly_engine=anomaly_engine,
        anomaly_enabled=anomaly_enabled,
        interval_sec=60.0,
    )


def test_anomaly_disabled_no_effect():
    engine = _EngineStub(response=[{"code": "IGNORED"}])
    watchdog = _make_watchdog(anomaly_enabled=False, anomaly_engine=engine)

    watchdog._tick()

    assert engine.calls == 0
    assert watchdog.last_anomalies == []


def test_anomaly_enabled_collects_anomalies_and_stays_alive():
    engine = _EngineStub(response=[{"code": "CONTRADICTION", "severity": "warning", "message": "bad"}])
    watchdog = _make_watchdog(
        anomaly_enabled=True,
        runtime_state={"reconcile": {"ghost_orders_count": 1}},
        anomaly_engine=engine,
    )

    watchdog._tick()

    assert engine.calls == 1
    assert any(item.get("code") == "CONTRADICTION" for item in watchdog.last_anomalies)
    assert any(item.get("code") == "GHOST_ORDER_DETECTED" for item in watchdog.last_anomalies)


def test_anomaly_hook_exception_is_contained():
    engine = _EngineStub(raises=True)
    watchdog = _make_watchdog(anomaly_enabled=True, anomaly_engine=engine)

    watchdog._tick()

    assert engine.calls == 1
    assert watchdog.last_anomalies == []


def test_anomaly_enabled_with_empty_runtime_state_is_safe():
    engine = _EngineStub(response=[])
    watchdog = _make_watchdog(anomaly_enabled=True, runtime_state={}, anomaly_engine=engine)

    anomalies = watchdog._run_anomaly_checks()

    assert anomalies == []
    assert watchdog.last_anomalies == []
    watchdog._tick()


def test_runtime_contradictions_are_expressible_deterministically():
    contradiction = (
        FakeRuntimeState.ready()
        .mark_ghost_order()
        .mark_exposure_mismatch(local_exposure=3.5, remote_exposure=1.0)
        .mark_db_contention(latency_p95=3.0, locked_errors=4)
        .mark_heartbeat_stale(age_sec=180.0)
    )

    snapshot = contradiction.to_snapshot()

    assert snapshot["runtime_state_label"] == "DEGRADED"
    assert snapshot["reason"] == "heartbeat_stale"
    assert snapshot["db_locked_errors"] == 4
    assert snapshot["local_exposure"] == 3.5
    assert snapshot["remote_exposure"] == 1.0
    assert snapshot["last_heartbeat_age"] == 180.0


def test_fake_runtime_state_rejects_invalid_override_field():
    state = FakeRuntimeState.ready()

    try:
        state.with_overrides(not_a_field=True)
    except KeyError as exc:
        assert "unsupported override fields" in str(exc)
    else:
        raise AssertionError("expected KeyError for unsupported runtime-state override")
