from observability.runtime_probe import RuntimeProbe
from tests.helpers.fake_runtime_state import FakeRuntimeState
from tests.helpers.fake_settings import FakeSettingsService


class _TelegramStub:
    def get_sender(self):
        return object()


class _AlertsSvcStub:
    def __init__(self, fake_state: FakeRuntimeState):
        self._state = fake_state

    def availability_status(self):
        return self._state.alert_pipeline_snapshot()


class _SafeModeStub:
    def is_enabled(self):
        return True


class _DbStub:
    def get_recent_observability_snapshots(self, limit=1):
        return [{"id": 1}]


class _RuntimeControllerNoChecker:
    pass


class _TradingEngineReadyNoHealth:
    def readiness(self):
        return {"state": "READY", "health": {}}


class _SettingsStub:
    def __init__(self, fake_state: FakeRuntimeState):
        self._state = fake_state

    def load_telegram_config_row(self):
        return {"alerts_enabled": self._state.alerts_enabled}


class _HealthProbeStub(RuntimeProbe):
    def __init__(self, status):
        super().__init__()
        self._status = status

    def collect_health(self):
        return {
            "component_a": {
                "name": "component_a",
                "status": self._status,
                "reason": "test_reason",
                "details": {},
            }
        }


def test_runtime_probe_alert_pipeline_state_uses_wired_services():
    fake_state = FakeRuntimeState.degraded(reason="sender_unavailable")
    probe = RuntimeProbe(
        db=_DbStub(),
        settings_service=_SettingsStub(fake_state),
        telegram_service=_TelegramStub(),
        telegram_alerts_service=_AlertsSvcStub(fake_state),
        safe_mode=_SafeModeStub(),
    )

    state = probe.collect_runtime_state()

    assert state["alert_pipeline"] == fake_state.alert_pipeline_snapshot()
    assert state["safe_mode_enabled"] is True


def test_collect_health_reports_unknown_with_ready_fallback_for_missing_health_checks():
    probe = RuntimeProbe(runtime_controller=_RuntimeControllerNoChecker())

    health = probe.collect_health()
    runtime_health = health["runtime_controller"]

    assert runtime_health["status"] == "UNKNOWN"
    assert runtime_health["reason"] == "no-checker"
    assert runtime_health["details"]["fallback_status"] == "READY"


def test_collect_health_reports_unknown_for_ready_state_without_health_payload():
    probe = RuntimeProbe(trading_engine=_TradingEngineReadyNoHealth())

    health = probe.collect_health()
    engine_health = health["trading_engine"]

    assert engine_health["status"] == "UNKNOWN"
    assert engine_health["reason"] == "ready_without_health"
    assert engine_health["details"]["fallback_status"] == "READY"


def test_runtime_probe_alert_pipeline_safe_on_missing_fake_settings_keys():
    probe = RuntimeProbe(
        db=_DbStub(),
        settings_service=FakeSettingsService(),
        telegram_service=_TelegramStub(),
    )

    state = probe.collect_runtime_state()

    assert state["alert_pipeline"]["alerts_enabled"] is False
    assert state["alert_pipeline"]["status"] == "DISABLED"


def test_fake_settings_snapshot_reload_round_trip():
    settings = FakeSettingsService({"anomaly_alerts_enabled": "yes"})
    settings.set("region", "eu-west")

    reloaded = FakeSettingsService.from_snapshot(settings.snapshot())

    assert reloaded.get_bool("anomaly_alerts_enabled") is True
    assert reloaded.get("region") == "eu-west"


def test_fake_runtime_state_builder_variants_are_deterministic():
    ready = FakeRuntimeState.ready()
    unknown = FakeRuntimeState.unknown(reason="no_data")

    assert ready.to_snapshot()["runtime_state_label"] == "READY"
    assert ready.alert_pipeline_snapshot()["status"] == "READY"
    assert unknown.to_snapshot()["runtime_state_label"] == "UNKNOWN"
    assert unknown.alert_pipeline_snapshot()["status"] == "DISABLED"


def test_unknown_status_is_blocker():
    probe = _HealthProbeStub("BROKEN")

    report = probe.get_live_readiness_report()

    assert report["level"] == "NOT_READY"
    assert report["ready"] is False
    assert report["blockers"][0]["name"] == "component_a"
    assert "UNRECOGNIZED_STATE::BROKEN" in report["blockers"][0]["reason"]


def test_starting_status_is_blocker():
    probe = _HealthProbeStub("STARTING")

    report = probe.get_live_readiness_report()

    assert report["level"] == "NOT_READY"
    assert report["ready"] is False
    assert report["blockers"][0]["name"] == "component_a"
    assert report["blockers"][0]["status"] == "STARTING"
    assert "UNRECOGNIZED_STATE::STARTING" in report["blockers"][0]["reason"]


def test_ready_still_passes():
    probe = _HealthProbeStub("READY")

    report = probe.get_live_readiness_report()

    assert report["level"] == "READY"
    assert report["ready"] is True
    assert report["blockers"] == []


def test_degraded_separated():
    probe = _HealthProbeStub("DEGRADED")

    report = probe.get_live_readiness_report()

    assert report["level"] == "DEGRADED"
    assert report["ready"] is False
    assert report["blockers"] == []
    assert report["details"]["degraded"][0]["name"] == "component_a"


def test_none_status_fail_closed():
    probe = _HealthProbeStub(None)

    report = probe.get_live_readiness_report()

    assert report["level"] == "NOT_READY"
    assert report["ready"] is False
    assert report["blockers"][0]["name"] == "component_a"
    assert report["blockers"][0]["status"] == "UNKNOWN"
    assert report["blockers"][0]["reason"] == "test_reason"


# ---------------------------------------------------------------------------
# Task: reviewer_strong_collectors — collect_correlation_context()
# ---------------------------------------------------------------------------

class _FakeEventBus:
    def queue_depth(self):
        return 5

    def published_total_count(self):
        return 42

    def delivered_total_count(self):
        return 39

    def subscriber_error_counts(self):
        return {"handler_a": 2}


class _FakeAsyncDBWriter:
    _written = 100
    _failed = 3
    _dropped = 1

    class queue:
        @staticmethod
        def qsize():
            return 7


def test_collect_correlation_context_from_event_bus():
    probe = RuntimeProbe(event_bus=_FakeEventBus())
    ctx = probe.collect_correlation_context()

    assert "event_bus" in ctx
    assert ctx["event_bus"]["queue_depth"] == 5
    assert ctx["event_bus"]["published_total"] == 42
    assert ctx["event_bus"]["side_effects_confirmed"] == 39
    assert ctx["event_bus"]["subscriber_errors"] == {"handler_a": 2}


def test_collect_correlation_context_from_async_db_writer():
    probe = RuntimeProbe(async_db_writer=_FakeAsyncDBWriter())
    ctx = probe.collect_correlation_context()

    assert "db_write_queue" in ctx
    assert ctx["db_write_queue"]["queue_depth"] == 7
    assert ctx["db_write_queue"]["written"] == 100
    assert ctx["db_write_queue"]["failed"] == 3
    assert ctx["db_write_queue"]["dropped"] == 1


def test_collect_correlation_context_returns_both_sections():
    probe = RuntimeProbe(event_bus=_FakeEventBus(), async_db_writer=_FakeAsyncDBWriter())
    ctx = probe.collect_correlation_context()

    assert "event_bus" in ctx
    assert "db_write_queue" in ctx


def test_collect_correlation_context_empty_when_no_collectors():
    probe = RuntimeProbe()
    ctx = probe.collect_correlation_context()
    assert ctx == {}


def test_collect_correlation_context_collects_direct_db_state():
    class _DbOrdersStub:
        def get_recent_orders_for_diagnostics(self, limit=500):
            return [
                {"order_id": "o1", "status": "SUBMITTED", "remote_status": "SUBMITTED"},
                {"order_id": "o2", "status": "OPEN", "remote_status": "CANCELLED"},
                {"order_id": "o3", "status": "COMPLETED", "remote_status": "COMPLETED"},
            ]

    probe = RuntimeProbe(db=_DbOrdersStub())
    ctx = probe.collect_correlation_context()

    assert ctx["db_state"]["inflight_orders_count"] == 2
    assert ctx["db_state"]["remote_mismatch_count"] == 1


def test_collect_correlation_context_tolerates_event_bus_without_accessors():
    """If event_bus lacks the new methods, falls back gracefully to stats()."""
    class _LegacyEventBus:
        def stats(self):
            return {"queue_size": 3}

        def subscriber_error_counts(self):
            return {}

    probe = RuntimeProbe(event_bus=_LegacyEventBus())
    ctx = probe.collect_correlation_context()

    assert ctx["event_bus"]["queue_depth"] == 3
    assert "published_total" not in ctx["event_bus"]
