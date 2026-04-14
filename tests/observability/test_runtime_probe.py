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


class _RuntimeHeartbeatStub:
    # 2000-01-01T00:00:00Z
    last_signal_at = "2000-01-01T00:00:00+00:00"


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

    expected = fake_state.alert_pipeline_snapshot()
    assert state["alert_pipeline"]["enabled"] == expected["alerts_enabled"]
    for key, value in expected.items():
        assert state["alert_pipeline"][key] == value
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


class _WorkerDead:
    def is_alive(self):
        return False


class _LivenessEventBus:
    _workers = [_WorkerDead()]

    def queue_depth(self):
        return 4

    def delivered_total_count(self):
        return 11


class _FakeAsyncDBWriter:
    _written = 100
    _failed = 3
    _dropped = 1

    class queue:
        @staticmethod
        def qsize():
            return 7


class _DbDiagnosticsStub:
    def get_recent_orders_for_diagnostics(self, limit=200):
        del limit
        return [
            {"order_id": "o-submitted-missing", "status": "SUBMITTED"},
            {"order_id": "o-reconciled", "status": "SUBMITTED"},
            {"order_id": "o-finalized-missing", "status": "COMPLETED"},
        ]

    def get_recent_audit_events_for_diagnostics(self, limit=300):
        del limit
        return [{"order_id": "o-reconciled"}]


class _TableManagerStub:
    def total_exposure(self):
        return 12.5


class _RiskDeskStub:
    bankroll_current = 321.0
    exchange_balance = 319.5
    local_exposure = 12.5
    remote_exposure = 11.9


class _RuntimeControllerStub:
    table_manager = _TableManagerStub()
    risk_desk = _RiskDeskStub()


class _RiskDeskBankrollOnlyStub:
    bankroll_current = 222.0


class _RuntimeControllerBankrollOnlyStub:
    table_manager = _TableManagerStub()
    risk_desk = _RiskDeskBankrollOnlyStub()


def test_collect_metrics_emits_default_liveness_signals():
    probe = RuntimeProbe(
        runtime_controller=_RuntimeHeartbeatStub(),
        event_bus=_LivenessEventBus(),
    )

    metrics = probe.collect_metrics()

    assert "heartbeat_age" in metrics
    assert metrics["heartbeat_age"] > 0.0
    assert metrics["last_heartbeat_age_sec"] == metrics["heartbeat_age"]
    assert metrics["queue_depth"] == 4.0
    assert metrics["worker_threads_alive"] == 0.0
    assert metrics["worker_alive"] == 0.0
    assert metrics["completed_total"] == 11.0
    assert metrics["completed_delta"] == 0.0


def test_collect_metrics_completed_delta_advances_deterministically():
    class _Writer:
        _written = 7

    probe = RuntimeProbe(async_db_writer=_Writer())
    first = probe.collect_metrics()
    assert first["completed_total"] == 7.0
    assert first["completed_delta"] == 0.0

    _Writer._written = 10
    second = probe.collect_metrics()
    assert second["completed_total"] == 10.0
    assert second["completed_delta"] == 3.0


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


def test_collect_correlation_context_omits_db_state_when_query_fails():
    class _DbOrdersFailingStub:
        def get_recent_orders_for_diagnostics(self, limit=500):
            raise RuntimeError("db unavailable")

    probe = RuntimeProbe(db=_DbOrdersFailingStub())
    ctx = probe.collect_correlation_context()

    assert "db_state" not in ctx


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


def test_collect_reviewer_context_emits_canonical_blocks():
    probe = RuntimeProbe(
        db=_DbDiagnosticsStub(),
        event_bus=_FakeEventBus(),
        async_db_writer=_FakeAsyncDBWriter(),
        runtime_controller=_RuntimeControllerStub(),
    )

    ctx = probe.collect_reviewer_context()

    assert ctx["risk"]["expected_exposure"] == 12.5
    assert ctx["risk"]["actual_exposure"] == 11.9
    assert ctx["risk"]["local_exposure"] == 12.5
    assert ctx["risk"]["remote_exposure"] == 11.9
    assert ctx["db"]["db_writer_backlog"] == 7
    assert ctx["db"]["db_writer_failed"] == 3
    assert ctx["db"]["db_writer_dropped"] == 1
    assert ctx["financials"]["ledger_balance"] == 321.0
    assert ctx["financials"]["venue_balance"] == 319.5
    assert ctx["event_bus"]["expected_fanout"] == 41
    assert ctx["event_bus"]["delivered_fanout"] == 39
    assert len(ctx["recent_orders"]) == 3
    assert len(ctx["recent_audit"]) == 1
    assert ctx["reconcile_chain"]["missing_count"] == 1
    assert ctx["reconcile_chain"]["finalized_missing_count"] == 1


def test_collect_reviewer_context_keeps_bankroll_only_financials_balanced():
    probe = RuntimeProbe(
        db=_DbDiagnosticsStub(),
        runtime_controller=_RuntimeControllerBankrollOnlyStub(),
    )
    ctx = probe.collect_reviewer_context()
    assert ctx["financials"]["ledger_balance"] == 222.0
    assert ctx["financials"]["venue_balance"] == 222.0
