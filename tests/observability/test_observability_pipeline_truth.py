from observability.runtime_probe import RuntimeProbe
from tests.helpers.fake_runtime_state import FakeRuntimeState
from tests.helpers.fake_settings import FakeSettingsService


class _AlertsSvcStub:
    def __init__(self, fake_state: FakeRuntimeState):
        self._state = fake_state

    def availability_status(self):
        return self._state.alert_pipeline_snapshot()


class _DbStub:
    def get_recent_observability_snapshots(self, limit=1):
        return [{"id": 1}]


def test_observability_pipeline_truth_for_missing_sender_is_not_deliverable():
    fake_state = FakeRuntimeState.ready().mark_sender_unavailable()
    probe = RuntimeProbe(
        db=_DbStub(),
        settings_service=FakeSettingsService({"anomaly_alerts_enabled": True}),
        telegram_service=None,
        telegram_alerts_service=_AlertsSvcStub(fake_state),
        safe_mode=None,
    )

    pipeline = probe.collect_runtime_state()["alert_pipeline"]

    assert pipeline["alerts_enabled"] is True
    assert pipeline["sender_available"] is False
    assert pipeline["deliverable"] is False
    assert pipeline["status"] == "DEGRADED"
    assert pipeline["reason"] == "sender_unavailable"
    assert pipeline["status"] != "READY"


def test_observability_pipeline_truth_for_deliverable_sender():
    fake_state = FakeRuntimeState.ready(reason=None)
    probe = RuntimeProbe(
        db=_DbStub(),
        settings_service=FakeSettingsService({"anomaly_alerts_enabled": True}),
        telegram_service=None,
        telegram_alerts_service=_AlertsSvcStub(fake_state),
        safe_mode=None,
    )

    pipeline = probe.collect_runtime_state()["alert_pipeline"]

    assert pipeline["alerts_enabled"] is True
    assert pipeline["sender_available"] is True
    assert pipeline["deliverable"] is True
    assert pipeline["status"] == "READY"
    assert pipeline["reason"] is None
    assert pipeline["last_delivery_ok"] is True
