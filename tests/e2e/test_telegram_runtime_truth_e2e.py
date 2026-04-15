from observability.runtime_probe import RuntimeProbe
from services.telegram_alerts_service import TelegramAlertsService
from services.telegram_signal_processor import TelegramSignalProcessor
from telegram_module import TelegramModule


class _SettingsEnabled:
    def load_telegram_config_row(self):
        return {
            "alerts_enabled": True,
            "alerts_chat_id": "321",
            "min_alert_severity": "WARNING",
            "alert_cooldown_sec": 0,
            "alert_dedup_enabled": False,
            "alert_format_rich": True,
        }


class _SettingsEnabledMissingChat:
    def load_telegram_config_row(self):
        return {
            "alerts_enabled": True,
            "alerts_chat_id": "",
            "min_alert_severity": "WARNING",
            "alert_cooldown_sec": 0,
            "alert_dedup_enabled": False,
            "alert_format_rich": True,
        }


class _SenderOk:
    def send_alert_message(self, chat_id, text):
        return None


class _DbStub:
    def get_recent_observability_snapshots(self, limit=1):
        return [{"id": 1}]


class _BusStub:
    def __init__(self):
        self.subscribers = {}
        self.events = []

    def publish(self, topic, payload):
        self.events.append((topic, dict(payload or {})))


class _TelegramContractHarness(TelegramModule):
    def __init__(self, bus):
        self.bus = bus


def test_telegram_runtime_truth_e2e_sender_missing_is_explicit_degraded_and_not_deliverable():
    alerts = TelegramAlertsService(settings_service=_SettingsEnabled(), telegram_sender=None)
    probe = RuntimeProbe(db=_DbStub(), telegram_alerts_service=alerts, settings_service=_SettingsEnabled())

    state = probe.collect_runtime_state()["alert_pipeline"]

    assert set(("alerts_enabled", "sender_available", "deliverable", "reason")) <= set(state)
    assert state["alerts_enabled"] is True
    assert state["sender_available"] is False
    assert state["deliverable"] is False
    assert state["status"] == "DEGRADED"
    assert state["reason"] == "sender_unavailable"
    assert state["status"] != "READY"


def test_telegram_runtime_truth_e2e_sender_present_is_deliverable_and_ready():
    alerts = TelegramAlertsService(settings_service=_SettingsEnabled(), telegram_sender=_SenderOk())
    probe = RuntimeProbe(db=_DbStub(), telegram_alerts_service=alerts, settings_service=_SettingsEnabled())

    state = probe.collect_runtime_state()["alert_pipeline"]

    assert state["alerts_enabled"] is True
    assert state["sender_available"] is True
    assert state["deliverable"] is True
    assert state["status"] == "READY"
    assert state["reason"] is None


def test_telegram_runtime_truth_e2e_probe_uses_real_deliverability_not_only_enabled_flag():
    alerts = TelegramAlertsService(settings_service=_SettingsEnabledMissingChat(), telegram_sender=_SenderOk())
    probe = RuntimeProbe(db=_DbStub(), telegram_alerts_service=alerts, settings_service=_SettingsEnabledMissingChat())

    state = probe.collect_runtime_state()["alert_pipeline"]

    assert state["alerts_enabled"] is True
    assert state["sender_available"] is True
    assert state["deliverable"] is False
    assert state["status"] == "DEGRADED"
    assert state["reason"] == "alerts_chat_id_missing"


def test_telegram_runtime_truth_e2e_normalization_boundary_is_explicit_for_valid_signal():
    processor = TelegramSignalProcessor()

    result = processor.normalize_ingestion_signal(
        {
            "market_id": "1.100",
            "selection_id": "11",
            "odds": "2.14",
            "action": "back",
            "event_name": "A v B",
            "copy_meta": {"master_id": "M1"},
        }
    )

    assert result["ok"] is True
    normalized = result["normalized_signal"]
    assert normalized["boundary_stage"] == "telegram_ingestion_normalized_v1"
    assert normalized["market_id"] == "1.100"
    assert normalized["selection_id"] == 11
    assert normalized["bet_type"] == "BACK"
    assert normalized["price"] == 2.14
    assert normalized["order_origin"] == "COPY"
    assert normalized["copy_meta"] == {"master_id": "M1"}


def test_telegram_runtime_truth_e2e_normalization_fails_closed_for_ambiguous_origin_meta():
    processor = TelegramSignalProcessor()

    result = processor.normalize_ingestion_signal(
        {
            "market_id": "1.100",
            "selection_id": 11,
            "price": 2.2,
            "copy_meta": {"master_id": "M1"},
            "pattern_meta": {"pattern_id": "P1"},
        }
    )

    assert result == {
        "ok": False,
        "error_code": "COPY_PATTERN_MUTUALLY_EXCLUSIVE",
        "error_reason": "copy_meta and pattern_meta cannot coexist",
        "normalized_signal": {},
    }


def test_telegram_runtime_truth_e2e_authoritative_route_prefers_runtime_signal_gate_when_present():
    bus = _BusStub()
    bus.subscribers = {"SIGNAL_RECEIVED": [object()], "REQ_QUICK_BET": [object()]}
    module = _TelegramContractHarness(bus)

    route = module._publish_order_signal(
        {
            "market_id": "1.101",
            "selection_id": 9,
            "telegram_boundary_stage": "telegram_ingestion_normalized_v1",
        }
    )

    assert route == "SIGNAL_RECEIVED"
    assert len(bus.events) == 1
    topic, payload = bus.events[0]
    assert topic == "SIGNAL_RECEIVED"
    assert payload["telegram_routing_contract"] == "telegram_authoritative_routing_v1"
    assert payload["telegram_route_target"] == "SIGNAL_RECEIVED"


def test_telegram_runtime_truth_e2e_routing_fails_closed_if_boundary_stage_missing():
    bus = _BusStub()
    bus.subscribers = {"SIGNAL_RECEIVED": [object()]}
    module = _TelegramContractHarness(bus)

    try:
        module._publish_order_signal({"market_id": "1.202", "selection_id": 12})
    except ValueError as exc:
        assert str(exc) == "TELEGRAM_ROUTING_BOUNDARY_INVALID_STAGE"
    else:
        raise AssertionError("Expected fail-closed boundary validation")

    assert bus.events == []
