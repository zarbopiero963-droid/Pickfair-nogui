from observability.runtime_probe import RuntimeProbe


class _SettingsStub:
    def load_telegram_config_row(self):
        return {"alerts_enabled": True}


class _TelegramStub:
    def get_sender(self):
        return object()


class _AlertsSvcStub:
    def availability_status(self):
        return {
            "alerts_enabled": True,
            "sender_available": False,
            "deliverable": False,
            "reason": "sender_unavailable",
            "last_delivery_ok": False,
            "last_delivery_error": "sender_unavailable",
        }


class _SafeModeStub:
    def is_enabled(self):
        return True


class _DbStub:
    def get_recent_observability_snapshots(self, limit=1):
        return [{"id": 1}]


def test_runtime_probe_alert_pipeline_state_uses_wired_services():
    probe = RuntimeProbe(
        db=_DbStub(),
        settings_service=_SettingsStub(),
        telegram_service=_TelegramStub(),
        telegram_alerts_service=_AlertsSvcStub(),
        safe_mode=_SafeModeStub(),
    )

    state = probe.collect_runtime_state()

    assert state["alert_pipeline"]["alerts_enabled"] is True
    assert state["alert_pipeline"]["sender_available"] is False
    assert state["alert_pipeline"]["deliverable"] is False
    assert state["safe_mode_enabled"] is True
