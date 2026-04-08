from tests.helpers.fake_settings import FakeSettingsService


TOGGLES = (
    "anomaly_enabled",
    "anomaly_alerts_enabled",
    "anomaly_actions_enabled",
)


def test_all_anomaly_toggles_persist_across_reload():
    settings = FakeSettingsService()

    for key in TOGGLES:
        settings.set_bool(key, True)

    reloaded = FakeSettingsService.from_state(settings.export_state())

    for key in TOGGLES:
        assert reloaded.get_bool(key, default=False) is True


def test_missing_anomaly_toggles_fall_back_false_deterministically():
    settings = FakeSettingsService()

    for key in TOGGLES:
        assert settings.get_bool(key, default=False) is False
