from tests.helpers.fake_settings import FakeSettingsService


def test_anomaly_enabled_defaults_false_when_missing():
    settings = FakeSettingsService()

    assert settings.get_bool("anomaly_enabled", default=False) is False
    assert settings.snapshot() == {}


def test_anomaly_enabled_true_persists_across_reload():
    settings = FakeSettingsService()
    settings.set_bool("anomaly_enabled", True)

    reloaded = FakeSettingsService.from_state(settings.export_state())

    assert reloaded.get_bool("anomaly_enabled", default=False) is True


def test_anomaly_enabled_false_persists_across_reload():
    settings = FakeSettingsService({"anomaly_enabled": True})
    settings.set_bool("anomaly_enabled", False)

    reloaded = FakeSettingsService.from_state(settings.export_state())

    assert reloaded.get_bool("anomaly_enabled", default=True) is False
