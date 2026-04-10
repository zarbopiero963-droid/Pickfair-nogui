import pytest

from core.runtime_controller import RuntimeController
from core.system_state import RoserpinaConfig
from observability.runtime_probe import RuntimeProbe


class _Bus:
    def subscribe(self, *_args):
        return None


class _Settings:
    def __init__(self, creds=True, password="pw"):
        self._creds = creds
        self._password = password

    def load_roserpina_config(self):
        return RoserpinaConfig()

    def has_live_credentials_configured(self):
        return self._creds

    def load_password(self):
        return self._password

    def load_live_readiness_policy(self):
        return {"safe_mode_blocks_live": True}


class _Betfair:
    def connect(self, **_kwargs):
        return {"ok": True}


class _Telegram:
    pass


class _DB:
    def _execute(self, *_args, **_kwargs):
        return None

    def _fetch_one(self, *_args, **_kwargs):
        return None

    def _fetch_all(self, *_args, **_kwargs):
        return []


def _runtime(settings=None):
    return RuntimeController(
        bus=_Bus(),
        db=_DB(),
        settings_service=settings or _Settings(),
        betfair_service=_Betfair(),
        telegram_service=_Telegram(),
    )


@pytest.mark.failure
def test_missing_runtime_component_reports_runtime_blocker():
    runtime = _runtime()
    runtime._startup_in_progress = True
    report = runtime.evaluate_live_readiness(runtime_probe=RuntimeProbe(runtime_controller=runtime), context={"execution_mode": "LIVE"})
    assert "RUNTIME_NOT_INITIALIZED" in report["blockers"]


@pytest.mark.failure
def test_unknown_readiness_signal_fails_closed():
    runtime = _runtime()
    report = runtime.evaluate_live_readiness(runtime_probe=None, context={"execution_mode": "LIVE"})
    assert report["ready"] is False
    assert "READINESS_SIGNAL_UNKNOWN" in report["blockers"]


@pytest.mark.failure
def test_contradictory_state_is_blocked():
    runtime = _runtime()
    runtime.simulation_mode = True
    report = runtime.evaluate_live_readiness(runtime_probe=RuntimeProbe(runtime_controller=runtime), context={"execution_mode": "LIVE"})
    assert report["ready"] is False
    assert "LIVE_DEPENDENCY_MISSING" in report["blockers"]


@pytest.mark.failure
def test_malformed_context_fails_closed():
    runtime = _runtime()
    report = runtime.evaluate_live_readiness(
        runtime_probe=RuntimeProbe(runtime_controller=runtime),
        context={"execution_mode": {"bad": "value"}},
    )
    assert report["ready"] is False
    assert "INVALID_EXECUTION_MODE" in report["blockers"]
