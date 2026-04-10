import pytest

from core.runtime_controller import RuntimeController
from core.system_state import RoserpinaConfig, RuntimeMode
from observability.runtime_probe import RuntimeProbe


class _Bus:
    def subscribe(self, *_args):
        return None

    def publish(self, *_args, **_kwargs):
        return None


class _DB:
    def _execute(self, *_args, **_kwargs):
        return None

    def _fetch_one(self, *_args, **_kwargs):
        return None

    def _fetch_all(self, *_args, **_kwargs):
        return []


class _Settings:
    def __init__(self, *, creds=False, password="", safe_mode_blocks_live=True):
        self._creds = creds
        self._password = password
        self._safe_mode_blocks_live = safe_mode_blocks_live

    def load_roserpina_config(self):
        return RoserpinaConfig()

    def has_live_credentials_configured(self):
        return self._creds

    def load_password(self):
        return self._password

    def load_live_readiness_policy(self):
        return {"safe_mode_blocks_live": self._safe_mode_blocks_live}


class _Betfair:
    def connect(self, **_kwargs):
        return {"connected": True}


class _Telegram:
    pass


class _SafeMode:
    def __init__(self, enabled=False):
        self._enabled = enabled

    def is_enabled(self):
        return self._enabled


class _ReadyComponent:
    def is_ready(self):
        return True


def _build_controller(*, creds=False, password="", safe_mode=False):
    settings = _Settings(creds=creds, password=password)
    runtime = RuntimeController(
        bus=_Bus(),
        db=_DB(),
        settings_service=settings,
        betfair_service=_Betfair(),
        telegram_service=_Telegram(),
    )
    probe = RuntimeProbe(runtime_controller=runtime, safe_mode=_SafeMode(enabled=safe_mode))
    return runtime, probe


@pytest.mark.integration
def test_clean_default_boot_not_live_ready_without_live_path():
    runtime, probe = _build_controller(creds=False, password="")
    report = runtime.get_live_readiness_report(runtime_probe=probe, context={"execution_mode": "LIVE"})
    assert report["ready"] is False
    assert "LIVE_DEPENDENCY_MISSING" in report["blockers"]


@pytest.mark.integration
def test_runtime_incomplete_blocks_live():
    runtime, probe = _build_controller(creds=True, password="pw")
    runtime._startup_failed = True
    report = runtime.get_live_readiness_report(runtime_probe=probe, context={"execution_mode": "LIVE"})
    assert report["ready"] is False
    assert "RUNTIME_NOT_INITIALIZED" in report["blockers"]


@pytest.mark.integration
def test_missing_live_dependency_blocks_live():
    runtime, probe = _build_controller(creds=False, password="pw")
    report = runtime.get_live_readiness_report(runtime_probe=probe, context={"execution_mode": "LIVE"})
    assert report["ready"] is False
    assert "LIVE_DEPENDENCY_MISSING" in report["blockers"]


@pytest.mark.integration
def test_kill_switch_active_blocks_live():
    runtime, probe = _build_controller(creds=True, password="pw")
    runtime.mode = RuntimeMode.LOCKDOWN
    report = runtime.get_live_readiness_report(runtime_probe=probe, context={"execution_mode": "LIVE"})
    assert report["ready"] is False
    assert "KILL_SWITCH_ACTIVE" in report["blockers"]


@pytest.mark.integration
def test_valid_live_ready_state_passes_gate():
    runtime, probe = _build_controller(creds=True, password="pw")
    health_probe = RuntimeProbe(
        runtime_controller=runtime,
        safe_mode=_SafeMode(enabled=False),
        db=_ReadyComponent(),
        shutdown_manager=_ReadyComponent(),
        betfair_service=type("Bf", (), {"is_connected": staticmethod(lambda: True)})(),
        trading_engine=type("Te", (), {"readiness": staticmethod(lambda: {"state": "READY", "health": {"x": 1}})})(),
    )
    report = runtime.get_live_readiness_report(
        runtime_probe=health_probe,
        context={"execution_mode": "LIVE"},
    )
    assert report["ready"] is True
    assert report["blockers"] == []


@pytest.mark.integration
def test_missing_execution_mode_fails_closed():
    runtime, probe = _build_controller(creds=True, password="pw")
    report = runtime.get_live_readiness_report(runtime_probe=probe, context={})
    assert report["ready"] is False
    assert "INVALID_EXECUTION_MODE" in report["blockers"]
