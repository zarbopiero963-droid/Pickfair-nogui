from pathlib import Path

from core.runtime_controller import RuntimeController


class _Bus:
    def subscribe(self, *_args, **_kwargs):
        return None

    def publish(self, *_args, **_kwargs):
        return None


class _Db:
    def _execute(self, *_args, **_kwargs):
        return None


class _Settings:
    def load_roserpina_config(self):
        class Cfg:
            table_count = 1
            anti_duplication_enabled = False
            allow_recovery = False
            auto_reset_drawdown_pct = 90
            defense_drawdown_pct = 7.5
            lockdown_drawdown_pct = 95

            def __getattr__(self, _name):
                return 0

        return Cfg()

    def load_live_enabled(self):
        return False

    def load_live_readiness_ok(self):
        return True


class _Betfair:
    def __init__(self):
        self.connect_calls = 0

    def set_simulation_mode(self, *_args, **_kwargs):
        return None

    def connect(self, **_kwargs):
        self.connect_calls += 1
        return {"ok": True}

    def get_account_funds(self):
        return {"available": 1.0}

    def status(self):
        return {"connected": True}


class _Telegram:
    def start(self):
        return {"ok": True}

    def status(self):
        return {"connected": True}


def test_live_requires_mode_plus_gate_approval():
    rc = RuntimeController(
        bus=_Bus(),
        db=_Db(),
        settings_service=_Settings(),
        betfair_service=_Betfair(),
        telegram_service=_Telegram(),
    )

    refused = rc.start(execution_mode="LIVE", live_enabled=False)
    allowed = rc.start(execution_mode="SIMULATION", live_enabled=False)

    assert refused["refused"] is True
    assert refused["reason_code"] == "live_not_enabled"
    assert allowed["started"] is True


def test_runtime_bootstrap_uses_explicit_gate_helper():
    source = Path("core/runtime_controller.py").read_text(encoding="utf-8")

    assert "assert_live_gate_or_refuse" in source
