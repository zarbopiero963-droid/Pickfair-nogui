import pytest

from core.runtime_controller import RuntimeController
from core.system_state import RoserpinaConfig, RuntimeMode, DeskMode


class _Bus:
    def __init__(self):
        self.events = []
    def subscribe(self, *_args):
        return None
    def publish(self, topic, payload=None):
        self.events.append((topic, payload or {}))




class _DB:
    def _execute(self, *_args, **_kwargs):
        return None
    def _fetch_one(self, *_args, **_kwargs):
        return None
    def _fetch_all(self, *_args, **_kwargs):
        return []

class _Settings:
    def load_roserpina_config(self):
        cfg = RoserpinaConfig()
        cfg.anti_duplication_enabled = False
        return cfg


class _Betfair:
    def __init__(self):
        self.sim_flags = []
    def set_simulation_mode(self, enabled):
        self.sim_flags.append(bool(enabled))
    def get_account_funds(self):
        return {"available": 0.0}
    def status(self):
        return {"connected": True}


class _Telegram:
    pass


@pytest.mark.integration
@pytest.mark.parametrize("simulation_mode", [False, True])
def test_runtime_controller_routing_parity(simulation_mode):
    bus = _Bus()
    rc = RuntimeController(bus=bus, db=_DB(), settings_service=_Settings(), betfair_service=_Betfair(), telegram_service=_Telegram())
    rc.mode = RuntimeMode.ACTIVE
    rc.duplication_guard = type("DG", (), {"build_event_key": staticmethod(lambda s: "1.2:11:BACK:default"), "is_duplicate": staticmethod(lambda _k: False), "register": staticmethod(lambda _k: None)})()
    rc.mm.calculate = lambda **_kw: type("D", (), {"approved": True, "recommended_stake": 4.0, "table_id": 1, "reason": "ok", "desk_mode": DeskMode.NORMAL, "metadata": {}})()

    signal = {"market_id": "1.2", "selection_id": 11, "price": 2.0, "simulation_mode": simulation_mode, "copy_meta": {"k": "v"}, "pattern_meta": {"p": 1}}
    rc._on_signal_received(signal)

    routed = [e for e in bus.events if e[0] == "CMD_QUICK_BET"]
    assert len(routed) == 1
    payload = routed[0][1]
    assert payload["market_id"] == "1.2"
    assert payload["selection_id"] == 11
    assert payload["stake"] == 4.0


@pytest.mark.integration
def test_duplication_lock_released_on_table_allocation_failure():
    """Regression: acquire() succeeded but table=None early-return never released the lock.
    After a table-allocation rejection the same event_key must be acquirable again."""
    bus = _Bus()

    class _SettingsAntiDup:
        def load_roserpina_config(self):
            cfg = RoserpinaConfig()
            cfg.anti_duplication_enabled = True
            return cfg

    rc = RuntimeController(
        bus=bus,
        db=_DB(),
        settings_service=_SettingsAntiDup(),
        betfair_service=_Betfair(),
        telegram_service=_Telegram(),
    )
    rc.mode = RuntimeMode.ACTIVE
    # Force allocate to always return None regardless of table_count
    rc.table_manager.allocate = lambda **_kw: None

    signal = {"market_id": "1.9", "selection_id": 99, "price": 3.0}

    # First send: lock acquired → table fails → lock must be released
    rc._on_signal_received(signal)
    rejected = [e for e in bus.events if e[0] == "SIGNAL_REJECTED"]
    assert len(rejected) == 1
    assert "nessun_tavolo" in rejected[0][1].get("reason", "")

    # Second send with same signal: must NOT be rejected as duplicate
    bus.events.clear()
    rc._on_signal_received(signal)
    duplicate_rejections = [
        e for e in bus.events
        if e[0] == "SIGNAL_REJECTED" and "duplicato" in e[1].get("reason", "")
    ]
    assert duplicate_rejections == [], "lock was not released on table-allocation failure"


@pytest.mark.integration
def test_duplication_lock_released_on_mm_rejection():
    """Regression: acquire() succeeded but decision.approved=False early-return never released
    the lock. After a money-management rejection the same event_key must be acquirable again."""
    bus = _Bus()

    class _SettingsAntiDup:
        def load_roserpina_config(self):
            cfg = RoserpinaConfig()
            cfg.anti_duplication_enabled = True
            return cfg

    rc = RuntimeController(
        bus=bus,
        db=_DB(),
        settings_service=_SettingsAntiDup(),
        betfair_service=_Betfair(),
        telegram_service=_Telegram(),
    )
    rc.mode = RuntimeMode.ACTIVE

    # Override MM to always reject without triggering LOCKDOWN
    rc.mm.calculate = lambda **_kw: type(
        "D", (), {"approved": False, "recommended_stake": 0.0, "table_id": 1,
                  "reason": "test_rejected", "desk_mode": DeskMode.NORMAL, "metadata": {}}
    )()

    signal = {"market_id": "1.10", "selection_id": 88, "price": 2.5}

    # First send: lock acquired → MM rejects → lock must be released
    rc._on_signal_received(signal)
    rejected = [e for e in bus.events if e[0] == "SIGNAL_REJECTED"]
    assert len(rejected) == 1

    # Second send: must NOT be rejected as duplicate
    bus.events.clear()
    rc._on_signal_received(signal)
    duplicate_rejections = [
        e for e in bus.events
        if e[0] == "SIGNAL_REJECTED" and "duplicato" in e[1].get("reason", "")
    ]
    assert duplicate_rejections == [], "lock was not released on MM rejection"


@pytest.mark.integration
def test_runtime_controller_pause_block_semantics_same_live_sim():
    out = []
    for sim in (False, True):
        bus = _Bus()
        rc = RuntimeController(bus=bus, db=_DB(), settings_service=_Settings(), betfair_service=_Betfair(), telegram_service=_Telegram())
        rc.mode = RuntimeMode.PAUSED
        rc.duplication_guard = type("DG", (), {"build_event_key": staticmethod(lambda s: "1:1:BACK:default"), "is_duplicate": staticmethod(lambda _k: False), "register": staticmethod(lambda _k: None)})()
        rc._on_signal_received({"market_id": "1", "selection_id": 1, "price": 2.0, "simulation_mode": sim})
        out.append([e[0] for e in bus.events])

    assert out[0] == out[1]
    assert "SIGNAL_REJECTED" in out[0]


@pytest.mark.integration
def test_is_live_allowed_fail_closed_when_deploy_gate_denies_ready_state():
    rc = RuntimeController(
        bus=_Bus(),
        db=_DB(),
        settings_service=_Settings(),
        betfair_service=_Betfair(),
        telegram_service=_Telegram(),
    )
    rc.execution_mode = "LIVE"
    rc.live_enabled = True
    rc.live_readiness_ok = True
    rc.get_deploy_gate_status = lambda **_kwargs: {"allowed": False, "readiness": "READY"}

    assert rc.is_live_allowed() is False


@pytest.mark.integration
def test_effective_execution_mode_stays_simulation_when_deploy_gate_denies_ready_state():
    rc = RuntimeController(
        bus=_Bus(),
        db=_DB(),
        settings_service=_Settings(),
        betfair_service=_Betfair(),
        telegram_service=_Telegram(),
    )
    rc.execution_mode = "LIVE"
    rc.live_enabled = True
    rc.live_readiness_ok = True
    rc.get_deploy_gate_status = lambda **_kwargs: {"allowed": False, "readiness": "READY"}

    assert rc.get_effective_execution_mode() == "SIMULATION"
