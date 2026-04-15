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

    def load_market_data_config(self):
        return {
            "market_data_mode": "poll",
            "enabled": False,
            "market_ids": [],
            "snapshot_fallback_enabled": True,
            "snapshot_fallback_interval_sec": 1,
        }


class _Betfair:
    def __init__(self):
        self.sim_flags = []
    def set_simulation_mode(self, enabled):
        self.sim_flags.append(bool(enabled))
    def get_account_funds(self):
        return {"available": 0.0}
    def status(self):
        return {"connected": True}
    def get_live_client(self):
        return object()
    def get_market_book_snapshot(self, market_id):
        _ = market_id
        return None
    def ensure_stream_session_ready(self):
        return True


class _Telegram:
    def start(self):
        return {"started": True}
    def stop(self):
        return None
    def status(self):
        return {"connected": True}


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
@pytest.mark.parametrize(
    ("meta_field", "meta_value"),
    [
        ("copy_meta", {"channel": "telegram", "copy_target": "desk-a"}),
        ("pattern_meta", {"pattern_id": 7, "pattern_name": "late_over"}),
    ],
)
def test_runtime_controller_preserves_origin_metadata_passthrough(meta_field, meta_value):
    bus = _Bus()
    rc = RuntimeController(
        bus=bus,
        db=_DB(),
        settings_service=_Settings(),
        betfair_service=_Betfair(),
        telegram_service=_Telegram(),
    )
    rc.mode = RuntimeMode.ACTIVE
    rc.duplication_guard = type(
        "DG",
        (),
        {
            "build_event_key": staticmethod(lambda s: "1.2:11:BACK:telegram"),
            "is_duplicate": staticmethod(lambda _k: False),
            "register": staticmethod(lambda _k: None),
            "acquire": staticmethod(lambda _k: True),
            "release": staticmethod(lambda _k: None),
        },
    )()
    rc.mm.calculate = lambda **_kw: type(
        "D",
        (),
        {
            "approved": True,
            "recommended_stake": 4.0,
            "table_id": 1,
            "reason": "ok",
            "desk_mode": DeskMode.NORMAL,
            "metadata": {},
        },
    )()

    signal = {
        "market_id": "1.2",
        "selection_id": 11,
        "price": 2.0,
        "simulation_mode": True,
        "order_origin": "telegram",
        meta_field: meta_value,
    }
    rc._on_signal_received(signal)

    routed = [e for e in bus.events if e[0] == "CMD_QUICK_BET"]
    assert len(routed) == 1
    payload = routed[0][1]
    assert payload["order_origin"] == "telegram"
    assert payload[meta_field] == meta_value


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


@pytest.mark.integration
def test_runtime_controller_market_data_ingestion_boundary_uses_market_tracker(monkeypatch):
    events = []

    class _BusBoundary(_Bus):
        def publish(self, topic, payload=None):
            super().publish(topic, payload)
            events.append((topic, payload or {}))

    class _SettingsStream(_Settings):
        def load_market_data_config(self):
            return {
                "market_data_mode": "stream",
                "enabled": True,
                "market_ids": ["1.900"],
                "heartbeat_timeout_sec": 2,
                "snapshot_fallback_enabled": True,
                "snapshot_fallback_interval_sec": 1,
            }

    class _FakeStreamingFeed:
        def __init__(self, *, client_getter, config, on_market_book, on_disconnect, session_gate=None):
            _ = client_getter, config, on_disconnect, session_gate
            self.on_market_book = on_market_book
            self.started = False
            self.stopped = False

        def start(self):
            self.started = True
            self.on_market_book({"marketId": "1.900", "runners": []})
            return {"started": True}

        def stop(self):
            self.stopped = True
            return {"stopped": True}

    monkeypatch.setattr("core.runtime_controller.StreamingFeed", _FakeStreamingFeed)

    rc = RuntimeController(
        bus=_BusBoundary(),
        db=_DB(),
        settings_service=_SettingsStream(),
        betfair_service=_Betfair(),
        telegram_service=_Telegram(),
    )
    rc.simulation_mode = False

    received = []
    rc.market_tracker.on_market_book = lambda book: received.append(dict(book))

    rc._start_market_data_feed()
    assert rc.streaming_feed is not None
    assert received and received[0]["marketId"] == "1.900"
    rc._stop_market_data_feed()
    assert rc.streaming_feed is None


@pytest.mark.integration
def test_runtime_controller_market_tracker_receives_coherent_edge_case_book(monkeypatch):
    class _SettingsStream(_Settings):
        def load_market_data_config(self):
            return {
                "market_data_mode": "stream",
                "enabled": True,
                "market_ids": ["1.901"],
                "heartbeat_timeout_sec": 2,
            }

    coherent_book = {
        "marketId": "1.901",
        "market_id": "1.901",
        "status": "SUSPENDED",
        "inplay": True,
        "marketDefinition": {"status": "SUSPENDED", "inPlay": True},
        "runners": [
            {
                "selectionId": 101,
                "runnerName": "Runner 101",
                "status": "REMOVED",
                "ex": {"availableToBack": [{"price": 2.02, "size": 9.0}], "availableToLay": []},
            },
            {
                "selectionId": 102,
                "runnerName": "Runner 102",
                "status": "ACTIVE",
                "ex": {"availableToBack": [{"price": 3.1, "size": 4.0}], "availableToLay": [{"price": 3.2, "size": 6.0}]},
            },
        ],
    }

    class _FakeStreamingFeed:
        def __init__(self, *, client_getter, config, on_market_book, on_disconnect, session_gate=None):
            _ = client_getter, config, on_disconnect, session_gate
            self.on_market_book = on_market_book

        def start(self):
            self.on_market_book(coherent_book)
            return {"started": True}

        def stop(self):
            return {"stopped": True}

    monkeypatch.setattr("core.runtime_controller.StreamingFeed", _FakeStreamingFeed)

    rc = RuntimeController(
        bus=_Bus(),
        db=_DB(),
        settings_service=_SettingsStream(),
        betfair_service=_Betfair(),
        telegram_service=_Telegram(),
    )
    rc.simulation_mode = False

    received = []
    rc.market_tracker.on_market_book = lambda book: received.append(dict(book))
    rc._start_market_data_feed()

    assert len(received) == 1
    out = received[0]
    assert out["marketId"] == "1.901"
    assert out["status"] == "SUSPENDED"
    assert out["inplay"] is True
    assert len(out["runners"]) == 2
    assert out["runners"][0]["selectionId"] == 101
    assert out["runners"][1]["ex"]["availableToLay"] == [{"price": 3.2, "size": 6.0}]


@pytest.mark.integration
def test_runtime_controller_status_surfaces_streaming_feed_degraded_state(monkeypatch):
    class _SettingsStream(_Settings):
        def load_market_data_config(self):
            return {
                "market_data_mode": "stream",
                "enabled": True,
                "market_ids": ["1.902"],
            }

    class _FakeStreamingFeed:
        def __init__(self, *, client_getter, config, on_market_book, on_disconnect, session_gate=None):
            _ = client_getter, config, on_market_book, on_disconnect, session_gate

        def start(self):
            return {"started": True}

        def stop(self):
            return {"stopped": True}

        def status(self):
            return {
                "running": True,
                "auth_degraded": True,
                "keepalive_failure_count": 2,
                "last_keepalive_error": "SESSION_EXPIRED",
            }

    monkeypatch.setattr("core.runtime_controller.StreamingFeed", _FakeStreamingFeed)

    rc = RuntimeController(
        bus=_Bus(),
        db=_DB(),
        settings_service=_SettingsStream(),
        betfair_service=_Betfair(),
        telegram_service=_Telegram(),
    )
    rc.simulation_mode = False
    rc._start_market_data_feed()

    status = rc.get_status()
    assert "streaming_feed" in status
    assert status["streaming_feed"]["auth_degraded"] is True
    assert status["streaming_feed"]["keepalive_failure_count"] == 2


@pytest.mark.integration
def test_runtime_controller_market_tracker_preserves_rare_metadata_fields(monkeypatch):
    class _SettingsStream(_Settings):
        def load_market_data_config(self):
            return {
                "market_data_mode": "stream",
                "enabled": True,
                "market_ids": ["1.903"],
                "heartbeat_timeout_sec": 2,
            }

    metadata_rich_book = {
        "marketId": "1.903",
        "market_id": "1.903",
        "status": "SUSPENDED",
        "inplay": True,
        "marketDefinition": {
            "status": "SUSPENDED",
            "inPlay": True,
            "keyLineDefinition": {"kl": [{"id": 301, "hc": 0.0}]},
            "runners": [
                {
                    "id": 301,
                    "name": "Runner 301",
                    "status": "REMOVED",
                    "adjustmentFactor": 14.2,
                    "removalDate": "2026-04-01T09:30:00Z",
                    "bsp": 3.6,
                    "spn": 3.4,
                    "spf": 3.8,
                }
            ],
        },
        "runners": [
            {
                "selectionId": 301,
                "runnerName": "Runner 301",
                "status": "REMOVED",
                "handicap": 0.0,
                "ltp": 3.5,
                "adjustmentFactor": 14.2,
                "removalDate": "2026-04-01T09:30:00Z",
                "bsp": 3.6,
                "spn": 3.4,
                "spf": 3.8,
                "ex": {
                    "availableToBack": [{"price": 3.4, "size": 5.0}],
                    "availableToLay": [{"price": 3.7, "size": 4.0}],
                    "tradedVolume": [{"price": 3.5, "size": 110.0}],
                },
            }
        ],
    }

    class _FakeStreamingFeed:
        def __init__(self, *, client_getter, config, on_market_book, on_disconnect, session_gate=None):
            _ = client_getter, config, on_disconnect, session_gate
            self.on_market_book = on_market_book

        def start(self):
            self.on_market_book(metadata_rich_book)
            return {"started": True}

        def stop(self):
            return {"stopped": True}

    monkeypatch.setattr("core.runtime_controller.StreamingFeed", _FakeStreamingFeed)

    rc = RuntimeController(
        bus=_Bus(),
        db=_DB(),
        settings_service=_SettingsStream(),
        betfair_service=_Betfair(),
        telegram_service=_Telegram(),
    )
    rc.simulation_mode = False

    received = []
    rc.market_tracker.on_market_book = lambda book: received.append(dict(book))
    rc._start_market_data_feed()

    assert len(received) == 1
    out = received[0]
    assert out["marketId"] == "1.903"
    assert out["status"] == "SUSPENDED"
    assert out["marketDefinition"]["keyLineDefinition"] == {"kl": [{"id": 301, "hc": 0.0}]}
    assert out["marketDefinition"]["runners"][0]["adjustmentFactor"] == pytest.approx(14.2)
    runner = out["runners"][0]
    assert runner["selectionId"] == 301
    assert runner["status"] == "REMOVED"
    assert runner["adjustmentFactor"] == pytest.approx(14.2)
    assert runner["removalDate"] == "2026-04-01T09:30:00Z"
    assert runner["bsp"] == pytest.approx(3.6)
    assert runner["spn"] == pytest.approx(3.4)
    assert runner["spf"] == pytest.approx(3.8)


@pytest.mark.integration
def test_runtime_controller_turbulence_recovery_clears_degraded_and_restores_coherence(monkeypatch):
    class _SettingsStream(_Settings):
        def load_market_data_config(self):
            return {
                "market_data_mode": "stream",
                "enabled": True,
                "market_ids": ["1.904"],
                "heartbeat_timeout_sec": 2,
            }

    class _FakeStreamingFeed:
        def __init__(self, *, client_getter, config, on_market_book, on_disconnect, session_gate=None):
            _ = client_getter, config, on_disconnect, session_gate
            self.on_market_book = on_market_book
            self._degraded = True

        def start(self):
            self.on_market_book(
                {
                    "marketId": "1.904",
                    "market_id": "1.904",
                    "status": "SUSPENDED",
                    "inplay": False,
                    "marketDefinition": {"status": "SUSPENDED", "inPlay": False},
                    "runners": [
                        {
                            "selectionId": 401,
                            "runnerName": "Runner 401",
                            "status": "ACTIVE",
                            "ex": {"availableToBack": [{"price": 2.2, "size": 5.0}], "availableToLay": []},
                        }
                    ],
                }
            )
            return {"started": True}

        def emit_recovered(self):
            self._degraded = False
            self.on_market_book(
                {
                    "marketId": "1.904",
                    "market_id": "1.904",
                    "status": "OPEN",
                    "inplay": True,
                    "marketDefinition": {
                        "status": "OPEN",
                        "inPlay": True,
                        "runners": [{"id": 401, "name": "Runner 401", "status": "ACTIVE", "adjustmentFactor": 8.4}],
                    },
                    "runners": [
                        {
                            "selectionId": 401,
                            "runnerName": "Runner 401",
                            "status": "ACTIVE",
                            "adjustmentFactor": 8.4,
                            "ex": {
                                "availableToBack": [{"price": 2.1, "size": 12.0}],
                                "availableToLay": [{"price": 2.2, "size": 11.0}],
                                "tradedVolume": [{"price": 2.15, "size": 90.0}],
                            },
                        }
                    ],
                }
            )

        def stop(self):
            return {"stopped": True}

        def status(self):
            if self._degraded:
                return {
                    "running": True,
                    "auth_degraded": True,
                    "keepalive_failure_count": 1,
                    "last_keepalive_error": "SESSION_EXPIRED",
                }
            return {
                "running": True,
                "auth_degraded": False,
                "keepalive_failure_count": 0,
                "last_keepalive_error": "",
            }

    monkeypatch.setattr("core.runtime_controller.StreamingFeed", _FakeStreamingFeed)

    rc = RuntimeController(
        bus=_Bus(),
        db=_DB(),
        settings_service=_SettingsStream(),
        betfair_service=_Betfair(),
        telegram_service=_Telegram(),
    )
    rc.simulation_mode = False

    received = []
    rc.market_tracker.on_market_book = lambda book: received.append(dict(book))
    rc._start_market_data_feed()

    degraded_status = rc.get_status()
    assert degraded_status["streaming_feed"]["auth_degraded"] is True
    assert degraded_status["streaming_feed"]["keepalive_failure_count"] == 1

    assert rc.streaming_feed is not None
    rc.streaming_feed.emit_recovered()

    recovered_status = rc.get_status()
    assert recovered_status["streaming_feed"]["auth_degraded"] is False
    assert recovered_status["streaming_feed"]["keepalive_failure_count"] == 0

    assert len(received) >= 2
    final = received[-1]
    assert final["marketId"] == "1.904"
    assert final["status"] == "OPEN"
    assert final["inplay"] is True
    assert final["marketDefinition"]["status"] == "OPEN"
    assert final["marketDefinition"]["runners"][0]["adjustmentFactor"] == pytest.approx(8.4)
    runner = final["runners"][0]
    assert runner["selectionId"] == 401
    assert runner["status"] == "ACTIVE"
    assert runner["adjustmentFactor"] == pytest.approx(8.4)
    assert runner["ex"]["availableToBack"] == [{"price": 2.1, "size": 12.0}]
    assert runner["ex"]["tradedVolume"] == [{"price": 2.15, "size": 90.0}]
