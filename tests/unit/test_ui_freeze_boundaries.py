from __future__ import annotations

from concurrent.futures import Future
import threading

import pytest

from observability.watchdog_service import WatchdogService
from telegram_module import TelegramModule


class _DoneExecutor:
    def __init__(self, *args, **kwargs):
        _ = args, kwargs
        self.calls: list[str] = []

    def submit(self, name, fn):
        self.calls.append(str(name))
        fut: Future = Future()
        try:
            fut.set_result(fn())
        except Exception as exc:  # pragma: no cover
            fut.set_exception(exc)
        return fut

    def shutdown(self, wait=True, cancel_futures=False):
        _ = wait, cancel_futures
        return None


class _PendingExecutor:
    def __init__(self, *args, **kwargs):
        _ = args, kwargs
        self.calls: list[str] = []
        self.futures: list[Future] = []

    def submit(self, name, fn):
        self.calls.append(str(name))
        fut: Future = Future()
        self.futures.append(fut)
        return fut

    def shutdown(self, wait=True, cancel_futures=False):
        _ = wait, cancel_futures
        return None


class _Var:
    def __init__(self, value):
        self._value = value

    def get(self):
        return self._value


class _UIQ:
    def post(self, fn, *args, **kwargs):
        fn(*args, **kwargs)


class _Bus:
    def __init__(self):
        self.events = []
        self.subscribers = {}

    def publish(self, name, payload=None):
        self.events.append((name, payload))


class _DB:
    def __init__(self):
        self.saved = []

    def save_received_signal(self, payload):
        self.saved.append(dict(payload or {}))


class _TelegramHarness(TelegramModule):
    def __init__(self):
        self.executor = _DoneExecutor()
        self.uiq = _UIQ()
        self.bus = _Bus()
        self.db = _DB()
        self.simulation_mode = True
        self.tg_auto_stake_var = _Var("1")
        self.tg_auto_bet_var = _Var(True)
        self.tg_confirm_var = _Var(False)
        self.resolution_calls = 0

    def _resolve_signal_to_payload(self, signal_data: dict, stake: float):
        self.resolution_calls += 1
        return {
            "market_id": "1.123",
            "selection_id": 11,
            "bet_type": "BACK",
            "price": 2.0,
            "stake": stake,
            "runner_name": "Runner",
            "telegram_boundary_stage": "telegram_ingestion_normalized_v1",
        }, "AUTO_RESOLVED"

    def _refresh_telegram_signals_tree(self):
        return None


@pytest.mark.unit
def test_telegram_signal_resolution_is_submitted_to_executor():
    h = _TelegramHarness()
    h.bus.subscribers = {"SIGNAL_RECEIVED": [object()]}

    h._handle_telegram_signal({"event_name": "A v B", "price": 2.1})

    assert "telegram_signal_resolution" in h.executor.calls
    assert h.resolution_calls == 1
    assert any(e[0] == "SIGNAL_RECEIVED" for e in h.bus.events)


@pytest.mark.unit
def test_telegram_module_prefers_signal_received_when_present():
    h = _TelegramHarness()
    h.bus.subscribers = {"REQ_QUICK_BET": [object()], "SIGNAL_RECEIVED": [object()]}

    h._publish_order_signal(
        {
            "market_id": "1.1",
            "selection_id": 10,
            "telegram_boundary_stage": "telegram_ingestion_normalized_v1",
        }
    )

    assert h.bus.events[0][0] == "SIGNAL_RECEIVED"
    assert h.bus.events[0][1]["telegram_routing_contract"] == "telegram_authoritative_routing_v1"
    assert h.bus.events[0][1]["telegram_route_target"] == "SIGNAL_RECEIVED"


@pytest.mark.unit
def test_telegram_module_falls_back_to_req_quick_bet_when_runtime_gate_not_wired():
    h = _TelegramHarness()
    h.bus.subscribers = {"REQ_QUICK_BET": [object()]}

    h._publish_order_signal(
        {
            "market_id": "1.2",
            "selection_id": 20,
            "telegram_boundary_stage": "telegram_ingestion_normalized_v1",
        }
    )

    assert h.bus.events[0][0] == "REQ_QUICK_BET"
    assert h.bus.events[0][1]["telegram_route_target"] == "REQ_QUICK_BET"


@pytest.mark.unit
def test_telegram_module_detects_eventbus_private_subscribers_shape():
    h = _TelegramHarness()

    class _EventBusLike:
        def __init__(self):
            self._subscribers = {"SIGNAL_RECEIVED": [object()]}
            self._lock = threading.Lock()
            self.events = []

        def publish(self, name, payload=None):
            self.events.append((name, payload))

    h.bus = _EventBusLike()
    h._publish_order_signal(
        {
            "market_id": "1.2",
            "selection_id": 20,
            "telegram_boundary_stage": "telegram_ingestion_normalized_v1",
        }
    )

    assert h.bus.events[0][0] == "SIGNAL_RECEIVED"


@pytest.mark.unit
def test_mini_gui_runtime_actions_are_delegated_to_executor(monkeypatch):
    import mini_gui

    class _FakeDB:
        def close_all_connections(self):
            return None

    class _FakeBus:
        def subscribe(self, *_a, **_k):
            return None

        def publish(self, *_a, **_k):
            return None

    class _FakeShutdown:
        def register(self, *_a, **_k):
            return None

    class _FakeSettings:
        def __init__(self, _db):
            pass

        def load_betfair_config(self):
            return {}

        def load_roserpina_config(self):
            return {}

        def load_simulation_config(self):
            return {}

        def load_execution_settings(self):
            return {}

    class _FakeBetfair:
        def __init__(self, _settings):
            pass

        def get_client(self):
            return None

        def set_simulation_mode(self, _value):
            return None

        def status(self):
            return {"connected": False}

        def disconnect(self):
            return None

    class _FakeTelegramService:
        def __init__(self, *_a, **_k):
            pass

        def status(self):
            return {"connected": False}

        def stop(self):
            return None

    class _FakeRuntime:
        def __init__(self, **_kwargs):
            self.start_calls = 0
            self.status_calls = 0

        def set_simulation_mode(self, _v):
            return None

        def start(self, **_kwargs):
            self.start_calls += 1
            return {"started": True}

        def get_status(self):
            self.status_calls += 1
            return {"mode": "STOPPED", "tables": []}

    class _FakeTradingEngine:
        def __init__(self, **_kwargs):
            self.runtime_controller = None
            self.simulation_broker = None
            self.betfair_client = None

    monkeypatch.setattr(mini_gui, "Database", _FakeDB)
    monkeypatch.setattr(mini_gui, "EventBus", _FakeBus)
    monkeypatch.setattr(mini_gui, "ExecutorManager", _DoneExecutor)
    monkeypatch.setattr(mini_gui, "ShutdownManager", _FakeShutdown)
    monkeypatch.setattr(mini_gui, "SettingsService", _FakeSettings)
    monkeypatch.setattr(mini_gui, "BetfairService", _FakeBetfair)
    monkeypatch.setattr(mini_gui, "TelegramService", _FakeTelegramService)
    monkeypatch.setattr(mini_gui, "TradingEngine", _FakeTradingEngine)
    monkeypatch.setattr(mini_gui, "RuntimeController", _FakeRuntime)

    gui = mini_gui.MiniPickfairGUI(test_mode=True)
    try:
        gui._runtime_start()
        gui._refresh_runtime_status()
        assert "gui_start" in gui.executor.calls
        assert "gui_refresh_runtime_status" in gui.executor.calls
    finally:
        gui.destroy()


@pytest.mark.unit
def test_risk_tree_row_payload_matches_defined_columns(monkeypatch):
    import mini_gui

    class _FakeDB:
        def close_all_connections(self):
            return None

    class _FakeBus:
        def subscribe(self, *_a, **_k):
            return None

        def publish(self, *_a, **_k):
            return None

    class _FakeShutdown:
        def register(self, *_a, **_k):
            return None

    class _FakeSettings:
        def __init__(self, _db):
            pass

        def load_betfair_config(self):
            return {}

        def load_roserpina_config(self):
            return {}

        def load_simulation_config(self):
            return {}

        def load_execution_settings(self):
            return {}

    class _FakeBetfair:
        def __init__(self, _settings):
            pass

        def get_client(self):
            return None

        def set_simulation_mode(self, _value):
            return None

        def status(self):
            return {"connected": False}

        def disconnect(self):
            return None

    class _FakeTelegramService:
        def __init__(self, *_a, **_k):
            pass

        def status(self):
            return {"connected": False}

        def stop(self):
            return None

    class _FakeRuntime:
        def __init__(self, **_kwargs):
            pass

        def set_simulation_mode(self, _v):
            return None

        def get_status(self):
            return {
                "mode": "ACTIVE",
                "tables": [
                    {
                        "table_id": 7,
                        "status": "ACTIVE",
                        "loss_amount": 1.5,
                        "current_exposure": 2.5,
                        "current_event_key": "EVT-1",
                        "market_id": "1.123",
                        "selection_id": 99,
                    }
                ],
            }

    class _FakeTradingEngine:
        def __init__(self, **_kwargs):
            self.runtime_controller = None
            self.simulation_broker = None
            self.betfair_client = None

    monkeypatch.setattr(mini_gui, "Database", _FakeDB)
    monkeypatch.setattr(mini_gui, "EventBus", _FakeBus)
    monkeypatch.setattr(mini_gui, "ExecutorManager", _DoneExecutor)
    monkeypatch.setattr(mini_gui, "ShutdownManager", _FakeShutdown)
    monkeypatch.setattr(mini_gui, "SettingsService", _FakeSettings)
    monkeypatch.setattr(mini_gui, "BetfairService", _FakeBetfair)
    monkeypatch.setattr(mini_gui, "TelegramService", _FakeTelegramService)
    monkeypatch.setattr(mini_gui, "TradingEngine", _FakeTradingEngine)
    monkeypatch.setattr(mini_gui, "RuntimeController", _FakeRuntime)

    gui = mini_gui.MiniPickfairGUI(test_mode=True)
    try:
        gui._refresh_runtime_status()
        assert len(gui.risk_tree.rows) == 1
        row = gui.risk_tree.rows[0]
        assert len(row) == 7
        assert row[4] == "EVT-1"
        assert row[5] == "1.123"
        assert row[6] == 99
    finally:
        gui.destroy()


@pytest.mark.unit
def test_runtime_command_coalescing_prevents_unbounded_executor_stacking(monkeypatch):
    import mini_gui

    class _FakeDB:
        def close_all_connections(self):
            return None

    class _FakeBus:
        def subscribe(self, *_a, **_k):
            return None

        def publish(self, *_a, **_k):
            return None

    class _FakeShutdown:
        def register(self, *_a, **_k):
            return None

    class _FakeSettings:
        def __init__(self, _db):
            pass

        def load_betfair_config(self):
            return {}

        def load_roserpina_config(self):
            return {}

        def load_simulation_config(self):
            return {}

        def load_execution_settings(self):
            return {}

    class _FakeBetfair:
        def __init__(self, _settings):
            pass

        def get_client(self):
            return None

        def set_simulation_mode(self, _value):
            return None

        def status(self):
            return {"connected": False}

        def disconnect(self):
            return None

    class _FakeTelegramService:
        def __init__(self, *_a, **_k):
            pass

        def status(self):
            return {"connected": False}

        def stop(self):
            return None

    class _FakeRuntime:
        def __init__(self, **_kwargs):
            self.start_calls = 0

        def set_simulation_mode(self, _v):
            return None

        def start(self, **_kwargs):
            self.start_calls += 1
            return {"started": True}

        def get_status(self):
            return {"mode": "STOPPED", "tables": []}

    class _FakeTradingEngine:
        def __init__(self, **_kwargs):
            self.runtime_controller = None
            self.simulation_broker = None
            self.betfair_client = None

    monkeypatch.setattr(mini_gui, "Database", _FakeDB)
    monkeypatch.setattr(mini_gui, "EventBus", _FakeBus)
    monkeypatch.setattr(mini_gui, "ExecutorManager", _PendingExecutor)
    monkeypatch.setattr(mini_gui, "ShutdownManager", _FakeShutdown)
    monkeypatch.setattr(mini_gui, "SettingsService", _FakeSettings)
    monkeypatch.setattr(mini_gui, "BetfairService", _FakeBetfair)
    monkeypatch.setattr(mini_gui, "TelegramService", _FakeTelegramService)
    monkeypatch.setattr(mini_gui, "TradingEngine", _FakeTradingEngine)
    monkeypatch.setattr(mini_gui, "RuntimeController", _FakeRuntime)

    gui = mini_gui.MiniPickfairGUI(test_mode=True)
    try:
        gui._runtime_start()
        gui._runtime_start()
        assert gui.executor.calls.count("gui_start") == 1
        assert gui._runtime_command_rejected_total == 1
    finally:
        gui.destroy()


class _StopEventProbe:
    def __init__(self):
        self.wait_calls = 0
        self._set = False

    def is_set(self):
        return self._set

    def wait(self, _timeout):
        self.wait_calls += 1
        self._set = True


@pytest.mark.unit
def test_watchdog_loop_waits_between_ticks_not_busy_spin(monkeypatch):
    class _Probe:
        def collect_health(self):
            return {}

        def collect_metrics(self):
            return {"gauges": {}}

    class _Registry:
        def snapshot(self):
            return {}

    class _Alerts:
        def snapshot(self):
            return {}

    class _Incidents:
        def snapshot(self):
            return {}

    svc = WatchdogService(
        probe=_Probe(),
        health_registry=_Registry(),
        metrics_registry=_Registry(),
        alerts_manager=_Alerts(),
        incidents_manager=_Incidents(),
        snapshot_service=_Registry(),
        interval_sec=0.01,
    )

    stop_event = _StopEventProbe()
    tick_count = {"n": 0}

    monkeypatch.setattr(svc, "_stop_event", stop_event)
    monkeypatch.setattr(svc, "_tick", lambda: tick_count.__setitem__("n", tick_count["n"] + 1))

    svc._run_loop()

    assert tick_count["n"] == 1
    assert stop_event.wait_calls == 1
