"""PR-5b (epica #374) — orchestratore N-bot `TelegramMultiBotRuntime`.

Compone N `TelegramBotApiRuntime` indipendenti presentando la superficie
duck-typed attesa da `TelegramService.self.listener`, con semantica AGGREGATA
fail-closed. Qui i child sono FAKE (nessun transport/rete): si verifica solo la
logica di aggregazione (state/running/_runtime_thread/start/stop/status).
"""
from __future__ import annotations

import pytest

from telegram_bot_runtime import TelegramMultiBotRuntime, _child_thread_alive

pytestmark = pytest.mark.unit


class _FakeThread:
    def __init__(self, alive: bool):
        self._alive = alive

    def is_alive(self) -> bool:
        return self._alive


class _FakeChild:
    """Child runtime fake: espone la superficie che l'orchestratore consuma."""

    def __init__(self, *, state="CONNECTED", running=True, thread_alive=True,
                 handlers=1, chats=1, start_res=None, stop_res=None, last_error=""):
        self._state = state
        self._running = running
        self._thread = _FakeThread(thread_alive) if thread_alive is not None else None
        self._handlers = handlers
        self._chats = chats
        self._start_res = start_res if start_res is not None else {"started": True}
        self._stop_res = stop_res if stop_res is not None else {"stopped": True}
        self._last_error = last_error
        self.start_called = 0
        self.stop_called = 0

    @property
    def state(self):
        return self._state

    @property
    def running(self):
        return self._running

    @property
    def _runtime_thread(self):
        return self._thread

    def start(self):
        self.start_called += 1
        return self._start_res

    def stop(self):
        self.stop_called += 1
        # Dopo uno stop "riuscito" il thread muore (per i fake che lo simulano).
        if self._stop_res.get("stopped"):
            self._thread = None
        return self._stop_res

    def status(self):
        return {
            "state": self._state,
            "running": self._running,
            "handlers_registered": self._handlers,
            "active_network_resources": self._handlers,
            "monitored_chat_count": self._chats,
            "listener_started": self._running,
            "last_error": self._last_error,
            "last_successful_message_ts": None,
        }


def test_requires_at_least_one_runtime():
    with pytest.raises(ValueError):
        TelegramMultiBotRuntime([])


def test_all_connected_aggregates_connected_and_sums():
    a = _FakeChild(handlers=1, chats=2)
    b = _FakeChild(handlers=1, chats=3)
    rt = TelegramMultiBotRuntime([a, b])
    assert rt.state == "CONNECTED"
    assert rt.running is True
    st = rt.status()
    assert st["handlers_registered"] == 2
    assert st["monitored_chat_count"] == 5
    assert st["expected_handlers"] == 2
    assert st["bot_count"] == 2
    assert st["healthy_bot_count"] == 2


def test_one_child_failed_aggregates_failed():
    # BLOCK fail-closed: un solo bot degradato => l'intero runtime è FAILED (in
    # PR-5b l'autoheal service-level fa restart-all; per-bot in PR-5c).
    a = _FakeChild(state="CONNECTED", handlers=1)
    b = _FakeChild(state="FAILED", running=False, handlers=0)
    rt = TelegramMultiBotRuntime([a, b])
    assert rt.state == "FAILED"
    assert rt.running is False
    assert rt.status()["healthy_bot_count"] == 1


def test_all_stopped_and_all_created_aggregate():
    stopped = TelegramMultiBotRuntime([_FakeChild(state="STOPPED", running=False),
                                       _FakeChild(state="STOPPED", running=False)])
    assert stopped.state == "STOPPED"
    created = TelegramMultiBotRuntime([_FakeChild(state="CREATED", running=False),
                                       _FakeChild(state="CREATED", running=False)])
    assert created.state == "CREATED"


def test_runtime_thread_any_alive():
    dead = _FakeChild(thread_alive=False)
    alive = _FakeChild(thread_alive=True)
    rt = TelegramMultiBotRuntime([dead, alive])
    assert rt._runtime_thread.is_alive() is True
    both_dead = TelegramMultiBotRuntime([_FakeChild(thread_alive=False),
                                         _FakeChild(thread_alive=False)])
    assert both_dead._runtime_thread.is_alive() is False


def test_start_all_ok():
    a = _FakeChild(start_res={"started": True})
    b = _FakeChild(start_res={"started": True})
    rt = TelegramMultiBotRuntime([a, b])
    out = rt.start()
    assert out == {"started": True}
    assert a.start_called == 1 and b.start_called == 1


def test_start_partial_failure_is_failclosed_and_does_not_stop_started():
    # BLOCK: se un bot non parte, l'aggregato è started=False con errori; i child
    # già avviati NON vengono fermati qui (thread visibile => guard blocca retry).
    a = _FakeChild(start_res={"started": True})
    b = _FakeChild(start_res={"started": False, "error": "boom_b"})
    rt = TelegramMultiBotRuntime([a, b])
    out = rt.start()
    assert out["started"] is False
    assert "boom_b" in out["error"]
    assert a.stop_called == 0  # il child avviato NON è stato fermato


def test_stop_all_dead_is_stopped():
    a = _FakeChild(thread_alive=True, stop_res={"stopped": True})
    b = _FakeChild(thread_alive=True, stop_res={"stopped": True})
    rt = TelegramMultiBotRuntime([a, b])
    out = rt.stop()
    assert out == {"stopped": True}
    assert rt.intentional_stop is True


def test_stop_failclosed_if_any_child_thread_survives():
    # BLOCK anti-409: se un thread child sopravvive allo stop, l'aggregato NON è
    # stopped (un restart aprirebbe un secondo getUpdates sullo stesso token).
    a = _FakeChild(thread_alive=True, stop_res={"stopped": True})

    class _Survivor(_FakeChild):
        def stop(self):
            self.stop_called += 1
            return {"stopped": False, "error": "bot_transport_thread_still_alive"}

    b = _Survivor(thread_alive=True)
    rt = TelegramMultiBotRuntime([a, b])
    out = rt.stop()
    assert out["stopped"] is False
    assert "multibot_thread_still_alive" in out["error"]
    assert rt.intentional_stop is False


def test_runtime_snapshot_carries_expected_handlers():
    rt = TelegramMultiBotRuntime([_FakeChild(handlers=1), _FakeChild(handlers=1)])
    snap = rt.runtime_snapshot()
    assert snap["handlers_registered"] == 2
    assert snap["expected_handlers"] == 2


def test_child_thread_alive_failclosed_on_exception():
    # Helper: se is_alive() solleva => fail-closed (True: non nascondere l'orfano).
    class _BoomThread:
        def is_alive(self):
            raise RuntimeError("boom")

    class _Child:
        _runtime_thread = _BoomThread()

    assert _child_thread_alive(_Child()) is True
