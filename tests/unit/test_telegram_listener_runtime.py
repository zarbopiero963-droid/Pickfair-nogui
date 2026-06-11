from __future__ import annotations

import asyncio
import threading
import time

import pytest

from telegram_listener import TelegramListener, _keyword_searchable_text


class FakeTelethonClient:
    """Client fake con la stessa interfaccia async usata dal runtime."""

    def __init__(self, *, authorized: bool = True, connect_error: Exception | None = None,
                 slow_connect: bool = False):
        self.authorized = authorized
        self.connect_error = connect_error
        self.slow_connect = slow_connect
        self.handlers = []
        self.connected = False
        self._disconnected = None  # asyncio.Event creato nel loop del runtime
        self._connect_gate = None

    def release_connect(self, loop):
        loop.call_soon_threadsafe(self._connect_gate.set)

    async def connect(self):
        if self.connect_error is not None:
            raise self.connect_error
        if self.slow_connect:
            self._connect_gate = asyncio.Event()
            await self._connect_gate.wait()
        self.connected = True
        self._disconnected = asyncio.Event()

    async def is_user_authorized(self):
        return self.authorized

    def add_event_handler(self, callback, event_filter=None):
        self.handlers.append(callback)
        self.event_filters = getattr(self, "event_filters", [])
        self.event_filters.append(event_filter)

    def is_connected(self):
        return self.connected

    async def run_until_disconnected(self):
        await self._disconnected.wait()

    async def disconnect(self):
        self.connected = False
        if self._disconnected is not None:
            self._disconnected.set()


def _make_listener(client, *, chats=(-100999,), connect_timeout=5.0, **kwargs):
    listener = TelegramListener(
        api_id=1,
        api_hash="x",
        session_string="sess",
        client_factory=lambda api_id, api_hash, session: client,
        connect_timeout=connect_timeout,
        **kwargs,
    )
    listener.set_monitored_chats(list(chats))
    return listener


# ---------------------------------------------------------------------------
# PASS: lifecycle con client fake
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_start_reaches_connected_with_live_client():
    client = FakeTelethonClient()
    listener = _make_listener(client)
    try:
        result = listener.start()
        assert result["started"] is True
        assert result["state"] == "CONNECTED"
        snap = listener.runtime_snapshot()
        assert snap["client_alive"] is True
        assert snap["handlers_registered"] == 1
        assert snap["active_network_resources"] == 1
        assert len(client.handlers) == 1
    finally:
        listener.stop()


@pytest.mark.unit
def test_stop_releases_runtime_resources():
    client = FakeTelethonClient()
    listener = _make_listener(client)
    listener.start()
    result = listener.stop()
    assert result["stopped"] is True
    snap = listener.runtime_snapshot()
    assert snap["state"] == "STOPPED"
    assert snap["active_network_resources"] == 0
    assert snap["handlers_registered"] == 0
    assert snap["client_alive"] is False
    deadline = time.time() + 5
    while listener._runtime_thread.is_alive() and time.time() < deadline:
        time.sleep(0.05)
    assert not listener._runtime_thread.is_alive()


@pytest.mark.unit
def test_incoming_message_emits_signal_and_liveness():
    client = FakeTelethonClient()
    listener = _make_listener(client)
    signals, messages = [], []
    listener.set_callbacks(
        on_signal=signals.append, on_message=messages.append
    )

    class Db:
        def get_signal_patterns(self):
            return [{
                "id": 1, "name": "ng", "enabled": True, "pattern": "",
                "keyword": "NEXT GOL", "market_type": "NEXT_GOAL",
                "bet_side": "BACK", "selection_template": "Next Goal",
            }]

    listener.set_database(Db())
    try:
        listener.start()
        out = listener.handle_incoming(
            "🆚Reading v Burton Albion\n⌚ time, 11m, 0 - 0\n🔥 P.Exc. NEXT GOL 🔊 ✅",
            chat_id=-100123,
        )
        assert out is not None
        assert out["market_type"] == "NEXT_GOAL"
        assert out["chat_id"] == -100123
        assert out["received_at"]
        assert listener.last_successful_message_ts == out["received_at"]
        assert len(signals) == 1 and len(messages) == 1
    finally:
        listener.stop()


@pytest.mark.unit
def test_non_signal_message_updates_liveness_only():
    client = FakeTelethonClient()
    listener = _make_listener(client)
    try:
        listener.start()
        out = listener.handle_incoming("chiacchiere senza segnale")
        assert out is None
        assert listener.last_successful_message_ts is not None
    finally:
        listener.stop()


@pytest.mark.unit
def test_connected_seeds_liveness_timestamp():
    client = FakeTelethonClient()
    listener = _make_listener(client)
    try:
        listener.start()
        # Senza seed il guard segnerebbe STALE_RUNTIME_NO_TIMESTAMP su un
        # canale sano ma silenzioso, e l'autoheal lo riavvierebbe.
        assert listener.state == "CONNECTED"
        assert listener.last_successful_message_ts is not None
    finally:
        listener.stop()


@pytest.mark.unit
def test_monitored_chats_filter_applied_with_injected_factory():
    client = FakeTelethonClient()
    listener = _make_listener(client)
    listener.set_monitored_chats([-100123, -100456])
    try:
        listener.start()
        assert len(client.event_filters) == 1
        event_filter = client.event_filters[0]
        # Con telethon installato il filtro NewMessage va passato anche ai
        # client iniettati: senza, il listener processerebbe tutte le chat.
        assert event_filter is not None
        assert list(getattr(event_filter, "chats", [])) == [-100123, -100456]
    finally:
        listener.stop()


@pytest.mark.unit
def test_unexpected_disconnect_marks_failed():
    client = FakeTelethonClient()
    listener = _make_listener(client)
    listener.start()
    assert listener.state == "CONNECTED"

    # Disconnessione lato client SENZA listener.stop(): fail-closed.
    future = asyncio.run_coroutine_threadsafe(client.disconnect(), listener._runtime_loop)
    future.result(timeout=5)

    deadline = time.time() + 5
    while listener.state != "FAILED" and time.time() < deadline:
        time.sleep(0.05)

    assert listener.state == "FAILED"
    assert listener.last_error == "disconnected_unexpectedly"
    snap = listener.runtime_snapshot()
    assert snap["active_network_resources"] == 0
    assert snap["client_alive"] is False


# ---------------------------------------------------------------------------
# BLOCK: fail-closed
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_missing_session_fails_closed_without_factory():
    listener = TelegramListener(api_id=1, api_hash="x", session_string=None)
    result = listener.start()
    assert result["started"] is False
    assert listener.state == "FAILED"
    assert listener.last_error in {"missing_session_string", "telethon_not_available"}


@pytest.mark.unit
def test_empty_monitored_chats_fails_closed():
    # chats=None in Telethon significa NESSUN filtro: il listener
    # ascolterebbe tutti i dialoghi dell'account. Mai di default.
    client = FakeTelethonClient()
    listener = _make_listener(client, chats=())
    result = listener.start()
    assert result["started"] is False
    assert listener.state == "FAILED"
    assert listener.last_error == "no_monitored_chats"


@pytest.mark.unit
def test_unauthorized_session_fails_closed():
    client = FakeTelethonClient(authorized=False)
    listener = _make_listener(client)
    result = listener.start()
    assert result["started"] is False
    assert listener.state == "FAILED"
    assert listener.last_error == "session_not_authorized"
    assert listener.runtime_snapshot()["active_network_resources"] == 0


@pytest.mark.unit
def test_connect_error_fails_closed():
    client = FakeTelethonClient(connect_error=RuntimeError("boom"))
    listener = _make_listener(client)
    result = listener.start()
    assert result["started"] is False
    assert listener.state == "FAILED"
    assert "boom" in listener.last_error


@pytest.mark.unit
def test_stop_during_slow_connect_aborts_startup():
    client = FakeTelethonClient(slow_connect=True)
    listener = _make_listener(client, connect_timeout=0.2)
    result = listener.start()
    # Timeout di connessione = startup fallita (mai CONNECTING eterno:
    # bloccherebbe l'autoheal con un runtime appeso indefinitamente).
    assert result["started"] is False
    assert listener.state == "FAILED"
    assert listener.last_error == "connect_timeout"

    # stop() mentre il connect è in volo: NON deve restare un handler vivo.
    stopper = threading.Thread(target=listener.stop)
    stopper.start()
    time.sleep(0.1)
    client.release_connect(listener._runtime_loop)
    stopper.join(timeout=15)
    assert not stopper.is_alive()

    deadline = time.time() + 5
    while listener._runtime_thread.is_alive() and time.time() < deadline:
        time.sleep(0.05)
    assert not listener._runtime_thread.is_alive()
    assert listener.state == "STOPPED"
    assert client.handlers == []  # mai registrato
    assert listener.runtime_handlers_registered == 0


@pytest.mark.unit
def test_stop_timeout_does_not_claim_stopped_with_live_runtime():
    client = FakeTelethonClient(slow_connect=True)
    listener = _make_listener(client, connect_timeout=0.2)
    listener._stop_timeout = 0.2
    listener.start()

    # connect mai completato e stop_timeout breve: il thread resta vivo.
    result = listener.stop()
    assert result["stopped"] is False
    assert listener.state == "FAILED"
    assert listener.last_error == "stop_timeout_runtime_thread_alive"

    # Un nuovo start NON deve sovrapporsi al runtime ancora vivo.
    again = listener.start()
    assert again["started"] is False
    assert listener.last_error == "previous_runtime_still_alive"

    # Cleanup: sblocca il connect, il runtime esce da solo (intentional_stop).
    client.release_connect(listener._runtime_loop)
    deadline = time.time() + 5
    while listener._runtime_thread.is_alive() and time.time() < deadline:
        time.sleep(0.05)
    assert not listener._runtime_thread.is_alive()


@pytest.mark.unit
def test_double_start_is_idempotent():
    client = FakeTelethonClient()
    listener = _make_listener(client)
    try:
        listener.start()
        again = listener.start()
        assert again["reason"] == "already_running"
        assert listener.runtime_snapshot()["handlers_registered"] == 1
    finally:
        listener.stop()


# ---------------------------------------------------------------------------
# Keyword: esclusione righe statistiche
# ---------------------------------------------------------------------------

_PBET_OVER_SUCCESSIVO = """P.Bet. OVER SUCCESSIVO 🔊 ❌

🏆Welsh Premiership
🆚Colwyn Bay v Flint Town United
⚽ 0 - 0
⌚ 5m
🥅Tiri in Porta  0-1
🎯Tiri Fuori  0-2
Possesso Palla: 48-52

📈Quota 0,5 HT Prematch:1.3
📊70.21%"""


@pytest.mark.unit
def test_keyword_ignores_stats_lines():
    text = _keyword_searchable_text(_PBET_OVER_SUCCESSIVO)
    assert "Quota 0,5 HT" not in text
    assert "Tiri in Porta" not in text
    assert "Possesso" not in text
    assert "OVER SUCCESSIVO" in text


@pytest.mark.unit
def test_keyword_in_stats_line_does_not_trigger_pattern():
    class Db:
        def get_signal_patterns(self):
            return [{
                "id": 1, "name": "05ht", "enabled": True, "pattern": "",
                "keyword": "0,5 HT", "market_type": "OVER_UNDER_HT_05",
                "bet_side": "BACK", "selection_template": "Over 0.5",
            }]

    listener = TelegramListener(api_id=1, api_hash="x", db=Db())
    # La keyword "0,5 HT" compare SOLO nella riga statistica 📈Quota:
    # non deve scattare (mercato sbagliato su segnale OVER SUCCESSIVO).
    assert listener.parse_signal(_PBET_OVER_SUCCESSIVO) is None
    # Ma se la frase del segnale la contiene davvero, scatta.
    msg = "🆚A v B\n⌚ 5m, 0 - 0\nP.Bet. 0,5 HT LIVE 🔊 ✅\n📈Quota 0,5 HT Prematch:1.3"
    sig = listener.parse_signal(msg)
    assert sig is not None and sig["market_type"] == "OVER_UNDER_HT_05"
