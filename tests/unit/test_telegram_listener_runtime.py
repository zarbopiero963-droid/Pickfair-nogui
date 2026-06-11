from __future__ import annotations

import asyncio
import time

import pytest

from telegram_listener import TelegramListener, _keyword_searchable_text


class FakeTelethonClient:
    """Client fake con la stessa interfaccia async usata dal runtime."""

    def __init__(self, *, authorized: bool = True, connect_error: Exception | None = None):
        self.authorized = authorized
        self.connect_error = connect_error
        self.handlers = []
        self.connected = False
        self._disconnected = None  # asyncio.Event creato nel loop del runtime

    async def connect(self):
        if self.connect_error is not None:
            raise self.connect_error
        self.connected = True
        self._disconnected = asyncio.Event()

    async def is_user_authorized(self):
        return self.authorized

    def add_event_handler(self, callback, event_filter=None):
        self.handlers.append(callback)

    def is_connected(self):
        return self.connected

    async def run_until_disconnected(self):
        await self._disconnected.wait()

    async def disconnect(self):
        self.connected = False
        if self._disconnected is not None:
            self._disconnected.set()


def _make_listener(client, **kwargs):
    return TelegramListener(
        api_id=1,
        api_hash="x",
        session_string="sess",
        client_factory=lambda api_id, api_hash, session: client,
        connect_timeout=5.0,
        **kwargs,
    )


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
