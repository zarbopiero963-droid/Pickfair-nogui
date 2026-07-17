"""PR-5a (epica #374) — wiring runtime del path Bot API in TelegramService.

Quando mancano le credenziali userbot (api_id/api_hash) ma è configurato UN bot
Bot API attivo con almeno una chat attiva, `TelegramService.start()` avvia il
`TelegramBotApiRuntime` (transport HTTP getUpdates) invece del listener Telethon.
Il path Telethon resta prioritario e invariato; con più bot attivi si fa
fail-closed (l'orchestrazione N-bot arriva dopo).

Test headless: il transport è iniettato via `bot_transport_factory` (come il
`client_factory` del path Telethon), così NON serve rete né telethon. La pipeline
di parsing (`handle_incoming` -> parse -> emit -> `_handle_signal` -> bus) è quella
reale del listener, usato come sink.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest

from services.telegram_service import TelegramService
from telegram_bot_runtime import TelegramBotApiRuntime

pytestmark = pytest.mark.unit

AUTHORIZED_CHAT = "-100999"

MSG_NEXT_GOL = """🆚Reading v Burton Albion
🏆English Sky Bet League 1
⌚ time, 11m, 0 - 0

🔥 P.Exc. NEXT GOL 🔊 ✅

📊88.33%"""


@dataclass
class _Cfg:
    enabled: bool = True
    api_id: str = ""
    api_hash: str = ""
    session_string: str = ""
    bot_token: str = ""
    monitored_chat_ids: list | None = None


class _Settings:
    def __init__(self, cfg: _Cfg):
        self.cfg = cfg

    def load_telegram_config(self):
        if self.cfg.monitored_chat_ids is None:
            self.cfg.monitored_chat_ids = []
        return self.cfg


class _Bus:
    def __init__(self):
        self.events = []

    def publish(self, topic, payload):
        self.events.append((topic, dict(payload or {})))


class _BotDB:
    """DB fake: espone solo ciò che il path Bot API usa."""

    def __init__(self, bots=None, chats_by_bot=None, patterns=None):
        self._bots = bots or []
        self._chats = chats_by_bot or {}
        self._patterns = patterns or []
        self.saved = []

    def get_telegram_bots(self, include_token=True):
        return [dict(b) for b in self._bots]

    def get_telegram_bot_chats(self, bot_id):
        return [dict(c) for c in self._chats.get(bot_id, [])]

    def get_signal_patterns(self, enabled_only=True):
        return [dict(p) for p in self._patterns]

    def save_received_signal(self, payload):
        self.saved.append(dict(payload))


class _FakeTransport:
    """Transport fake: cattura on_message, nessun thread reale."""

    def __init__(self, bot_token, chat_ids, on_message):
        self.bot_token = bot_token
        self.chat_ids = chat_ids
        self.on_message = on_message
        self._thread = None
        self.started = False
        self.stopped = False

    def start(self):
        self.started = True

    def stop(self, timeout=5.0):
        self.stopped = True


def _next_gol_pattern():
    return [{
        "id": 1, "name": "next-gol", "enabled": True, "pattern": "",
        "keyword": "NEXT GOL", "market_type": "NEXT_GOAL",
        "bet_side": "BACK", "selection_template": "Next Goal", "mm_auto": True,
    }]


def _one_active_bot_db(patterns=None):
    return _BotDB(
        bots=[{"id": 7, "label": "B", "bot_token": "fake-bot-value-xyz", "is_active": True, "has_token": True}],
        chats_by_bot={7: [{"chat_id": AUTHORIZED_CHAT, "title": "c", "is_active": True}]},
        patterns=patterns,
    )


def _make_factory(capture):
    def factory(bot_token, chat_ids, on_message):
        t = _FakeTransport(bot_token, chat_ids, on_message)
        capture.append(t)
        return t
    return factory


def _svc(db, *, capture=None):
    factory = _make_factory(capture) if capture is not None else None
    return TelegramService(
        settings_service=_Settings(_Cfg()),
        db=db,
        bus=_Bus(),
        bot_transport_factory=factory,
    )


def test_start_uses_botapi_when_no_telethon_creds():
    # BLOCK del gate rilassato: senza api_id/api_hash ma con un bot Bot API
    # attivo, start() DEVE avviare (sul vecchio codice sollevava
    # "Configurazione Telegram incompleta").
    cap = []
    svc = _svc(_one_active_bot_db(), capture=cap)
    result = svc.start()

    assert result["started"] is True
    assert result["state"] == "CONNECTED"
    assert result["chat_count"] == 1
    assert isinstance(svc.listener, TelegramBotApiRuntime)
    # transport costruito col token decifrato e l'allow-list della chat, e avviato
    assert len(cap) == 1 and cap[0].started is True
    assert cap[0].bot_token == "fake-bot-value-xyz"
    assert cap[0].chat_ids == [-100999]
    assert svc.handlers_registered == 1


def test_botapi_snapshot_is_invariant_coherent():
    # La coerenza health: CONNECTED => esattamente 1 handler nel runtime_snapshot
    # (l'invariant guard lo richiede). Il transport-fed listener non ha handler
    # Telethon: l'adapter mappa "transport attivo" a 1 handler.
    svc = _svc(_one_active_bot_db(), capture=[])
    svc.start()
    snap = svc.runtime_snapshot()
    assert snap["state"] == "CONNECTED"
    assert snap["handlers_registered"] == 1
    assert snap["running"] is True


def test_botapi_delivers_signal_to_bus():
    cap = []
    svc = _svc(_one_active_bot_db(patterns=_next_gol_pattern()), capture=cap)
    svc.start()

    # Simula un update consegnato dal transport (getUpdates) via on_message,
    # che è handle_incoming del sink: parse -> emit -> _handle_signal -> bus.
    fresh = datetime.now(timezone.utc) - timedelta(seconds=5)
    cap[0].on_message(MSG_NEXT_GOL, -100999, fresh)

    payloads = [p for t, p in svc.bus.events if t == "SIGNAL_RECEIVED"]
    assert payloads, "atteso un SIGNAL_RECEIVED sul bus"
    assert payloads[0].get("market_type") == "NEXT_GOAL"


def test_botapi_stale_message_does_not_emit_signal():
    # BLOCK anti-stale preservato attraverso il transport: un backlog vecchio
    # non genera segnale (riuso della guardia del listener).
    cap = []
    svc = _svc(_one_active_bot_db(patterns=_next_gol_pattern()), capture=cap)
    svc.start()

    old = datetime.now(timezone.utc) - timedelta(seconds=3600)
    cap[0].on_message(MSG_NEXT_GOL, -100999, old)

    assert "SIGNAL_RECEIVED" not in [t for t, _ in svc.bus.events]


def test_multi_active_bot_fails_closed():
    db = _BotDB(
        bots=[
            {"id": 1, "label": "A", "bot_token": "tok-a", "is_active": True, "has_token": True},
            {"id": 2, "label": "B", "bot_token": "tok-b", "is_active": True, "has_token": True},
        ],
        chats_by_bot={
            1: [{"chat_id": "-100111", "is_active": True}],
            2: [{"chat_id": "-100222", "is_active": True}],
        },
    )
    svc = _svc(db, capture=[])
    with pytest.raises(RuntimeError) as exc:
        svc.start()
    assert "multi_bot_runtime_not_yet_supported" in str(exc.value)
    assert svc.state == "FAILED"


def test_no_creds_no_bots_still_fails_closed():
    # Nessun userbot e nessun bot utilizzabile => fail-closed invariato.
    svc = _svc(_BotDB(), capture=[])
    with pytest.raises(RuntimeError) as exc:
        svc.start()
    assert "incompleta" in str(exc.value).lower()
    assert svc.state == "FAILED"


def test_bot_without_active_chat_is_not_usable():
    # Un bot attivo ma senza chat ATTIVE non è utilizzabile => fail-closed.
    db = _BotDB(
        bots=[{"id": 3, "label": "C", "bot_token": "tok-c", "is_active": True, "has_token": True}],
        chats_by_bot={3: [{"chat_id": "-100333", "is_active": False}]},
    )
    svc = _svc(db, capture=[])
    with pytest.raises(RuntimeError) as exc:
        svc.start()
    assert "incompleta" in str(exc.value).lower()


def test_telethon_path_selected_when_creds_present_no_botapi():
    # Regressione: con credenziali userbot presenti si usa il path Telethon,
    # MAI il transport Bot API, anche se un bot è configurato.
    cap = []
    db = _one_active_bot_db()
    svc = TelegramService(
        settings_service=_Settings(_Cfg(api_id="123", api_hash="hash", session_string="s")),
        db=db,
        bus=_Bus(),
        client_factory=lambda *_a: None,
        bot_transport_factory=lambda *a: cap.append(a) or _FakeTransport(*a),
    )
    try:
        svc.start()  # può fallire su telethon assente in questo ambiente: irrilevante
    except Exception:
        pass
    # Il fatto chiave: il transport Bot API NON è stato costruito (path Telethon).
    assert cap == []
    assert not isinstance(svc.listener, TelegramBotApiRuntime)


def test_transport_thread_death_reports_failed_not_silent_connected():
    # BLOCK (GPT/Fugu/Fable): se il thread getUpdates muore, lo stato NON resta
    # CONNECTED silenziosamente. Diventa FAILED con last_error e 0 handler, così
    # l'invariant guard e l'autoheal esistenti rilevano la perdita di ingestione.
    # Sul vecchio codice (state statico) lo snapshot restava CONNECTED.
    cap = []
    svc = _svc(_one_active_bot_db(), capture=cap)
    svc.start()
    assert svc.runtime_snapshot()["state"] == "CONNECTED"

    class _DeadThread:
        def is_alive(self):
            return False

    cap[0]._thread = _DeadThread()  # simula morte del thread del transport
    snap = svc.runtime_snapshot()
    assert snap["state"] == "FAILED"
    assert snap["running"] is False
    assert snap["handlers_registered"] == 0
    assert snap["last_error"]  # non vuoto -> l'autoheal ha di che agire


def test_bot_with_only_non_numeric_chat_is_failclosed():
    # BLOCK (Fable/Fugu/Codacy): un chat_id non numerico (es. @canale) non è
    # ascoltabile via getUpdates. Un bot con SOLE chat non numeriche è "non
    # usable" => fail-closed pulito ('incompleta'), NON un crash a start().
    db = _BotDB(
        bots=[{"id": 9, "label": "D", "bot_token": "tok-d", "is_active": True, "has_token": True}],
        chats_by_bot={9: [{"chat_id": "@miocanale", "is_active": True}]},
    )
    svc = _svc(db, capture=[])
    with pytest.raises(RuntimeError) as exc:
        svc.start()
    assert "incompleta" in str(exc.value).lower()
    assert svc.state == "FAILED"


def test_non_numeric_chat_is_skipped_numeric_kept():
    # Un bot con una chat numerica valida + una @canale: usa solo la numerica.
    db = _BotDB(
        bots=[{"id": 10, "label": "E", "bot_token": "tok-e", "is_active": True, "has_token": True}],
        chats_by_bot={10: [
            {"chat_id": "@canale", "is_active": True},
            {"chat_id": "-100777", "is_active": True},
        ]},
    )
    cap = []
    svc = _svc(db, capture=cap)
    result = svc.start()
    assert result["started"] is True
    assert cap[0].chat_ids == [-100777]  # @canale scartata, numerica tenuta


def test_already_running_reports_botapi_chat_count():
    # BLOCK (Greptile P1): una seconda start() mentre il servizio gira via Bot API
    # riporta il chat_count del BOT selezionato, non 0 (lista userbot vuota).
    cap = []
    svc = _svc(_one_active_bot_db(), capture=cap)
    svc.start()
    again = svc.start()
    assert again["reason"] == "already_running"
    assert again["chat_count"] == 1  # vecchio codice: 0 (len(cfg.monitored_chat_ids))


def test_stop_stops_transport():
    cap = []
    svc = _svc(_one_active_bot_db(), capture=cap)
    svc.start()
    assert cap[0].stopped is False
    out = svc.stop()
    assert out["stopped"] is True
    assert cap[0].stopped is True
    assert svc.state == "STOPPED"
    assert svc.listener is None


class _StuckTransport:
    """Transport fake che NON si ferma: `stopped` resta False (thread 'vivo')."""

    def __init__(self, *a):
        self.stopped = False
        self._thread = None

    def start(self):
        pass

    def stop(self, timeout=5.0):
        pass  # non ferma -> resta vivo


def test_botapi_stop_failclosed_if_transport_thread_survives():
    # BLOCK (CodeRabbit critical): se il thread del transport non termina allo
    # stop, l'adapter NON deve dichiarare stopped:True. Altrimenti il service
    # stacca il listener e un restart crea un SECONDO getUpdates sullo stesso
    # token -> Telegram 409 Conflict. Contratto fail-closed come il path Telethon.
    rt = TelegramBotApiRuntime(
        bot_token="tok",
        chat_ids=[123],
        transport_factory=lambda *a: _StuckTransport(),
    )
    rt.start()
    out = rt.stop()
    assert out["stopped"] is False
    assert "alive" in out["error"]
    assert rt.state == "FAILED"  # stato runtime coerente col service (non CONNECTED)
    assert rt.intentional_stop is False  # stop non avvenuto => recovery non soppresso
    assert rt._transport is not None      # riferimento mantenuto (thread ancora vivo)


def test_botapi_stop_success_clears_transport_and_is_idempotent():
    # Fable: uno stop riuscito azzera self._transport; un secondo stop resta
    # pulito (stopped:True), niente FAILED spurio su transport gia' fermo.
    cap = []
    svc = _svc(_one_active_bot_db(), capture=cap)
    svc.start()
    rt = svc.listener
    out1 = rt.stop()
    assert out1["stopped"] is True
    assert rt._transport is None
    out2 = rt.stop()
    assert out2["stopped"] is True
    assert rt.state == "STOPPED"


def test_botapi_start_failure_stops_transport_no_leak():
    # BLOCK (GPT/Fugu): se transport.start() solleva dopo aver (potenzialmente)
    # avviato il polling, il runtime deve FERMARE il transport (niente thread
    # orfano -> niente secondo getUpdates su un retry) e riportare started:False.
    holder = {}

    class _ExplodingTransport:
        def __init__(self):
            self.stopped = False
            self._thread = None

        def start(self):
            raise RuntimeError("boom dopo partial start")

        def stop(self, timeout=5.0):
            self.stopped = True

    def factory(*_a):
        t = _ExplodingTransport()
        holder["t"] = t
        return t

    rt = TelegramBotApiRuntime(
        bot_token="tok", chat_ids=[123], transport_factory=factory,
    )
    out = rt.start()
    assert out["started"] is False
    assert holder["t"].stopped is True   # transport fermato: nessun thread orfano
    assert rt.state == "FAILED"
    assert rt.running is False
    assert out["error"] == "RuntimeError"  # solo il tipo, mai il messaggio grezzo
    assert rt._transport is None  # thread morto dopo stop => riferimento azzerato


def test_botapi_start_failure_keeps_transport_if_thread_survives():
    # BLOCK (GPT/Fugu): se start() fallisce e il thread del transport resta VIVO,
    # il riferimento NON va azzerato: altrimenti il guard _runtime_thread del
    # service non lo vedrebbe e un retry aprirebbe un SECONDO getUpdates (409).
    class _StuckExplodingTransport:
        def __init__(self):
            self.stopped = False  # stop() non ferma -> resta vivo
            self._thread = None

        def start(self):
            raise RuntimeError("boom")

        def stop(self, timeout=5.0):
            pass

    rt = TelegramBotApiRuntime(
        bot_token="tok", chat_ids=[123],
        transport_factory=lambda *a: _StuckExplodingTransport(),
    )
    out = rt.start()
    assert out["started"] is False
    assert rt.state == "FAILED"
    assert rt._transport is not None      # KEPT (thread vivo): il guard del service lo vede
    assert rt.running is False            # started=False anche se il transport è vivo


def test_service_stop_failclosed_keeps_listener_if_transport_survives():
    # A livello service: stop con transport ancora vivo => stopped:False e il
    # listener NON viene staccato (nessun secondo runtime possibile).
    svc = TelegramService(
        settings_service=_Settings(_Cfg()),
        db=_one_active_bot_db(),
        bus=_Bus(),
        bot_transport_factory=lambda *a: _StuckTransport(),
    )
    svc.start()
    out = svc.stop()
    assert out["stopped"] is False
    assert svc.listener is not None  # fail-closed: non staccato
    assert svc.state == "FAILED"
