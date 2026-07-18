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

import threading
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


class _FakeThread:
    """Thread fittizio: `is_alive()` riflette il flag di stop del transport, così
    `_transport_alive()` esercita il path del thread REALE (`_thread.is_alive()`),
    non il fallback sul flag `stopped` — come in produzione (il transport reale
    espone sempre un `_thread`)."""

    def __init__(self, alive_fn):
        self._alive_fn = alive_fn

    def is_alive(self):
        return bool(self._alive_fn())


class _FakeTransport:
    """Transport fake: cattura on_message; `_thread.is_alive()` segue `stopped`."""

    def __init__(self, bot_token, chat_ids, on_message):
        self.bot_token = bot_token
        self.chat_ids = chat_ids
        self.on_message = on_message
        self.started = False
        self.stopped = False
        self._consecutive_failures = 0
        self._thread = _FakeThread(lambda: not self.stopped)

    def start(self):
        self.started = True

    def stop(self, timeout=5.0):
        self.stopped = True

    def health_snapshot(self):
        # Esercita il path ATOMICO dell'adapter (come il transport reale).
        alive = bool(self._thread is not None and self._thread.is_alive())
        return alive, int(self._consecutive_failures)


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


def test_botapi_selection_failclosed_on_chat_read_error():
    # BLOCK (Fugu full-range): un errore DB leggendo le chat di un bot ATTIVO NON
    # deve droppare silenziosamente la sorgente (rischio: avviare un set parziale
    # bypassando il fail-closed multi-bot). start() fa fail-closed.
    class _RaisingChatsDB(_BotDB):
        def get_telegram_bot_chats(self, bot_id):
            raise RuntimeError("db chats error")

    db = _RaisingChatsDB(
        bots=[{"id": 1, "label": "A", "bot_token": "tok-a", "is_active": True, "has_token": True}],
    )
    svc = _svc(db, capture=[])
    with pytest.raises(RuntimeError) as exc:
        svc.start()
    assert "config_read_error" in str(exc.value)
    assert svc.state == "FAILED"


def test_botapi_selection_failclosed_on_bots_list_error():
    # BLOCK (rilievo GPT-5.6 Terra + Fable full-range): un errore DB nell'ELENCARE i
    # bot NON deve essere degradato a "0 bot attivi" (che, con un bot usable letto
    # separatamente, riavvierebbe un singolo bot droppando il 2° attivo). Con la
    # lettura UNICA di _select_bot_api_source() l'errore PROPAGA => start() fa
    # fail-closed 'telegram_bot_config_read_error', non un avvio silenzioso.
    class _RaisingBotsDB(_BotDB):
        def get_telegram_bots(self, include_token=True):
            raise RuntimeError("db bots error")

    svc = _svc(_RaisingBotsDB(), capture=[])
    with pytest.raises(RuntimeError) as exc:
        svc.start()
    assert "config_read_error" in str(exc.value)
    assert svc.state == "FAILED"


def test_start_failure_raising_resets_handlers_registered():
    # BLOCK (Fable full-range): handlers_registered=1 è impostato PRIMA di
    # listener.start(). Se start() SOLLEVA, l'except deve azzerare
    # handlers_registered: un runtime FAILED con listener=None NON deve riportare
    # handler registrati (snapshot incoerente per invariant guard/monitoring).
    svc = _svc(_one_active_bot_db(), capture=[])

    class _RaisingListener:
        _runtime_thread = None

        def start(self):
            raise RuntimeError("boom in start")

        def status(self):  # pragma: no cover - non raggiunto (start solleva prima)
            return {}

    # Sostituisce la costruzione dell'adapter con un listener che solleva su start().
    svc._build_botapi_runtime = lambda *a, **k: _RaisingListener()  # type: ignore[assignment]
    with pytest.raises(RuntimeError):
        svc.start()
    assert svc.state == "FAILED"
    assert svc.handlers_registered == 0


def test_start_failure_keeps_listener_if_runtime_thread_survives():
    # BLOCK (GPT-5.6 Terra): se listener.start() avvia PARZIALMENTE un thread e poi
    # solleva, l'orfano NON va nascosto azzerando il riferimento: il service tenta
    # lo stop e, se il thread SOPRAVVIVE, TIENE il listener così il guard
    # previous_runtime_still_alive lo vede e un retry NON crea un secondo runtime
    # (niente doppio getUpdates/409, niente segnali di betting duplicati).
    svc = _svc(_one_active_bot_db(), capture=[])

    class _AliveThread:
        def is_alive(self):
            return True

    class _PartialStartListener:
        _runtime_thread = _AliveThread()

        def __init__(self):
            self.stop_called = False

        def start(self):
            raise RuntimeError("boom dopo partial start")

        def stop(self):
            self.stop_called = True  # stop tentato ma il thread resta vivo

        def status(self):
            return {}

    holder = {}

    def _build(*a, **k):
        lst = _PartialStartListener()
        holder["lst"] = lst
        return lst

    svc._build_botapi_runtime = _build  # type: ignore[assignment]

    with pytest.raises(RuntimeError):
        svc.start()
    assert holder["lst"].stop_called is True     # stop tentato sul path di errore
    assert svc.listener is holder["lst"]         # riferimento TENUTO (residuo non nascosto)
    assert svc.handlers_registered == 1          # handler non azzerati: c'è un thread vivo
    assert svc.state == "FAILED"

    # Retry: il guard vede il thread vivo e blocca (nessun secondo listener).
    r = svc.start()
    assert r.get("reason") == "previous_runtime_still_alive"
    assert r.get("started") is False


def test_start_failure_failclosed_when_liveness_check_raises():
    # BLOCK (GPT-5.6 Terra): se la verifica di liveness del thread residuo SOLLEVA,
    # il fallback deve essere FAIL-CLOSED (assumi il thread VIVO): si TIENE il
    # listener e il retry resta bloccato. Un fail-open (assumere morto) azzererebbe
    # il riferimento e riaprirebbe la duplicazione dei segnali di betting.
    svc = _svc(_one_active_bot_db(), capture=[])

    class _ExplodingThread:
        def is_alive(self):
            raise RuntimeError("is_alive boom")

    class _PartialStartListener:
        _runtime_thread = _ExplodingThread()

        def start(self):
            raise RuntimeError("boom dopo partial start")

        def stop(self):
            pass

        def status(self):
            return {}

    holder = {}

    def _build(*a, **k):
        lst = _PartialStartListener()
        holder["lst"] = lst
        return lst

    svc._build_botapi_runtime = _build  # type: ignore[assignment]

    with pytest.raises(RuntimeError):
        svc.start()
    # Liveness incerta (is_alive solleva) => fail-closed: listener TENUTO, handler non azzerati.
    assert svc.listener is holder["lst"]
    assert svc.handlers_registered == 1
    assert svc.state == "FAILED"


def test_start_recovers_when_cached_state_is_stale_connected():
    # BLOCK (Fugu full-range): self.state cache "CONNECTED" stale dopo la morte del
    # transport NON deve far tornare already_running (bloccando il recovery). Il
    # refresh pre-idempotenza riallinea allo stato reale (FAILED) e start procede.
    cap = []
    svc = _svc(_one_active_bot_db(), capture=cap)
    svc.start()
    assert svc.state == "CONNECTED"

    class _Dead:
        def is_alive(self):
            return False

    cap[0]._thread = _Dead()   # transport morto
    svc.state = "CONNECTED"    # cache stale (nessuno ha ancora rinfrescato)

    r = svc.start()            # deve RECUPERARE, non tornare already_running
    assert r.get("reason") != "already_running"


def test_multi_active_bot_starts_orchestrator_and_both_deliver():
    # BLOCK PR-5b: 2 bot attivi usable => orchestratore N-bot AVVIATO (non più
    # fail-closed 'multi_bot_runtime_not_yet_supported'); ogni bot consegna al bus
    # via il proprio transport/sink; handlers_registered aggregato == 2 e
    # l'invariant guard è soddisfatto (CONNECTED => handlers == expected == 2).
    db = _BotDB(
        bots=[
            {"id": 1, "label": "A", "bot_token": "tok-a", "is_active": True, "has_token": True},
            {"id": 2, "label": "B", "bot_token": "tok-b", "is_active": True, "has_token": True},
        ],
        chats_by_bot={
            1: [{"chat_id": "-100111", "is_active": True}],
            2: [{"chat_id": "-100222", "is_active": True}],
        },
        patterns=_next_gol_pattern(),
    )
    cap = []
    svc = _svc(db, capture=cap)
    out = svc.start()
    assert out["started"] is True
    assert svc.state == "CONNECTED"
    assert len(cap) == 2  # un transport per bot
    st = svc.status()
    assert st["handlers_registered"] == 2
    assert st["unusable_active_bot_count"] == 0

    fresh = datetime.now(timezone.utc) - timedelta(seconds=5)
    cap[0].on_message(MSG_NEXT_GOL, -100111, fresh)   # bot A -> sua chat
    cap[1].on_message(MSG_NEXT_GOL, -100222, fresh)   # bot B -> sua chat
    payloads = [p for t, p in svc.bus.events if t == "SIGNAL_RECEIVED"]
    assert len(payloads) == 2  # entrambi i bot hanno consegnato

    # Invariant guard generalizzato soddisfatto con N handler == expected (dopo la
    # prima consegna last_successful_message_ts è valorizzato => niente STALE).
    assert svc.health_status()["invariant_ok"] is True


def test_handle_signal_serializes_fanin_across_threads():
    # BLOCK PR-5b: con N bot, thread transport distinti chiamano _handle_signal in
    # parallelo. La SEZIONE CRITICA (update monotono di last_successful_message_ts +
    # save_received_signal) gira SOTTO _signal_fanin_lock: un ALTRO thread NON deve
    # poter acquisire il lock mentre la sezione critica è in corso (serializzazione
    # cross-thread). NB: bus.publish è ora FUORI dal lock (evita lock-order inversion),
    # quindi la prova sonda il lock durante save_received_signal, non durante publish.
    probe = {"acquired_by_other_thread": None, "saved_under_lock": False}
    svc_holder = {}

    class _ProbeDB(_BotDB):
        def save_received_signal(self, payload):
            probe["saved_under_lock"] = True
            svc = svc_holder["svc"]
            result = {}

            def _try():
                got = svc._signal_fanin_lock.acquire(blocking=False)
                result["got"] = got
                if got:
                    svc._signal_fanin_lock.release()

            th = threading.Thread(target=_try)  # thread DIVERSO => non rientrante
            th.start()
            th.join()
            probe["acquired_by_other_thread"] = result.get("got")
            super().save_received_signal(payload)

    svc = _svc(_ProbeDB(), capture=None)
    svc_holder["svc"] = svc
    svc._handle_signal({"market_type": "X"})
    assert probe["saved_under_lock"] is True
    # Lock TENUTO durante la sezione critica: l'altro thread non ha potuto acquisirlo.
    assert probe["acquired_by_other_thread"] is False


def test_fanin_lock_reentrant_no_deadlock_on_sync_resubscribe():
    # BLOCK (Fable full-range): _signal_fanin_lock resta RLock per difesa. La sezione
    # critica (save_received_signal) gira SOTTO il lock; se un hook del DB rientra
    # sincrono in _handle_signal dallo STESSO thread, un Lock non-rientrante
    # deadlockerebbe per sempre. L'RLock consente il rientro same-thread e completa.
    reentry = {"count": 0}
    svc_holder = {}

    class _ReentrantDB(_BotDB):
        def save_received_signal(self, payload):
            super().save_received_signal(payload)
            if reentry["count"] == 0:
                reentry["count"] += 1
                # rientro sincrono, STESSO thread, mentre il lock è ancora tenuto
                svc_holder["svc"]._handle_signal({"market_type": "REENTRY"})

    svc = _svc(_ReentrantDB(), capture=None)
    svc_holder["svc"] = svc
    done = threading.Event()

    def _run():
        svc._handle_signal({"market_type": "X"})
        done.set()

    threading.Thread(target=_run, daemon=True).start()
    # RLock rientrante => completa; Lock non-rientrante => deadlock => timeout.
    assert done.wait(timeout=3.0), "deadlock fan-in: lock non rientrante"
    assert reentry["count"] == 1
    assert len(svc.db.saved) == 2  # segnale originale + rientro


def test_last_message_ts_is_last_write_wins_and_never_blocks_on_bad_ts():
    # BLOCK (rilievo convergente Fugu Ultra / Fable 5 + regressione CI su
    # test_handle_signal_preserves_listener_received_at): last_successful_message_ts
    # è LAST-WRITE-WINS (riflette la ricezione dell'ultimo messaggio, contratto
    # storico). Il confronto stringa "monotòno" (Greptile P1) era fragile e va evitato:
    # (a) un received_at esplicito anche BACKDATED viene preservato;
    # (b) un received_at malformato / offset misto NON deve sollevare né bloccare
    #     save/publish. Sul vecchio codice monotòno (a) regrediva e (b) rischiava
    #     TypeError/blocco della sezione critica.
    svc = _svc(_BotDB(), capture=None)
    svc._handle_signal({"market_type": "A", "received_at": "2026-07-18T12:00:05+00:00"})
    # (a) un segnale successivo con received_at BACKDATED viene preservato (LWW)
    svc._handle_signal({"market_type": "B", "received_at": "2026-04-15T00:00:00+00:00"})
    assert svc.last_successful_message_ts == "2026-04-15T00:00:00+00:00"
    # (b) received_at con offset non-UTC / formato diverso: nessuna eccezione, save+publish avvengono
    svc._handle_signal({"market_type": "C", "received_at": "2026-07-18T14:00:00+05:00"})
    assert svc.last_successful_message_ts == "2026-07-18T14:00:00+05:00"
    published = [p for t, p in svc.bus.events if t == "SIGNAL_RECEIVED"]
    assert len(published) == 3  # tutti pubblicati, nessun blocco


def test_second_active_unusable_bot_surfaced_not_blocking():
    # BLOCK PR-5b: un 2° bot ATTIVO ma non-usable (bot B: sole chat non numeriche
    # `@canale`, risoluzione @username rimandata) NON blocca né viene droppato in
    # silenzio: il bot usable (A) parte e l'unusable è SURFACED in status
    # (unusable_active_bot_count=1). Con 1 solo usable => runtime singolo (handlers=1).
    db = _BotDB(
        bots=[
            {"id": 1, "label": "A", "bot_token": "tok-a", "is_active": True, "has_token": True},
            {"id": 2, "label": "B", "bot_token": "tok-b", "is_active": True, "has_token": True},
        ],
        chats_by_bot={
            1: [{"chat_id": "-100111", "is_active": True}],   # usable
            2: [{"chat_id": "@canale", "is_active": True}],   # attivo ma NON usable
        },
    )
    cap = []
    svc = _svc(db, capture=cap)
    out = svc.start()
    assert out["started"] is True
    assert svc.state == "CONNECTED"
    assert len(cap) == 1  # solo il bot usable ha un transport
    st = svc.status()
    assert st["handlers_registered"] == 1
    assert st["unusable_active_bot_count"] == 1


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
    """Transport fake che NON si ferma: `stopped` resta False => `_thread.is_alive()`
    resta True (thread genuinamente 'vivo')."""

    def __init__(self, *a):
        self.stopped = False
        self._thread = _FakeThread(lambda: not self.stopped)

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


def test_already_running_botapi_not_reevaluated_on_config_change():
    # BLOCK (Fugu/Fable full-range): un servizio GIÀ attivo via Bot API NON deve
    # rivalutare il gate. Se la config cambia (2° bot attivato) una start()
    # ridondante NON deve sollevare/andare FAILED: ritorna already_running.
    db = _one_active_bot_db()
    svc = _svc(db, capture=[])
    r1 = svc.start()
    assert r1["started"] is True and svc.state == "CONNECTED"

    # "si attiva un secondo bot" nel DB (chat NUMERICA => usable)
    db._bots.append({"id": 99, "label": "B2", "bot_token": "tok2", "is_active": True, "has_token": True})
    db._chats[99] = [{"chat_id": "-100888", "is_active": True}]

    r2 = svc.start()  # sul vecchio ordine: rivalutava il gate -> RuntimeError
    assert r2["reason"] == "already_running"
    assert svc.state == "CONNECTED"
    assert r2["chat_count"] == 1  # conteggio dal listener in esecuzione


def test_botapi_persistent_poll_failure_degrades_to_failed():
    # BLOCK (Fugu full-range): un transport con thread VIVO ma getUpdates in
    # fallimento permanente (401/409) non deve restare CONNECTED (fail-open):
    # oltre la soglia lo stato degrada a FAILED con handler=0 e last_error, così
    # l'invariant guard/autoheal rilevano l'ingestione morta.
    cap = []
    svc = _svc(_one_active_bot_db(), capture=cap)
    svc.start()
    assert svc.runtime_snapshot()["state"] == "CONNECTED"

    # simula backoff permanente: molti fallimenti consecutivi, thread vivo
    cap[0]._consecutive_failures = 5
    snap = svc.runtime_snapshot()
    assert snap["state"] == "FAILED"
    assert snap["handlers_registered"] == 0
    st = svc.listener.status()
    assert st["last_error"] == "bot_transport_persistent_poll_failure"

    # un poll riuscito azzera il contatore -> torna CONNECTED
    cap[0]._consecutive_failures = 0
    assert svc.runtime_snapshot()["state"] == "CONNECTED"


def test_botapi_start_does_not_overwrite_live_orphan_transport():
    # BLOCK (Fable full-range): dopo uno start fallito con thread VIVO (transport
    # orfano mantenuto), un retry di start() NON deve costruire un SECONDO
    # transport sullo stesso token (409). Fail-closed finché non si fa stop().
    built = []

    class _StuckExplodingTransport:
        def __init__(self):
            self.stopped = False
            self._thread = _FakeThread(lambda: not self.stopped)

        def start(self):
            raise RuntimeError("boom")  # start fallisce, ma il thread resta vivo

        def stop(self, timeout=5.0):
            pass

    def factory(*_a):
        t = _StuckExplodingTransport()
        built.append(t)
        return t

    rt = TelegramBotApiRuntime(bot_token="tok", chat_ids=[123], transport_factory=factory)
    out1 = rt.start()
    assert out1["started"] is False
    assert rt._transport is not None            # orfano mantenuto (thread vivo)
    assert len(built) == 1

    out2 = rt.start()                            # retry diretto sull'adapter
    assert out2["started"] is False
    assert "orphan" in out2["error"]
    assert len(built) == 1                       # NESSUN secondo transport costruito


def test_botapi_status_snapshot_is_internally_coherent():
    # BLOCK (Fugu full-range): lo snapshot di status() deve essere COERENTE —
    # state, handlers_registered e running derivano da UNA sola lettura di
    # liveness/fallimenti, così non escono mai valori incoerenti (es. state
    # FAILED con handlers=1). Copre i tre stati: sano, degradato, thread morto.
    cap = []
    svc = _svc(_one_active_bot_db(), capture=cap)
    svc.start()

    class _Dead:
        def is_alive(self):
            return False

    # sano: CONNECTED <=> 1 handler
    s = svc.listener.status()
    assert s["state"] == "CONNECTED" and s["handlers_registered"] == 1 and s["running"] is True

    # degradato (fallimento permanente, thread vivo): FAILED + 0 handler
    cap[0]._consecutive_failures = 5
    s = svc.listener.status()
    assert s["state"] == "FAILED" and s["handlers_registered"] == 0

    # thread morto: FAILED + 0 handler + running False
    cap[0]._consecutive_failures = 0
    cap[0]._thread = _Dead()
    s = svc.listener.status()
    assert s["state"] == "FAILED" and s["handlers_registered"] == 0 and s["running"] is False


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
            self._thread = _FakeThread(lambda: not self.stopped)

        def start(self):
            raise RuntimeError("boom dopo partial start")

        def stop(self, timeout=5.0):
            self.stopped = True  # stop efficace -> thread morto

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
            self.stopped = False  # stop() non ferma -> _thread.is_alive() resta True
            self._thread = _FakeThread(lambda: not self.stopped)

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
