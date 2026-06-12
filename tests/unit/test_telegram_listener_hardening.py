"""PR-G programma test hedge-fund grade: hardening ingresso Telegram.

Due gap reali di produzione provati da questa suite (fix autorizzato
dall'owner su telegram_listener.py):

1. STALE: il listener non guardava MAI la data del messaggio
   (event.message.date) — timbrava tutto con l'ora di ricezione. Un
   backlog consegnato da Telethon dopo un reconnect (es. "NEXT GOL" di
   30 minuti prima) generava una bet su una situazione di gioco che non
   esiste piu'. Contratto nuovo: messaggi piu' vecchi di
   max_message_age_seconds (default 300) vengono scartati; la liveness
   del canale si aggiorna comunque (il canale e' vivo, il segnale no).

2. CHAT NON AUTORIZZATA: l'unica barriera era il filtro Telethon
   events.NewMessage(chats=...) alla registrazione; handle_incoming
   accettava qualsiasi chat_id. Contratto nuovo: difesa in profondita',
   chat_id presente e fuori da monitored_chats => scarto totale
   (niente liveness, niente callback, niente segnale).

Dedup verificato: filtro Telethon registrato, parsing keyword/righe
statistiche, full chain e dedup-2-messaggi sono GIA' coperti
(test_telegram_listener_runtime, test_telegram_listener_keyword,
test_e2e_telegram_full_chain); qui solo stale, bypass chat e segnali
concatenati. Il flood da 100 messaggi sta nell'E2E full chain.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from telegram_listener import TelegramListener

MSG_NEXT_GOL = """🆚Reading v Burton Albion
🏆English Sky Bet League 1
⌚ time, 11m, 0 - 0

🔥 P.Exc. NEXT GOL 🔊 ✅

📊88.33%"""

# Messaggio con DUE keyword valide in righe normali (non statistiche):
# il contratto del parser e' first-match-wins, UN solo segnale.
MSG_CONCATENATED = """🆚Reading v Burton Albion

🔥 P.Exc. NEXT GOL 🔊 ✅
🔥 P.Bet. 0,5 HT 🔊 ✅"""

AUTHORIZED_CHAT = -100999
UNAUTHORIZED_CHAT = -100500


class _Db:
    @staticmethod
    def get_signal_patterns(enabled_only=True):
        _ = enabled_only
        return [
            {
                "id": 1, "name": "next-gol", "enabled": True, "pattern": "",
                "keyword": "NEXT GOL", "market_type": "NEXT_GOAL",
                "bet_side": "BACK", "selection_template": "Next Goal",
                "mm_auto": True,
            },
            {
                "id": 2, "name": "05ht", "enabled": True, "pattern": "",
                "keyword": "0,5 HT", "market_type": "OVER_UNDER_HT_05",
                "bet_side": "BACK", "selection_template": "Over 0.5",
                "mm_auto": True,
            },
        ]


def _listener(**overrides):
    listener = TelegramListener(api_id=123, api_hash="hash", **overrides)
    listener.set_database(_Db())
    listener.monitored_chats = [AUTHORIZED_CHAT]
    signals = []
    messages = []
    listener.set_callbacks(
        on_signal=signals.append,
        on_message=messages.append,
    )
    return listener, signals, messages


def _utc_now():
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# GAP 1: messaggio STALE => nessun segnale (la liveness resta viva)
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_stale_message_is_discarded_without_signal():
    """Backlog post-reconnect: un NEXT GOL di un'ora fa NON deve
    generare ordini (la partita e' cambiata). Era: nessun controllo."""
    listener, signals, _messages = _listener()

    out = listener.handle_incoming(
        MSG_NEXT_GOL,
        chat_id=AUTHORIZED_CHAT,
        message_date=_utc_now() - timedelta(seconds=3600),
    )

    assert out is None
    assert signals == []
    # Il canale e' vivo: la liveness si aggiorna anche su messaggio stale
    # (altrimenti l'autoheal riavvierebbe un canale sano).
    assert listener.last_successful_message_ts is not None


@pytest.mark.unit
def test_fresh_message_with_date_emits_signal():
    """Controllo PASS: un messaggio recente (entro la soglia) passa."""
    listener, signals, _messages = _listener()

    out = listener.handle_incoming(
        MSG_NEXT_GOL,
        chat_id=AUTHORIZED_CHAT,
        message_date=_utc_now() - timedelta(seconds=5),
    )

    assert out is not None
    assert out["market_type"] == "NEXT_GOAL"
    assert len(signals) == 1


@pytest.mark.unit
def test_stale_threshold_is_configurable():
    """Soglia iniettabile: con max_message_age_seconds=60 un messaggio
    di 120s e' stale, uno di 10s passa."""
    listener, signals, _messages = _listener(max_message_age_seconds=60)

    stale = listener.handle_incoming(
        MSG_NEXT_GOL, chat_id=AUTHORIZED_CHAT,
        message_date=_utc_now() - timedelta(seconds=120),
    )
    fresh = listener.handle_incoming(
        MSG_NEXT_GOL, chat_id=AUTHORIZED_CHAT,
        message_date=_utc_now() - timedelta(seconds=10),
    )

    assert stale is None
    assert fresh is not None
    assert len(signals) == 1


@pytest.mark.unit
def test_naive_message_date_is_treated_as_utc():
    """Telethon consegna datetime aware-UTC, ma una data naive non deve
    esplodere ne' bypassare la guardia: viene assunta UTC."""
    listener, signals, _messages = _listener()

    out = listener.handle_incoming(
        MSG_NEXT_GOL,
        chat_id=AUTHORIZED_CHAT,
        message_date=datetime.utcnow() - timedelta(seconds=3600),
    )

    assert out is None
    assert signals == []


@pytest.mark.unit
def test_malformed_message_date_is_fail_closed():
    """Data non confrontabile: impossibile provare che il segnale sia
    fresco => scarto fail-closed, nessuna eccezione."""
    listener, signals, _messages = _listener()

    out = listener.handle_incoming(
        MSG_NEXT_GOL, chat_id=AUTHORIZED_CHAT, message_date="garbage",
    )

    assert out is None
    assert signals == []


@pytest.mark.unit
def test_future_dated_message_is_discarded_symmetric_guard():
    """Clock skew serio (data molto nel futuro) neutralizzerebbe il
    controllo stale (eta' negativa < soglia): oltre la stessa tolleranza
    il messaggio va scartato. Uno skew piccolo (secondi) invece passa."""
    listener, signals, _messages = _listener()

    far_future = listener.handle_incoming(
        MSG_NEXT_GOL, chat_id=AUTHORIZED_CHAT,
        message_date=_utc_now() + timedelta(seconds=3600),
    )
    small_skew = listener.handle_incoming(
        MSG_NEXT_GOL, chat_id=AUTHORIZED_CHAT,
        message_date=_utc_now() + timedelta(seconds=5),
    )

    assert far_future is None
    assert small_skew is not None
    assert len(signals) == 1


@pytest.mark.unit
@pytest.mark.parametrize("bad_threshold", [0, -5, -0.1])
def test_non_positive_max_age_is_rejected_at_construction(bad_threshold):
    """Soglia <= 0 renderebbe stale OGNI messaggio reale (listener muto
    senza errori): la misconfigurazione deve fallire subito, esplicita."""
    with pytest.raises(ValueError, match="max_message_age_seconds"):
        TelegramListener(
            api_id=123, api_hash="hash",
            max_message_age_seconds=bad_threshold,
        )


@pytest.mark.unit
def test_internal_call_without_date_keeps_working():
    """Controllo PASS (retrocompatibilita'): le chiamate interne/sim senza
    message_date non sono filtrate."""
    listener, signals, _messages = _listener()

    out = listener.handle_incoming(MSG_NEXT_GOL, chat_id=AUTHORIZED_CHAT)

    assert out is not None
    assert len(signals) == 1


@pytest.mark.unit
def test_event_handler_forwards_message_date_to_stale_guard():
    """Wiring: la data arriva dall'evento Telethon (event.message.date)
    fino alla guardia — un evento stale iniettato nel handler asincrono
    non produce segnale."""
    listener, signals, _messages = _listener()
    event = SimpleNamespace(
        raw_text=MSG_NEXT_GOL,
        chat_id=AUTHORIZED_CHAT,
        message=SimpleNamespace(date=_utc_now() - timedelta(seconds=3600)),
    )

    asyncio.run(listener._on_new_message_event(event))

    assert signals == []
    assert listener.last_successful_message_ts is not None


# ---------------------------------------------------------------------------
# GAP 2: chat non autorizzata => scarto totale (difesa in profondita')
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_unauthorized_chat_is_discarded_entirely():
    """Se un messaggio di una chat fuori lista bypassa il filtro Telethon,
    l'handler deve scartarlo: niente segnale, niente callback, niente
    liveness (non e' un messaggio del canale monitorato)."""
    listener, signals, messages = _listener()

    out = listener.handle_incoming(MSG_NEXT_GOL, chat_id=UNAUTHORIZED_CHAT)

    assert out is None
    assert signals == []
    assert messages == []
    assert listener.last_successful_message_ts is None


@pytest.mark.unit
def test_authorized_chat_passes():
    """Controllo PASS: la chat in lista continua a funzionare."""
    listener, signals, _messages = _listener()

    out = listener.handle_incoming(MSG_NEXT_GOL, chat_id=AUTHORIZED_CHAT)

    assert out is not None
    assert len(signals) == 1


@pytest.mark.unit
def test_internal_call_without_chat_id_keeps_working():
    """Controllo PASS (retrocompatibilita'): chat_id assente = chiamata
    interna/sim, nessun filtro chat (in runtime il preflight di start()
    garantisce monitored_chats non vuoto e il filtro Telethon a monte)."""
    listener, signals, _messages = _listener()

    out = listener.handle_incoming(MSG_NEXT_GOL)

    assert out is not None
    assert len(signals) == 1


# ---------------------------------------------------------------------------
# Segnali concatenati: first-match-wins, UN solo segnale
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_concatenated_signals_emit_exactly_one():
    """Messaggio con DUE keyword valide: il parser e' first-match-wins —
    esattamente UN segnale (il primo pattern), mai due ordini da un
    singolo messaggio."""
    listener, signals, _messages = _listener()

    out = listener.handle_incoming(MSG_CONCATENATED, chat_id=AUTHORIZED_CHAT)

    assert out is not None
    assert len(signals) == 1
    assert signals[0]["market_type"] == "NEXT_GOAL"
