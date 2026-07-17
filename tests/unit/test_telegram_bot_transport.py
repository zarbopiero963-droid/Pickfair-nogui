"""Test hard del trasporto Telegram Bot API (PR-1, epica #374).

Coprono: consegna messaggi (message/channel_post/caption), conversione data,
avanzamento dell'offset, fail-open su update malformato (senza incastrare il
loop), allow-list chat, anti-replay fail-closed su data mancante/rotta,
isolamento delle eccezioni di callback, redazione del bot_token nei log,
lifecycle start/stop.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

import pytest

from telegram_bot_transport import BotApiPollError, TelegramBotApiTransport

TOKEN = "123456:SECRETBOTTOKENVALUE"
CHAT = -1001234567890
_DATE = 1_700_000_000  # unix, deterministico


def _msg_update(update_id, *, chat_id=CHAT, text: str | None = "segnale", date=_DATE, kind="message"):
    inner = {"message_id": update_id, "date": date, "chat": {"id": chat_id, "type": "channel"}}
    if text is not None:
        inner["text"] = text
    return {"update_id": update_id, kind: inner}


def _payload(updates, ok=True):
    return {"ok": ok, "result": list(updates)}


def _transport(on_message, chat_ids=(CHAT,), fetch=None):
    return TelegramBotApiTransport(TOKEN, chat_ids, on_message, fetch=fetch or (lambda o: _payload([])))


# ---------------------------------------------------------------------------
# Consegna + conversione data
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_message_update_delivered_with_tzaware_date():
    got = []
    t = _transport(lambda *a: got.append(a), fetch=lambda o: _payload([_msg_update(10)]))
    new_offset = t.poll_once(0)

    assert got, "il messaggio valido non è stato consegnato"
    text, chat_id, message_date = got[-1]
    assert text == "segnale"
    assert chat_id == CHAT
    # Data tz-aware convertita correttamente (contratto handle_incoming anti-stale).
    assert message_date == datetime.fromtimestamp(_DATE, tz=timezone.utc)
    assert message_date.tzinfo is not None
    assert new_offset == 11  # max(update_id)+1


@pytest.mark.unit
def test_channel_post_delivered():
    got = []
    t = _transport(lambda *a: got.append(a), fetch=lambda o: _payload([_msg_update(3, kind="channel_post")]))
    t.poll_once(0)
    assert got and got[0][0] == "segnale"


@pytest.mark.unit
def test_caption_used_when_no_text():
    upd = _msg_update(4, text=None)
    upd["message"]["caption"] = "segnale-da-caption"
    got = []
    t = _transport(lambda *a: got.append(a), fetch=lambda o: _payload([upd]))
    t.poll_once(0)
    assert got and got[0][0] == "segnale-da-caption"


# ---------------------------------------------------------------------------
# Offset / fail-open (BLOCK: un update rotto non deve incastrare il loop)
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_offset_advances_past_all_updates():
    got = []
    t = _transport(lambda *a: got.append(a),
                   fetch=lambda o: _payload([_msg_update(10), _msg_update(11)]))
    assert t.poll_once(0) == 12
    assert len(got) == 2


@pytest.mark.unit
def test_malformed_update_skipped_but_offset_advances():
    got = []
    updates = [
        {"update_id": 5},                                  # nessun message
        {"update_id": 6, "message": {"date": _DATE}},      # niente chat/text
        _msg_update(7),                                    # valido
    ]
    t = _transport(lambda *a: got.append(a), fetch=lambda o: _payload(updates))
    new_offset = t.poll_once(0)
    # Solo il valido consegnato, ma l'offset supera TUTTI gli update consumati.
    assert len(got) == 1 and got[0][1] == CHAT
    assert new_offset == 8


@pytest.mark.unit
def test_batch_without_update_id_raises_anti_wedge():
    # Batch NON vuoto ma senza ALCUN update_id valido: non c'e' nulla da ack,
    # l'offset non puo' avanzare. Deve sollevare (=> run() fa backoff) invece di
    # ritornare lo stesso offset e reincastrarsi in hot-loop.
    updates = [{"message": {"text": "x", "date": _DATE, "chat": {"id": CHAT}}}]  # niente update_id
    t = _transport(lambda *a: None, fetch=lambda o: _payload(updates))
    with pytest.raises(BotApiPollError):
        t.poll_once(0)


@pytest.mark.unit
def test_getupdates_not_ok_raises_for_backoff():
    # ok=False solleva (cosi' run() applica il backoff, niente hot-loop); l'offset
    # non viene consumato perche' poll_once non ritorna.
    t = _transport(lambda *a: None, fetch=lambda o: _payload([_msg_update(9)], ok=False))
    with pytest.raises(BotApiPollError):
        t.poll_once(3)


# ---------------------------------------------------------------------------
# Sicurezza / fail-closed
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_non_monitored_chat_ignored():
    got = []
    t = _transport(lambda *a: got.append(a),
                   fetch=lambda o: _payload([_msg_update(1, chat_id=-999)]))
    new_offset = t.poll_once(0)
    assert not got           # allow-list: chat estranea scartata
    assert new_offset == 2   # ma consumata (offset avanza)


@pytest.mark.unit
@pytest.mark.parametrize("bad_date", [None, "oggi", float("inf")])
def test_missing_or_bad_date_skipped_failclosed(bad_date):
    upd = _msg_update(2, date=_DATE)
    if bad_date is None:
        del upd["message"]["date"]
    else:
        upd["message"]["date"] = bad_date
    got = []
    t = _transport(lambda *a: got.append(a), fetch=lambda o: _payload([upd]))
    new_offset = t.poll_once(0)
    assert not got           # anti-replay: senza data valida non si consegna
    assert new_offset == 3   # comunque consumato


@pytest.mark.unit
def test_callback_exception_is_isolated():
    def boom(*_a):
        raise RuntimeError("callback rotta")
    t = _transport(boom, fetch=lambda o: _payload([_msg_update(30)]))
    # L'eccezione della callback non deve propagare né bloccare l'avanzamento.
    assert t.poll_once(0) == 31


@pytest.mark.unit
def test_bot_token_never_in_error_or_logs(caplog):
    t = _transport(lambda *a: None,
                   fetch=lambda o: {"ok": False, "description": f"Unauthorized for bot {TOKEN}"})
    with caplog.at_level(logging.DEBUG, logger="telegram_bot_transport"), \
            pytest.raises(BotApiPollError) as ei:
        t.poll_once(0)
    msg = str(ei.value)
    assert TOKEN not in msg
    assert "[REDACTED]" in msg
    blob = "\n".join(r.getMessage() for r in caplog.records)
    assert TOKEN not in blob


@pytest.mark.unit
def test_redact_helper():
    t = _transport(lambda *a: None)
    redacted = t._redact(f"GET https://api.telegram.org/bot{TOKEN}/getUpdates")
    assert TOKEN not in redacted
    assert "[REDACTED]" in redacted


@pytest.mark.unit
def test_missing_token_rejected():
    with pytest.raises(ValueError):
        TelegramBotApiTransport("", [CHAT], lambda *a: None)


@pytest.mark.unit
def test_empty_chat_ids_rejected():
    # Allow-list obbligatoria e non vuota (fail-closed): niente config = niente
    # consegna, non "consegna tutto".
    with pytest.raises(ValueError):
        TelegramBotApiTransport(TOKEN, [], lambda *a: None)


@pytest.mark.unit
@pytest.mark.parametrize(
    "bad_base",
    ["ftp://x", "file:///etc/passwd", "api.telegram.org", "http://api.telegram.org"],
)
def test_invalid_api_base_rejected(bad_base):
    # http:// e' RIFIUTATO di default: il bot_token viaggia nell'URL getUpdates
    # e su http andrebbe in chiaro (MITM -> update falsi nel trading).
    with pytest.raises(ValueError):
        TelegramBotApiTransport(TOKEN, [CHAT], lambda *a: None, api_base=bad_base)


@pytest.mark.unit
def test_http_allowed_only_with_explicit_insecure_override():
    # http:// e' ammesso SOLO col flag esplicito e isolato (test locali).
    t = TelegramBotApiTransport(
        TOKEN, [CHAT], lambda *a: None,
        api_base="http://localhost:8081", allow_insecure_http=True,
    )
    assert t._api_base == "http://localhost:8081"


@pytest.mark.unit
def test_dispatch_failclosed_if_allowlist_emptied_at_runtime():
    got = []
    t = _transport(lambda *a: got.append(a), fetch=lambda o: _payload([_msg_update(1)]))
    t.chat_ids = set()  # svuotata a runtime -> difesa: scarta tutto
    t.poll_once(0)
    assert not got


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_run_survives_pollerror_with_backoff():
    # BLOCK: un ok=False persistente NON deve uccidere il thread ne' degenerare in
    # hot-loop. run() cattura BotApiPollError, applica backoff e continua il loop.
    calls = []

    def fetch(offset):
        calls.append(offset)
        return _payload([_msg_update(1)], ok=False)  # sempre non-ok => BotApiPollError

    t = TelegramBotApiTransport(
        TOKEN, [CHAT], lambda *a: None, fetch=fetch,
        base_backoff_sec=0.01, max_backoff_sec=0.02,
    )
    t.start()
    time.sleep(0.1)
    alive_during = t._thread is not None and t._thread.is_alive()
    t.stop(timeout=2.0)
    # Il thread e' rimasto vivo nonostante l'errore ripetuto, ha ripollato (backoff)
    # e si e' fermato in modo pulito su stop().
    assert alive_during, "il thread non deve morire su BotApiPollError"
    assert len(calls) >= 2, "il loop deve ripollare dopo il backoff, non uscire"
    assert t._thread is not None and not t._thread.is_alive()


@pytest.mark.unit
def test_start_stop_halts_loop():
    calls = []
    t = _transport(lambda *a: None)

    def fetch(offset):
        calls.append(offset)
        # Simula il long-poll: si sblocca quando stop() setta l'evento.
        t._stop.wait(0.05)
        return _payload([])

    t._fetch = fetch  # type: ignore[assignment]
    t.start()
    time.sleep(0.02)
    t.stop(timeout=2.0)
    assert t._thread is not None and not t._thread.is_alive()
    assert len(calls) >= 1
