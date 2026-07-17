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

from telegram_bot_transport import TelegramBotApiTransport

TOKEN = "123456:SECRETBOTTOKENVALUE"
CHAT = -1001234567890
_DATE = 1_700_000_000  # unix, deterministico


def _msg_update(update_id, *, chat_id=CHAT, text="segnale", date=_DATE, kind="message"):
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
def test_getupdates_not_ok_keeps_offset():
    got = []
    t = _transport(lambda *a: got.append(a), fetch=lambda o: _payload([_msg_update(9)], ok=False))
    assert t.poll_once(3) == 3   # offset invariato
    assert not got


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
def test_bot_token_never_in_logs(caplog):
    t = _transport(lambda *a: None,
                   fetch=lambda o: {"ok": False, "description": f"Unauthorized for bot {TOKEN}"})
    with caplog.at_level(logging.DEBUG, logger="telegram_bot_transport"):
        t.poll_once(0)
    blob = "\n".join(r.getMessage() for r in caplog.records)
    assert TOKEN not in blob
    assert "[REDACTED]" in blob


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


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------
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
