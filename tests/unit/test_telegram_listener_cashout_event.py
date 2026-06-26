"""Unit test per l'enrich event_name nel parse del cashout (Fase 2.1-B2.3)."""

from telegram_listener import TelegramListener


def _listener():
    return TelegramListener(api_id=1, api_hash="x")


def test_cashout_single_carries_event_name():
    out = _listener()._parse_cashout_signal("CASHOUT 🆚 Inter vs Milan")
    assert out is not None
    assert out["signal_type"] == "CASHOUT"
    assert out["event_name"] == "Inter vs Milan"


def test_cashout_single_event_name_from_vs_line():
    out = _listener()._parse_cashout_signal("Roma vs Lazio\ncashout adesso")
    assert out["signal_type"] == "CASHOUT"
    assert out["event_name"] == "Roma vs Lazio"


def test_cashout_all_also_carries_event_name_key():
    out = _listener()._parse_cashout_signal("CASHOUT ALL")
    assert out["signal_type"] == "CASHOUT_ALL"
    # Senza nome partita estraibile => stringa vuota (il CASHOUT_ALL la ignora).
    assert out["event_name"] == ""


def test_cashout_single_without_event_name_is_empty_string():
    # Fail-closed a valle: senza event_name il router salta il CASHOUT singolo.
    out = _listener()._parse_cashout_signal("cashout")
    assert out["signal_type"] == "CASHOUT"
    assert out["event_name"] == ""


def test_non_cashout_text_returns_none():
    assert _listener()._parse_cashout_signal("Punta casa @2.10") is None
