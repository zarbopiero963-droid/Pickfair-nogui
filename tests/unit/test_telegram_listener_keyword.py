from __future__ import annotations

from telegram_listener import TelegramListener


class _FakeDb:
    def __init__(self, patterns):
        self._patterns = patterns

    def get_signal_patterns(self):
        return self._patterns


def _listener_with(patterns):
    listener = TelegramListener(api_id=1, api_hash="x", db=_FakeDb(patterns))
    return listener


_BASE = {
    "id": 1,
    "name": "kw-rule",
    "enabled": True,
    "market_type": "MATCH_ODDS",
    "bet_side": "BACK",
    "selection_template": "Home",
}


def test_keyword_only_matches_when_keyword_present():
    listener = _listener_with([{**_BASE, "pattern": "", "keyword": "punta casa"}])
    signal = listener.parse_signal("Segnale: PUNTA CASA adesso @2.10")
    assert signal is not None
    assert signal.get("bet_type", signal.get("action", "BACK")).upper() == "BACK"


def test_keyword_only_blocks_when_keyword_absent():
    listener = _listener_with([{**_BASE, "pattern": "", "keyword": "punta casa"}])
    assert listener.parse_signal("Messaggio qualunque senza la frase @2.10") is None


def test_keyword_and_regex_both_required():
    rule = {**_BASE, "pattern": r"@\d", "keyword": "punta casa"}
    listener = _listener_with([rule])
    # Solo regex, manca keyword => blocca
    assert listener.parse_signal("quota @2.10 ma frase sbagliata") is None
    # Solo keyword, manca regex match => blocca
    assert listener.parse_signal("punta casa ma niente quota") is None
    # Entrambi presenti => match
    assert listener.parse_signal("punta casa @2.10") is not None


def test_rule_without_regex_and_keyword_is_skipped():
    listener = _listener_with([{**_BASE, "pattern": "", "keyword": ""}])
    assert listener.parse_signal("punta casa @2.10") is None
