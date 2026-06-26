"""2.1-C: copy-pattern con action CASHOUT/CASHOUT_ALL emette il signal_type cashout."""

from telegram_listener import TelegramListener


class _FakeDB:
    def __init__(self, patterns):
        self._patterns = patterns

    def get_signal_patterns(self, enabled_only=False):
        return [p for p in self._patterns if (not enabled_only or p.get("enabled", True))]


def _listener(patterns):
    lst = object.__new__(TelegramListener)
    lst.db = _FakeDB(patterns)
    return lst


def _pat(**over):
    p = {"id": 1, "label": "L", "pattern": "GOAL", "enabled": True, "action": "QUICK_BET"}
    p.update(over)
    return p


def test_cashout_action_emits_cashout_signal_type():
    lst = _listener([_pat(action="CASHOUT", pattern="CASHOUT NOW")])
    out = lst._parse_custom_patterns("CASHOUT NOW 🆚 Team A v Team B")
    assert out is not None
    assert out["signal_type"] == "CASHOUT"
    assert out["event_name"]  # restringe il cashout alla partita
    assert "bet_type" not in out  # non è un descrittore di bet


def test_cashout_all_action_emits_cashout_all():
    lst = _listener([_pat(action="CASHOUT_ALL", pattern="CLOSE ALL")])
    out = lst._parse_custom_patterns("CLOSE ALL")
    assert out is not None
    assert out["signal_type"] == "CASHOUT_ALL"
    assert "bet_type" not in out


def test_quick_bet_action_returns_bet_descriptor():
    lst = _listener([_pat(action="QUICK_BET", pattern="GOAL")])
    out = lst._parse_custom_patterns("GOAL Team A")
    assert out is not None
    assert "signal_type" not in out  # percorso bet normale, non cashout
    assert "bet_type" in out


def test_absent_action_defaults_to_bet_descriptor():
    p = _pat(pattern="GOAL")
    p.pop("action")
    lst = _listener([p])
    out = lst._parse_custom_patterns("GOAL Team A")
    assert out is not None
    assert "signal_type" not in out
