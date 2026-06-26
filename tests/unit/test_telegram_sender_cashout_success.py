"""Residuo 2.1-A: sim-broadcast guard in telegram_sender._on_cashout_success (B2.5)."""

from telegram_sender import TelegramSender


def _sender(default_chat_id="123"):
    s = object.__new__(TelegramSender)
    s.default_chat_id = default_chat_id
    s.sent = []
    s.queue_default_message = lambda text, message_type=None: s.sent.append((message_type, text))
    s._escape = lambda x: str(x)
    return s


def test_cashout_success_broadcasts_when_live():
    s = _sender()
    s._on_cashout_success({"green_up": 2.5, "status": "DONE", "sim": False})
    assert s.sent and s.sent[0][0] == "MASTER_CASHOUT"


def test_cashout_success_suppressed_in_simulation():
    s = _sender()
    s._on_cashout_success({"green_up": 2.5, "status": "DONE", "sim": True})
    assert s.sent == []  # nessun broadcast ai follower per un cashout simulato


def test_cashout_success_default_no_sim_broadcasts():
    s = _sender()
    s._on_cashout_success({"green_up": 2.5, "status": "DONE"})  # sim assente => live
    assert s.sent and s.sent[0][0] == "MASTER_CASHOUT"


def test_cashout_success_no_chat_id_no_broadcast():
    s = _sender(default_chat_id=None)
    s._on_cashout_success({"green_up": 2.5, "status": "DONE", "sim": False})
    assert s.sent == []
