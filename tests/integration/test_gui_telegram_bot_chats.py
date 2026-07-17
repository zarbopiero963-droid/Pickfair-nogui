"""PR-4b (epica #374) — editor chat per-bot (Bot API) nella tab Telegram.

Assegna i `chat_id` a CIASCUN bot: le chat sono scoped al bot_id (tabella
`telegram_bot_chats`, CRUD PR-3 `set/get_telegram_bot_chats`). Espone i metodi
-logica sul mixin `TelegramModule`, testabili headless (senza display) col
pattern del tab bot: app in `test_mode=True` + `Database` REALE su file
temporaneo iniettato come `app.db`, si pilotano le tk-var e si chiamano i
metodi-logica, verificando la persistenza reale.

SOLO persistenza/GUI: nessun wiring runtime (le chat non sono ancora ascoltate).
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_mini_gui_integration import _install_mini_gui_fakes  # noqa: E402

from database import Database  # noqa: E402

pytestmark = pytest.mark.integration


def _make(monkeypatch, db):
    mg = _install_mini_gui_fakes(monkeypatch)
    app = mg.MiniPickfairGUI(test_mode=True)
    app.db = db  # inietta la persistenza REALE per il CRUD bot/chat
    return app


@pytest.fixture
def db(tmp_path):
    return Database(str(tmp_path / "t.db"))


def test_add_chat_to_selected_bot_persists_and_clears_entry(monkeypatch, db):
    app = _make(monkeypatch, db)
    try:
        bid = db.save_telegram_bot("B", "fake-bot-value-not-real-abc")
        app.tg_selected_bot_id = bid
        app.tg_bot_chat_id_var.set("-100123")
        app.tg_bot_chat_title_var.set("Segnali A")
        app._add_telegram_bot_chat_from_ui()

        chats = db.get_telegram_bot_chats(bid)
        assert len(chats) == 1
        assert chats[0]["chat_id"] == "-100123"
        assert chats[0]["title"] == "Segnali A"
        assert chats[0]["is_active"] is True
        # entry pulite dopo l'aggiunta
        assert app.tg_bot_chat_id_var.get() == ""
        assert app.tg_bot_chat_title_var.get() == ""
    finally:
        app.destroy()


def test_add_chat_blocked_without_selected_bot(monkeypatch, db):
    # BLOCK: senza bot selezionato non si aggiunge nulla (fail-closed), niente crash.
    app = _make(monkeypatch, db)
    try:
        bid = db.save_telegram_bot("B", "fake-bot-value-not-real-abc")
        app.tg_selected_bot_id = None
        app.tg_bot_chat_id_var.set("-100999")
        app._add_telegram_bot_chat_from_ui()
        assert db.get_telegram_bot_chats(bid) == []
    finally:
        app.destroy()


def test_add_chat_blocked_without_chat_id(monkeypatch, db):
    # BLOCK: bot selezionato ma chat_id vuoto => niente persistenza.
    app = _make(monkeypatch, db)
    try:
        bid = db.save_telegram_bot("B", "fake-bot-value-not-real-abc")
        app.tg_selected_bot_id = bid
        app.tg_bot_chat_id_var.set("   ")
        app.tg_bot_chat_title_var.set("Titolo senza id")
        app._add_telegram_bot_chat_from_ui()
        assert db.get_telegram_bot_chats(bid) == []
    finally:
        app.destroy()


def test_chats_are_scoped_per_bot(monkeypatch, db):
    # BLOCK isolamento: una chat aggiunta al bot 1 NON compare sul bot 2.
    app = _make(monkeypatch, db)
    try:
        b1 = db.save_telegram_bot("uno", "val-uno-aaa")
        b2 = db.save_telegram_bot("due", "val-due-bbb")

        app.tg_selected_bot_id = b1
        app.tg_bot_chat_id_var.set("-100111")
        app._add_telegram_bot_chat_from_ui()

        assert [c["chat_id"] for c in db.get_telegram_bot_chats(b1)] == ["-100111"]
        assert db.get_telegram_bot_chats(b2) == []  # isolato
    finally:
        app.destroy()


def test_add_same_chat_id_updates_title_not_duplicates(monkeypatch, db):
    # Upsert per chat_id: riaggiungere lo stesso id aggiorna il titolo (una riga).
    app = _make(monkeypatch, db)
    try:
        bid = db.save_telegram_bot("B", "fake-bot-value-not-real-abc")
        app.tg_selected_bot_id = bid

        app.tg_bot_chat_id_var.set("-100222")
        app.tg_bot_chat_title_var.set("Primo")
        app._add_telegram_bot_chat_from_ui()

        app.tg_bot_chat_id_var.set("-100222")
        app.tg_bot_chat_title_var.set("Secondo")
        app._add_telegram_bot_chat_from_ui()

        chats = db.get_telegram_bot_chats(bid)
        assert len(chats) == 1
        assert chats[0]["title"] == "Secondo"
    finally:
        app.destroy()


def test_remove_chat_from_bot(monkeypatch, db):
    app = _make(monkeypatch, db)
    try:
        bid = db.save_telegram_bot("B", "fake-bot-value-not-real-abc")
        app.tg_selected_bot_id = bid
        for cid in ("-100a", "-100b"):
            app.tg_bot_chat_id_var.set(cid)
            app._add_telegram_bot_chat_from_ui()
        assert len(db.get_telegram_bot_chats(bid)) == 2

        app._remove_telegram_bot_chat("-100a")
        remaining = [c["chat_id"] for c in db.get_telegram_bot_chats(bid)]
        assert remaining == ["-100b"]
    finally:
        app.destroy()


def test_remove_chat_noop_without_selected_bot(monkeypatch, db):
    # Nessun bot selezionato => rimozione no-op (niente crash, niente effetto).
    app = _make(monkeypatch, db)
    try:
        bid = db.save_telegram_bot("B", "fake-bot-value-not-real-abc")
        db.set_telegram_bot_chats(bid, [{"chat_id": "-100z", "title": "z"}])
        app.tg_selected_bot_id = None
        app._remove_telegram_bot_chat("-100z")
        assert [c["chat_id"] for c in db.get_telegram_bot_chats(bid)] == ["-100z"]
    finally:
        app.destroy()


def test_add_chat_on_stale_bot_is_failclosed(monkeypatch, db):
    # BLOCK: bot selezionato ma rimosso "altrove" (selezione stale). L'aggiunta
    # deve pulire la selezione stale (prune) e bloccare => nessuna chat orfana.
    app = _make(monkeypatch, db)
    try:
        bid = db.save_telegram_bot("B", "fake-bot-value-not-real-abc")
        db.remove_telegram_bot(bid)      # rimosso altrove
        app.tg_selected_bot_id = bid     # selezione stale
        app.tg_bot_chat_id_var.set("-100stale")
        app._add_telegram_bot_chat_from_ui()

        assert app.tg_selected_bot_id is None          # prune ha azzerato
        assert db.get_telegram_bot_chats(bid) == []    # nessuna chat orfana
    finally:
        app.destroy()
