"""PR-4 (epica #374) — gestione multi-bot (Bot API) nella tab Telegram della GUI.

Espone il CRUD di persistenza PR-3 (`telegram_bots`) tramite metodi-logica sul
mixin `TelegramModule`, testabili headless (senza display) col pattern del tab
Alert: app in `test_mode=True` + `Database` REALE su file temporaneo iniettato
come `app.db`, si pilotano le tk-var e si chiamano i metodi-logica, verificando
la persistenza reale.

SOLO persistenza/GUI: nessun wiring runtime (i bot non sono ancora ascoltati).
Il `bot_token` è un segreto: mai ricaricato in chiaro nell'editor.
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_mini_gui_integration import _install_mini_gui_fakes  # noqa: E402

from database import Database  # noqa: E402

pytestmark = pytest.mark.integration

_SAMPLE = "fake-bot-value-not-real-abc"


def _make(monkeypatch, db):
    mg = _install_mini_gui_fakes(monkeypatch)
    app = mg.MiniPickfairGUI(test_mode=True)
    app.db = db  # inietta la persistenza REALE per il CRUD bot
    return app


@pytest.fixture
def db(tmp_path):
    return Database(str(tmp_path / "t.db"))


def test_create_bot_persists_and_clears_token_entry(monkeypatch, db):
    app = _make(monkeypatch, db)
    try:
        app.tg_bot_label_var.set("Bot Uno")
        app.tg_bot_token_var.set(_SAMPLE)
        app.tg_bot_active_var.set(True)
        app.tg_selected_bot_id = None  # creazione
        app._save_telegram_bot_from_ui()

        bots = db.get_telegram_bots()
        assert len(bots) == 1
        assert bots[0]["label"] == "Bot Uno"
        assert bots[0]["bot_token"] == _SAMPLE
        assert bots[0]["is_active"] is True
        # token pulito dall'entry dopo il salvataggio (mai in chiaro persistente in UI)
        assert app.tg_bot_token_var.get() == ""
        # selezione aggiornata al nuovo bot
        assert app.tg_selected_bot_id == bots[0]["id"]
    finally:
        app.destroy()


def test_create_bot_blocked_without_token(monkeypatch, db):
    app = _make(monkeypatch, db)
    try:
        app.tg_bot_label_var.set("Senza Token")
        app.tg_bot_token_var.set("")
        app.tg_selected_bot_id = None
        app._save_telegram_bot_from_ui()
        assert db.get_telegram_bots() == []  # fail-closed: niente token, niente bot
    finally:
        app.destroy()


def test_create_bot_blocked_without_label(monkeypatch, db):
    app = _make(monkeypatch, db)
    try:
        app.tg_bot_label_var.set("   ")
        app.tg_bot_token_var.set(_SAMPLE)
        app.tg_selected_bot_id = None
        app._save_telegram_bot_from_ui()
        assert db.get_telegram_bots() == []
    finally:
        app.destroy()


def test_update_with_blank_token_preserves_existing(monkeypatch, db):
    app = _make(monkeypatch, db)
    try:
        bid = db.save_telegram_bot("B", "orig-value-aaa", is_active=True)
        app.tg_selected_bot_id = bid
        app.tg_bot_label_var.set("B rinominato")
        app.tg_bot_token_var.set("")  # vuoto in update -> preserva
        app.tg_bot_active_var.set(False)
        app._save_telegram_bot_from_ui()

        bot = db.get_telegram_bots()[0]
        assert bot["label"] == "B rinominato"
        assert bot["is_active"] is False
        assert bot["bot_token"] == "orig-value-aaa"  # preservato
    finally:
        app.destroy()


def test_update_with_new_token_replaces(monkeypatch, db):
    app = _make(monkeypatch, db)
    try:
        bid = db.save_telegram_bot("B", "orig-value-aaa")
        app.tg_selected_bot_id = bid
        app.tg_bot_label_var.set("B")
        app.tg_bot_token_var.set("nuovo-value-bbb")
        app._save_telegram_bot_from_ui()
        assert db.get_telegram_bots()[0]["bot_token"] == "nuovo-value-bbb"
    finally:
        app.destroy()


def test_remove_selected_bot(monkeypatch, db):
    app = _make(monkeypatch, db)
    try:
        b1 = db.save_telegram_bot("uno", "val-uno")
        b2 = db.save_telegram_bot("due", "val-due")
        app.tg_selected_bot_id = b1
        app._remove_selected_telegram_bot()

        ids = {b["id"] for b in db.get_telegram_bots()}
        assert ids == {b2}
        assert app.tg_selected_bot_id is None
    finally:
        app.destroy()


def test_load_selected_bot_never_exposes_token(monkeypatch, db):
    app = _make(monkeypatch, db)
    try:
        bid = db.save_telegram_bot("B", "hidden-value-ccc", is_active=False)
        app.tg_selected_bot_id = bid
        app._load_selected_bot_into_editor()

        assert app.tg_bot_label_var.get() == "B"
        assert bool(app.tg_bot_active_var.get()) is False
        # il token NON viene MAI ricaricato in chiaro nell'entry (resta "" = preserva)
        assert app.tg_bot_token_var.get() == ""
    finally:
        app.destroy()


def test_new_button_enables_creating_multiple_bots(monkeypatch, db):
    # BLOCK del bug segnalato da GPT/Fugu/Fable: dopo il primo save la selezione
    # resta sul bot creato; senza "Nuovo" (reset selezione) un secondo save
    # AGGIORNEREBBE il bot 1 invece di crearne un altro -> multi-bot irraggiungibile.
    app = _make(monkeypatch, db)
    try:
        app.tg_bot_label_var.set("Uno")
        app.tg_bot_token_var.set(_SAMPLE)
        app.tg_selected_bot_id = None
        app._save_telegram_bot_from_ui()
        assert len(db.get_telegram_bots()) == 1
        assert app.tg_selected_bot_id is not None  # selezione ora sul bot 1

        app._new_telegram_bot()  # reset editor per una nuova creazione
        assert app.tg_selected_bot_id is None
        assert app.tg_bot_label_var.get() == ""
        assert app.tg_bot_token_var.get() == ""

        app.tg_bot_label_var.set("Due")
        app.tg_bot_token_var.set("altro-value-ddd")
        app._save_telegram_bot_from_ui()
        labels = sorted(b["label"] for b in db.get_telegram_bots())
        assert labels == ["Due", "Uno"]  # DUE bot distinti, non un update
    finally:
        app.destroy()


def test_deselect_clears_editor(monkeypatch, db):
    app = _make(monkeypatch, db)
    try:
        bid = db.save_telegram_bot("B", _SAMPLE, is_active=False)
        app.tg_selected_bot_id = bid
        app._load_selected_bot_into_editor()
        assert app.tg_bot_label_var.get() == "B"

        # deseleziona (id None): l'editor si pulisce, niente dati stale
        app.tg_selected_bot_id = None
        app._load_selected_bot_into_editor()
        assert app.tg_bot_label_var.get() == ""
        assert app.tg_bot_token_var.get() == ""
        assert bool(app.tg_bot_active_var.get()) is True
    finally:
        app.destroy()


def test_save_on_stale_selection_is_failclosed(monkeypatch, db):
    # Selezione stale su bot rimosso "altrove": save_telegram_bot(bot_id inesistente)
    # -> ValueError (rowcount 0) gestito da _safe_show_error. Nessun bot fantasma.
    app = _make(monkeypatch, db)
    try:
        bid = db.save_telegram_bot("B", _SAMPLE)
        db.remove_telegram_bot(bid)
        app.tg_selected_bot_id = bid  # stale
        app.tg_bot_label_var.set("Zombie")
        app.tg_bot_token_var.set("val-x")
        app._save_telegram_bot_from_ui()
        assert db.get_telegram_bots() == []  # fail-closed: nessuna riga creata/riassegnata
    finally:
        app.destroy()
