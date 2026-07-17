"""Test hard della persistenza Telegram multi-bot (epica #374, PR-3).

Copre lo strato di SOLA persistenza (nessun wiring runtime):
- `telegram_bots.bot_token` cifrato a riposo (enc:v1:) + assenza del plaintext
  su disco (guard PASS+BLOCK: sul path non cifrato fallirebbe);
- round-trip trasparente via `get_telegram_bots` (decifratura);
- passthrough del plaintext legacy + migrazione al primo save;
- chat per-bot isolate (scoped al bot_id);
- rimozione bot che elimina anche le sue chat (senza toccare gli altri bot);
- update con token `None` che PRESERVA il token esistente;
- token vuoto salvato vuoto (non cifrato).

I nomi/valori placeholder NON contengono "token"/"secret" (euristiche di
redazione diff e Bandit B105 sono sul nome): sono valori fittizi di test.
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest

from database import Database

_SAMPLE = "fake-bot-value-not-real-abc123"


def _raw_bot_token(db_path: str, bot_id: int):
    """Legge il valore GREZZO della colonna bot_token direttamente dal file DB."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute("SELECT bot_token FROM telegram_bots WHERE id = ?", (bot_id,))
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


@pytest.mark.unit
@pytest.mark.guardrail
def test_bot_token_encrypted_at_rest_and_round_trip():
    with tempfile.TemporaryDirectory() as td:
        db_path = str(Path(td) / "test.db")
        db = Database(db_path)

        bot_id = db.save_telegram_bot("bot-uno", _SAMPLE)

        # BLOCK: senza cifratura esplicita nel CRUD la colonna sarebbe plaintext.
        raw = _raw_bot_token(db_path, bot_id)
        assert raw and raw.startswith("enc:v1:"), "bot_token deve essere cifrato su disco"
        assert _SAMPLE not in raw, "il plaintext del token non deve comparire su disco"

        # Round-trip trasparente: get_telegram_bots decifra.
        bots = db.get_telegram_bots()
        assert len(bots) == 1
        assert bots[0]["id"] == bot_id
        assert bots[0]["label"] == "bot-uno"
        assert bots[0]["bot_token"] == _SAMPLE
        assert bots[0]["is_active"] is True


@pytest.mark.unit
@pytest.mark.guardrail
def test_legacy_plaintext_bot_token_passthrough_then_migrates():
    with tempfile.TemporaryDirectory() as td:
        db_path = str(Path(td) / "test.db")
        db = Database(db_path)
        # Riga legacy in chiaro inserita direttamente (simula pre-cifratura).
        legacy = "legacy-plaintext-bot-value-xyz"
        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO telegram_bots(id, label, bot_token, is_active, created_at, updated_at) "
            "VALUES (1, 'legacy', ?, 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
            (legacy,),
        )
        conn.commit()
        conn.close()

        # Lettura senza errori: passthrough del plaintext legacy.
        assert _raw_bot_token(db_path, 1) == legacy  # ancora in chiaro
        bots = db.get_telegram_bots()
        assert bots[0]["bot_token"] == legacy

        # Migrazione al primo save (update): la colonna passa a enc:v1:.
        db.save_telegram_bot("legacy", legacy, bot_id=1)
        raw = _raw_bot_token(db_path, 1)
        assert raw.startswith("enc:v1:")
        assert legacy not in raw
        assert db.get_telegram_bots()[0]["bot_token"] == legacy


@pytest.mark.unit
def test_empty_token_stored_empty_not_encrypted():
    with tempfile.TemporaryDirectory() as td:
        db_path = str(Path(td) / "test.db")
        db = Database(db_path)
        bot_id = db.save_telegram_bot("no-token", "")
        assert _raw_bot_token(db_path, bot_id) == ""  # vuoto, non enc:v1:
        assert db.get_telegram_bots()[0]["bot_token"] == ""


@pytest.mark.unit
def test_update_with_none_token_preserves_existing():
    with tempfile.TemporaryDirectory() as td:
        db_path = str(Path(td) / "test.db")
        db = Database(db_path)
        bot_id = db.save_telegram_bot("bot", _SAMPLE, is_active=True)
        raw_before = _raw_bot_token(db_path, bot_id)

        # Update solo di label/stato: token=None -> preservato (non azzerato).
        db.save_telegram_bot("bot-rinominato", None, is_active=False, bot_id=bot_id)
        raw_after = _raw_bot_token(db_path, bot_id)

        bots = db.get_telegram_bots()
        assert bots[0]["label"] == "bot-rinominato"
        assert bots[0]["is_active"] is False
        assert bots[0]["bot_token"] == _SAMPLE, "il token deve essere preservato"
        assert raw_after == raw_before, "la colonna cifrata non deve cambiare"


@pytest.mark.unit
def test_update_with_empty_string_clears_token():
    # Confine semantico None (preserva) vs "" (azzera) sul path di UPDATE:
    # save con "" deve SOVRASCRIVERE il token cifrato esistente con vuoto.
    with tempfile.TemporaryDirectory() as td:
        db_path = str(Path(td) / "test.db")
        db = Database(db_path)
        bot_id = db.save_telegram_bot("bot", _SAMPLE)
        assert _raw_bot_token(db_path, bot_id).startswith("enc:v1:")

        db.save_telegram_bot("bot", "", bot_id=bot_id)  # "" = azzera (non None)
        assert _raw_bot_token(db_path, bot_id) == ""
        assert db.get_telegram_bots()[0]["bot_token"] == ""


@pytest.mark.unit
def test_update_nonexistent_bot_raises():
    # Contratto esplicito: update di un bot_id inesistente non e' un no-op
    # silenzioso -> solleva (fail-closed).
    with tempfile.TemporaryDirectory() as td:
        db_path = str(Path(td) / "test.db")
        db = Database(db_path)
        with pytest.raises(ValueError):
            db.save_telegram_bot("fantasma", _SAMPLE, bot_id=99999)


@pytest.mark.unit
def test_set_bot_chats_deduplicates_same_chat_id():
    # chat_id duplicato nello stesso batch NON deve sollevare IntegrityError
    # sulla PK composta: upsert (l'ultima entry vince).
    with tempfile.TemporaryDirectory() as td:
        db_path = str(Path(td) / "test.db")
        db = Database(db_path)
        bot_id = db.save_telegram_bot("uno", _SAMPLE)
        db.set_telegram_bot_chats(bot_id, [
            {"chat_id": "-100999", "title": "primo", "is_active": True},
            {"chat_id": "-100999", "title": "secondo", "is_active": False},  # duplicato
        ])
        chats = db.get_telegram_bot_chats(bot_id)
        assert len(chats) == 1
        assert chats[0]["chat_id"] == "-100999"
        assert chats[0]["title"] == "secondo"      # ultima entry vince
        assert chats[0]["is_active"] is False


@pytest.mark.unit
def test_bot_chats_are_scoped_per_bot():
    with tempfile.TemporaryDirectory() as td:
        db_path = str(Path(td) / "test.db")
        db = Database(db_path)
        b1 = db.save_telegram_bot("uno", _SAMPLE)
        b2 = db.save_telegram_bot("due", _SAMPLE)

        db.set_telegram_bot_chats(b1, [
            {"chat_id": "-100111", "title": "canale-1", "is_active": True},
            {"chat_id": "-100112", "title": "canale-2", "is_active": False},
        ])
        db.set_telegram_bot_chats(b2, [{"chat_id": "-100222", "title": "canale-3"}])

        c1 = db.get_telegram_bot_chats(b1)
        c2 = db.get_telegram_bot_chats(b2)
        assert {c["chat_id"] for c in c1} == {"-100111", "-100112"}
        assert {c["chat_id"] for c in c2} == {"-100222"}
        # is_active preservato per-riga.
        assert {c["chat_id"]: c["is_active"] for c in c1} == {"-100111": True, "-100112": False}

        # Re-set di b1 non tocca b2 (swap scoped).
        db.set_telegram_bot_chats(b1, [{"chat_id": "-100113", "title": "nuovo"}])
        assert {c["chat_id"] for c in db.get_telegram_bot_chats(b1)} == {"-100113"}
        assert {c["chat_id"] for c in db.get_telegram_bot_chats(b2)} == {"-100222"}


@pytest.mark.unit
def test_remove_bot_deletes_its_chats_only():
    with tempfile.TemporaryDirectory() as td:
        db_path = str(Path(td) / "test.db")
        db = Database(db_path)
        b1 = db.save_telegram_bot("uno", _SAMPLE)
        b2 = db.save_telegram_bot("due", _SAMPLE)
        db.set_telegram_bot_chats(b1, [{"chat_id": "-100111"}])
        db.set_telegram_bot_chats(b2, [{"chat_id": "-100222"}])

        db.remove_telegram_bot(b1)

        ids = {b["id"] for b in db.get_telegram_bots()}
        assert ids == {b2}, "solo il bot rimosso sparisce"
        assert db.get_telegram_bot_chats(b1) == [], "le chat del bot rimosso sono eliminate"
        assert {c["chat_id"] for c in db.get_telegram_bot_chats(b2)} == {"-100222"}
