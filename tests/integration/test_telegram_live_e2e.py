"""
Test E2E Telegram Live — usa credenziali reali via GitHub Secrets.

Secrets richiesti:
  TG_API_ID          — Telegram App ID (da my.telegram.org)
  TG_API_HASH        — Telegram App Hash
  TG_SESSION_STRING  — Stringa di sessione Telethon (account follower/listener)
  TG_BOT_TOKEN       — Token del bot (mittente segnali master)
  TG_TEST_CHAT_ID    — ID del gruppo test (bot + account utente sono membri)

Setup una-tantum:
  1. Crea un gruppo Telegram di test
  2. Aggiungi il bot come admin (per poter inviare messaggi)
  3. Aggiungi l'account utente (il cui session_string è in TG_SESSION_STRING)
  4. Copia il chat_id del gruppo in TG_TEST_CHAT_ID (numero negativo, es. -1001234567890)

I test si saltano automaticamente se i secrets non sono configurati.
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
import time
import uuid

import pytest
import requests

# =========================================================
# CREDENZIALI DA ENVIRONMENT (GitHub Secrets)
# =========================================================
TG_API_ID = os.getenv("TG_API_ID", "")
TG_API_HASH = os.getenv("TG_API_HASH", "")
TG_SESSION_STRING = os.getenv("TG_SESSION_STRING", "")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_TEST_CHAT_ID = os.getenv("TG_TEST_CHAT_ID", "")

_CREDS_OK = all([TG_API_ID, TG_API_HASH, TG_SESSION_STRING, TG_BOT_TOKEN, TG_TEST_CHAT_ID])

pytestmark = pytest.mark.skipif(
    not _CREDS_OK,
    reason="Credenziali Telegram non configurate (TG_API_ID, TG_API_HASH, TG_SESSION_STRING, TG_BOT_TOKEN, TG_TEST_CHAT_ID)",
)


# =========================================================
# HELPERS
# =========================================================

def _bot_send(text: str) -> bool:
    """Invia un messaggio al gruppo test via Bot API (senza sessione Telethon)."""
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    resp = requests.post(url, json={"chat_id": TG_TEST_CHAT_ID, "text": text}, timeout=10)
    return resp.status_code == 200


def _make_tag() -> str:
    """UUID breve usato per identificare univocamente ogni messaggio di test."""
    return f"[TEST-{uuid.uuid4().hex[:8].upper()}]"


async def _listen_and_send(text_to_send: str, tag: str, timeout: float = 25.0) -> str | None:
    """
    Apre una connessione Telethon, registra l'handler, invia il messaggio via bot,
    attende che arrivi il messaggio con il tag univoco, restituisce il testo ricevuto.
    """
    from telethon import TelegramClient, events
    from telethon.sessions import StringSession

    chat_id = int(TG_TEST_CHAT_ID)
    received: asyncio.Future = asyncio.get_event_loop().create_future()

    client = TelegramClient(StringSession(TG_SESSION_STRING), int(TG_API_ID), TG_API_HASH)

    async with client:
        @client.on(events.NewMessage(chats=[chat_id]))
        async def _handler(event):
            msg_text = event.message.text or ""
            if tag in msg_text and not received.done():
                received.set_result(msg_text)

        # Piccolo delay per garantire che l'handler sia registrato prima dell'invio
        await asyncio.sleep(0.8)

        ok = _bot_send(text_to_send)
        assert ok, "Bot API: invio messaggio fallito"

        try:
            return await asyncio.wait_for(received, timeout=timeout)
        except asyncio.TimeoutError:
            return None


def _run(coro):
    """Esegue una coroutine nel loop di test."""
    return asyncio.get_event_loop().run_until_complete(coro)


# =========================================================
# FIXTURE: DB IN-MEMORY con pattern custom
# =========================================================

@pytest.fixture()
def listener_with_patterns():
    """
    Restituisce un TelegramListener con un DB SQLite in-memory
    che contiene due pattern personalizzati di test.
    """
    from telegram_listener import TelegramListener

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE signal_patterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            label TEXT,
            pattern TEXT,
            market_type TEXT DEFAULT 'MATCH_ODDS',
            bet_side TEXT DEFAULT 'BACK',
            selection_template TEXT DEFAULT '',
            min_minute INTEGER,
            max_minute INTEGER,
            min_score INTEGER,
            max_score INTEGER,
            live_only INTEGER DEFAULT 0,
            priority INTEGER DEFAULT 100,
            enabled INTEGER DEFAULT 1
        )
    """)

    # Pattern 1: OVER con template
    conn.execute("""
        INSERT INTO signal_patterns
        (label, pattern, market_type, bet_side, selection_template, min_minute, max_minute, live_only, priority)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, ("Over Test", r"OVER\s*(\d+[.,]?\d*)", "OVER_UNDER", "BACK", "Over {over_line}", 20, 85, 1, 10))

    # Pattern 2: NEXT GOL
    conn.execute("""
        INSERT INTO signal_patterns
        (label, pattern, market_type, bet_side, selection_template, min_minute, max_minute, live_only, priority)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, ("Next Gol", r"NEXT\s*GOL|PROSSIMO\s*GOL", "NEXT_GOAL", "BACK", "Next Goal", 1, 90, 1, 20))

    conn.commit()

    class _FakeDB:
        def get_signal_patterns(self, enabled_only=True):
            rows = conn.execute(
                "SELECT * FROM signal_patterns WHERE enabled=1 ORDER BY priority ASC"
            ).fetchall()
            return [dict(r) for r in rows]

    listener = TelegramListener(
        api_id=int(TG_API_ID or "1"),
        api_hash=TG_API_HASH or "x",
        session_string=TG_SESSION_STRING,
        db=_FakeDB(),
    )
    yield listener
    conn.close()


# =========================================================
# TEST 1: MASTER SIGNAL — parse completo
# =========================================================

@pytest.mark.asyncio
async def test_master_signal_received_and_parsed(listener_with_patterns):
    """
    Il bot invia un MASTER SIGNAL strutturato.
    Il listener Telethon lo riceve e il parser lo decodifica correttamente.
    """
    tag = _make_tag()
    market_id = "1.987654321"
    selection_id = "55123"

    text = (
        f"🟢 MASTER SIGNAL {tag}\n\n"
        f"event_name: Arsenal v Liverpool\n"
        f"market_name: Over/Under 2.5 Goals\n"
        f"selection: Over 2.5\n"
        f"action: BACK\n"
        f"master_price: 1.85\n"
        f"market_id: {market_id}\n"
        f"selection_id: {selection_id}\n"
        f"status: MATCHED"
    )

    received_text = await _listen_and_send(text, tag)
    assert received_text is not None, "Messaggio non ricevuto entro il timeout"

    signal = listener_with_patterns.parse_signal(received_text)

    assert signal is not None, "parse_signal ha restituito None"
    assert signal["market_id"] == market_id
    assert signal["selection_id"] == int(selection_id)
    assert signal["bet_type"] == "BACK"
    assert abs(signal["price"] - 1.85) < 0.01
    assert signal["event_name"] == "Arsenal v Liverpool"


# =========================================================
# TEST 2: PATTERN PERSONALIZZATO — OVER con template
# =========================================================

@pytest.mark.asyncio
async def test_custom_pattern_over_template(listener_with_patterns):
    """
    Il bot invia un messaggio che matcha il pattern OVER.
    Il sistema calcola over_line = gol_totali + 0.5 e genera la selection corretta.
    """
    tag = _make_tag()

    # Score 1-0 → total_goals=1 → over_line=1.5
    text = f"🆚 Roma v Milan\n1-0 62m\nOVER 1 @2.20\nstake 5\n{tag}"

    received_text = await _listen_and_send(text, tag)
    assert received_text is not None, "Messaggio non ricevuto entro il timeout"

    signal = listener_with_patterns.parse_signal(received_text)

    assert signal is not None, "Nessun pattern ha fatto match"
    assert signal.get("market_type") == "OVER_UNDER"
    assert signal.get("bet_type") == "BACK"
    # over_line = 1 (gol totali) + 0.5
    assert signal.get("selection") == "Over 1.5", f"selection inattesa: {signal.get('selection')}"
    assert signal.get("minute") == 62


# =========================================================
# TEST 3: PATTERN PERSONALIZZATO — filtro min_minute non passa
# =========================================================

@pytest.mark.asyncio
async def test_custom_pattern_filtered_by_minute(listener_with_patterns):
    """
    Il pattern OVER ha min_minute=20.
    Un messaggio al minuto 10 NON deve fare match con quel pattern.
    """
    tag = _make_tag()

    # Minuto 10 < min_minute 20 → il pattern OVER deve essere saltato
    text = f"🆚 Juve v Inter\n0-0 10m\nOVER 0 @1.50\nstake 3\n{tag}"

    received_text = await _listen_and_send(text, tag)
    assert received_text is not None, "Messaggio non ricevuto entro il timeout"

    signal = listener_with_patterns.parse_signal(received_text)

    # Nessun pattern custom deve matchare — potrebbe cadere nel legacy o None
    if signal is not None:
        assert signal.get("market_type") != "OVER_UNDER" or signal.get("minute", 0) >= 20, (
            "Pattern OVER non doveva matchare al minuto 10"
        )


# =========================================================
# TEST 4: PATTERN PERSONALIZZATO — NEXT GOL
# =========================================================

@pytest.mark.asyncio
async def test_custom_pattern_next_gol(listener_with_patterns):
    """
    Il bot invia "NEXT GOL" → il pattern NEXT_GOAL fa match.
    """
    tag = _make_tag()
    text = f"🆚 Napoli v Lazio\n2-1 55m\nNEXT GOL @3.10\nstake 10\n{tag}"

    received_text = await _listen_and_send(text, tag)
    assert received_text is not None, "Messaggio non ricevuto entro il timeout"

    signal = listener_with_patterns.parse_signal(received_text)

    assert signal is not None
    assert signal.get("market_type") == "NEXT_GOAL"
    assert signal.get("selection") == "Next Goal"
    assert signal.get("minute") == 55


# =========================================================
# TEST 5: SEGNALE LEGACY (formato emoji)
# =========================================================

@pytest.mark.asyncio
async def test_legacy_signal_emoji_format():
    """
    Il bot invia il formato emoji classico senza pattern custom.
    Il parser legacy estrae event_name, price, stake, minute.
    """
    from telegram_listener import TelegramListener

    # Listener senza DB (nessun pattern custom → va diretto al legacy parser)
    listener = TelegramListener(
        api_id=int(TG_API_ID),
        api_hash=TG_API_HASH,
        session_string=TG_SESSION_STRING,
        db=None,
    )

    tag = _make_tag()
    text = f"🆚 Atalanta v Fiorentina\n1-1 71m\n@2.50\nstake 8\n{tag}"

    received_text = await _listen_and_send(text, tag)
    assert received_text is not None, "Messaggio non ricevuto entro il timeout"

    signal = listener.parse_signal(received_text)

    assert signal is not None
    assert "Atalanta" in signal.get("event_name", "")
    assert abs(signal.get("price", 0) - 2.50) < 0.01
    assert signal.get("stake") == 8.0
    assert signal.get("minute") == 71


# =========================================================
# TEST 6: CASHOUT SIGNAL
# =========================================================

@pytest.mark.asyncio
async def test_cashout_all_signal():
    """
    Il bot invia CASHOUT ALL → il parser restituisce signal_type CASHOUT_ALL.
    """
    from telegram_listener import TelegramListener

    listener = TelegramListener(
        api_id=int(TG_API_ID),
        api_hash=TG_API_HASH,
        session_string=TG_SESSION_STRING,
        db=None,
    )

    tag = _make_tag()
    text = f"CASHOUT ALL {tag}"

    received_text = await _listen_and_send(text, tag)
    assert received_text is not None, "Messaggio non ricevuto entro il timeout"

    signal = listener.parse_signal(received_text)

    assert signal is not None
    assert signal.get("signal_type") == "CASHOUT_ALL"


# =========================================================
# TEST 7: MASTER → FOLLOWER — flusso completo end-to-end
# =========================================================

@pytest.mark.asyncio
async def test_master_follower_full_flow():
    """
    Simula il flusso completo master→follower:
    1. Bot invia MASTER SIGNAL (come farebbe TelegramSender del master)
    2. Listener Telethon (follower) lo riceve
    3. Il parser produce un payload con market_id e selection_id pronti per il broker
    """
    from telegram_listener import TelegramListener
    from services.telegram_signal_processor import TelegramSignalProcessor

    listener = TelegramListener(
        api_id=int(TG_API_ID),
        api_hash=TG_API_HASH,
        session_string=TG_SESSION_STRING,
        db=None,
    )
    processor = TelegramSignalProcessor()

    tag = _make_tag()
    market_id = "1.111222333"
    selection_id = "12345"

    # Formato identico a quello prodotto da TelegramSender._format_single_signal()
    text = (
        f"🟢 MASTER SIGNAL\n\n"
        f"event_name: Tottenham v Chelsea\n"
        f"market_name: Match Odds\n"
        f"selection: Tottenham\n"
        f"action: LAY\n"
        f"master_price: 3.20\n"
        f"market_id: {market_id}\n"
        f"selection_id: {selection_id}\n"
        f"status: MATCHED\n"
        f"{tag}"
    )

    received_text = await _listen_and_send(text, tag)
    assert received_text is not None, "Messaggio non ricevuto entro il timeout"

    raw_signal = listener.parse_signal(received_text)
    assert raw_signal is not None, "parse_signal ha restituito None"

    # Il processore costruisce il payload eseguibile
    payload = processor.build_runtime_signal(
        signal=raw_signal,
        stake=10.0,
        simulation_mode=True,
    )

    assert payload is not None, "build_runtime_signal ha restituito None"
    assert payload["market_id"] == market_id
    assert payload["selection_id"] == int(selection_id)
    assert payload["bet_type"] == "LAY"
    assert abs(payload["price"] - 3.20) < 0.01
    assert payload["stake"] == 10.0
    assert payload["simulation_mode"] is True
    assert payload["source"] == "TELEGRAM"
