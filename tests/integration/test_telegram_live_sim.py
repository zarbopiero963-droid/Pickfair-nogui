"""Telegram live in SIMULAZIONE (#562 M2, PR-B): gli stessi sette scenari di `test_telegram_live_e2e.py`, senza rete.

Il workflow `telegram-live-integration.yml` usa credenziali reali (bot + sessione Telethon) e su una lane CI senza
segreti si salterebbe sempre: uno skip non e' un PASS. Qui ogni scenario del test live gira COSI' COM'E' (stesso
messaggio, stesse asserzioni su parser e processore), ma il solo punto che tocca Telegram, `_listen_and_send`, e'
sostituito da un'eco locale: il messaggio "inviato" e' quello "ricevuto", come farebbe Telegram consegnandolo.

Cosa NON certifica (resta del test live, con credenziali e autorizzazione dell'owner): connessione Telethon,
invio via Bot API, consegna e latenza, filtro della chat reale. Nessun messaggio Telegram viene mai inviato: ogni
socket e ogni POST sono bloccati e il test fallisce se qualcuno ci prova.
"""
from __future__ import annotations

import asyncio
import importlib.util
import socket
import sys
from pathlib import Path

import pytest

_LIVE_PATH = Path(__file__).with_name("test_telegram_live_e2e.py")
_spec = importlib.util.spec_from_file_location("pf_telegram_live_e2e", _LIVE_PATH)
live = importlib.util.module_from_spec(_spec)
sys.modules["pf_telegram_live_e2e"] = live
_spec.loader.exec_module(live)

#: la fixture del test live (listener con due pattern custom su SQLite in memoria), riusata invariata
listener_with_patterns = live.listener_with_patterns

#: scenari del test live: (nome della funzione, usa la fixture `listener_with_patterns`)
SCENARIOS = (
    ("test_master_signal_received_and_parsed", True),
    ("test_custom_pattern_over_template", True),
    ("test_custom_pattern_filtered_by_minute", True),
    ("test_custom_pattern_next_gol", True),
    ("test_legacy_signal_emoji_format", False),
    ("test_cashout_all_signal", False),
    ("test_master_follower_full_flow", False),
)


class _NoNetwork(AssertionError):
    pass


def _refuse(*_args, **_kwargs):
    raise _NoNetwork("rete vietata nella simulazione Telegram: nessun messaggio reale")


@pytest.fixture(autouse=True)
def offline_telegram(monkeypatch):
    """Credenziali FINTE (mai reali), rete bloccata, trasporto sostituito da un'eco locale."""
    for name, value in (("TG_API_ID", "1"), ("TG_API_HASH", "sim-hash"), ("TG_SESSION_STRING", ""),
                        ("TG_BOT_TOKEN", "sim-token"), ("TG_TEST_CHAT_ID", "-1000000000001")):
        monkeypatch.setattr(live, name, value)
    monkeypatch.setattr(socket.socket, "connect", _refuse)
    monkeypatch.setattr(socket, "create_connection", _refuse)
    monkeypatch.setattr(live.requests, "post", _refuse)
    monkeypatch.setattr(live, "_bot_send", _refuse)
    delivered: list = []

    async def loopback(text_to_send: str, tag: str, timeout: float = 25.0):
        assert tag in text_to_send, "il messaggio di prova deve portare il proprio tag"
        delivered.append(text_to_send)
        return text_to_send                     # consegna come Telegram: testo identico, nessuna rete

    monkeypatch.setattr(live, "_listen_and_send", loopback)
    return delivered


@pytest.mark.parametrize("name,needs_listener", SCENARIOS)
def test_live_scenario_runs_offline_with_the_same_assertions(name, needs_listener, request, offline_telegram):
    scenario = getattr(live, name)
    args = (request.getfixturevalue("listener_with_patterns"),) if needs_listener else ()
    asyncio.run(scenario(*args))
    assert len(offline_telegram) == 1                                # un solo messaggio, consegnato in locale


def test_every_live_scenario_is_simulated():
    live_tests = {n for n in dir(live) if n.startswith("test_") and callable(getattr(live, n))}
    assert live_tests == {n for n, _ in SCENARIOS}


def test_the_simulation_cannot_reach_telegram(offline_telegram):
    with pytest.raises(_NoNetwork):
        socket.create_connection(("api.telegram.org", 443))
    with pytest.raises(_NoNetwork):
        live.requests.post("https://api.telegram.org/botX/sendMessage", json={})
    with pytest.raises(_NoNetwork):
        live._bot_send("ciao")


def test_a_wrong_parse_still_fails_in_simulation(offline_telegram, monkeypatch, request):
    """La simulazione non e' un PASS di sola attesa: se il parser sbaglia, lo scenario fallisce."""
    listener = request.getfixturevalue("listener_with_patterns")
    monkeypatch.setattr(listener, "parse_signal", lambda text: None)
    with pytest.raises(AssertionError):
        asyncio.run(live.test_master_signal_received_and_parsed(listener))
