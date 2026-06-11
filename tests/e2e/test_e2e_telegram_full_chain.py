"""E2E catena completa Telegram: messaggio grezzo -> ordine simulato.

Copre il flusso intero come sistema, non i pezzi:
listener live (client fake) -> parser -> TelegramService -> bus
SIGNAL_RECEIVED -> trading handler -> broker SIMULATO -> scrittura DB.

Usa i messaggi nel formato reale del canale dell'owner come fixture.
Nessuna rete, nessun denaro: il broker e' un fake e simulation_mode
viene verificato lungo tutta la catena.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field

import pytest

from core.duplication_guard import DuplicationGuard
from core.event_bus import EventBus
from services.telegram_service import TelegramService

# ---------------------------------------------------------------------------
# Fixtures: messaggi REALI del canale (formato P.Exc./P.Bet.)
# ---------------------------------------------------------------------------

MSG_NEXT_GOL = """🆚Reading v Burton Albion
🏆English Sky Bet League 1
⌚ time, 11m, 0 - 0

🔥 P.Exc. NEXT GOL 🔊 ✅

📊88.33%"""

MSG_CHIACCHIERE = "Buongiorno ragazzi, oggi grande giornata! Ricordate il money management."

# Messaggio di ALTRO mercato che contiene "0,5 HT" solo nella riga
# statistica 📈Quota: NON deve attivare il pattern 0,5 HT.
MSG_STATS_COLLISION = """P.Bet. OVER SUCCESSIVO 🔊 ❌

🏆Welsh Premiership
🆚Colwyn Bay v Flint Town United
⚽ 0 - 0
⌚ 5m
📈Quota 0,5 HT Prematch:1.3
📊70.21%"""


class _FakeTelethonClient:
    def __init__(self):
        self._disconnected = asyncio.Event()

    async def connect(self):
        self._disconnected = asyncio.Event()

    @staticmethod
    async def is_user_authorized():
        return True

    def add_event_handler(self, callback, event_filter=None):
        """No-op: il fake registra senza dispatch (i messaggi si iniettano)."""

    def is_connected(self):
        return not self._disconnected.is_set()

    async def run_until_disconnected(self):
        await self._disconnected.wait()

    async def disconnect(self):
        if self._disconnected is not None:
            self._disconnected.set()


@dataclass
class _TelegramCfg:
    enabled: bool = True
    api_id: str = "123"
    api_hash: str = "hash"
    session_string: str = "sess"
    monitored_chat_ids: list[int] = field(default_factory=lambda: [-100999])


class _Settings:
    def __init__(self, cfg):
        self.cfg = cfg

    def load_telegram_config(self):
        return self.cfg


class _Db:
    """DB fake: pattern keyword->mercato + registrazione segnali."""

    def __init__(self):
        self.received_signals = []
        self.saved_bets = []

    @staticmethod
    def get_signal_patterns(enabled_only=True):
        _ = enabled_only
        return [
            {
                "id": 1, "name": "next-gol", "enabled": True, "pattern": "",
                "keyword": "NEXT GOL", "market_type": "NEXT_GOAL",
                "bet_side": "BACK", "selection_template": "Next Goal",
                "mm_auto": True,
            },
            {
                "id": 2, "name": "05ht", "enabled": True, "pattern": "",
                "keyword": "0,5 HT", "market_type": "OVER_UNDER_HT_05",
                "bet_side": "BACK", "selection_template": "Over 0.5",
                "mm_auto": True,
            },
        ]

    def save_received_signal(self, payload):
        self.received_signals.append(dict(payload))

    def save_simulation_bet(self, **payload):
        self.saved_bets.append(payload)


class _FakeSimulationBroker:
    """Broker SOLO simulato: nessun denaro reale puo' passare di qui."""

    def __init__(self):
        self.placed_orders = []

    def place_bet(self, **payload):
        assert payload.get("simulation_mode") is True, (
            "REAL MONEY GUARD: un ordine senza simulation_mode=True "
            "non deve mai raggiungere il broker in questo E2E"
        )
        self.placed_orders.append(payload)
        return {"status": "SUCCESS", "bet_id": f"SIM-{len(self.placed_orders)}"}


def _build_pipeline():
    """Catena completa: service Telegram reale + trading handler simulato."""
    bus = EventBus()
    db = _Db()
    broker = _FakeSimulationBroker()
    guard = DuplicationGuard()

    svc = TelegramService(
        settings_service=_Settings(_TelegramCfg()),
        db=db,
        bus=bus,
        client_factory=lambda *_a: _FakeTelethonClient(),
        connect_timeout=5.0,
    )

    def trading_handler(signal):
        # acquire() e' atomico (check+register); is_duplicate da solo non
        # registra la chiave e non va usato per gating ordini.
        key = guard.build_event_key(signal)
        if not guard.acquire(key):
            return
        broker.place_bet(
            market_type=signal.get("market_type"),
            event_name=signal.get("event_name"),
            selection=signal.get("selection"),
            simulation_mode=True,
            received_at=signal.get("received_at"),
        )
        db.save_simulation_bet(**{
            "market_type": signal.get("market_type"),
            "event_name": signal.get("event_name"),
        })

    bus.subscribe("SIGNAL_RECEIVED", trading_handler)
    return svc, bus, db, broker


def _wait_until(condition, timeout=3.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return condition()


@pytest.mark.e2e
def test_raw_channel_message_to_simulated_order_and_db():
    svc, _bus, db, broker = _build_pipeline()
    assert svc.start()["started"] is True
    try:
        out = svc.listener.handle_incoming(MSG_NEXT_GOL, chat_id=-100999)
        assert out is not None

        assert _wait_until(lambda: len(broker.placed_orders) == 1)
        order = broker.placed_orders[0]
        assert order["market_type"] == "NEXT_GOAL"
        assert order["event_name"] == "Reading v Burton Albion"
        assert order["simulation_mode"] is True
        # Evidenza persistita: segnale ricevuto + bet simulata.
        # Il service normalizza simulation_mode a bool (la modalita' la
        # decide il runtime); il guard soldi-veri sta nel broker fake.
        assert len(db.received_signals) == 1
        assert isinstance(db.received_signals[0]["simulation_mode"], bool)
        assert db.received_signals[0]["received_at"] == out["received_at"]
        assert len(db.saved_bets) == 1
    finally:
        svc.stop()


@pytest.mark.e2e
def test_duplicate_message_places_single_order():
    svc, _bus, db, broker = _build_pipeline()
    assert svc.start()["started"] is True
    try:
        svc.listener.handle_incoming(MSG_NEXT_GOL, chat_id=-100999)
        svc.listener.handle_incoming(MSG_NEXT_GOL, chat_id=-100999)
        _wait_until(lambda: len(broker.placed_orders) >= 1)
        # Finestra di grazia DELIBERATA: il bus e' asincrono e un eventuale
        # secondo ordine (bug) arriverebbe dopo il primo; senza attesa il
        # test non potrebbe mai fallire per un duplicato.
        time.sleep(0.1)
        assert len(broker.placed_orders) == 1
        # Dedup a livello ordine: la bet simulata persiste una sola volta
        # (i segnali ricevuti restano 2: il dedup e' del trading handler).
        assert len(db.saved_bets) == 1
        assert len(db.received_signals) == 2
    finally:
        svc.stop()


@pytest.mark.e2e
def test_non_signal_message_places_no_order_but_updates_liveness():
    svc, _bus, db, broker = _build_pipeline()
    assert svc.start()["started"] is True
    try:
        out = svc.listener.handle_incoming(MSG_CHIACCHIERE, chat_id=-100999)
        assert out is None
        assert broker.placed_orders == []
        assert db.received_signals == []
        # La liveness si aggiorna comunque (stale detector dell'autoheal)
        assert svc.listener.last_successful_message_ts is not None
    finally:
        svc.stop()


@pytest.mark.e2e
def test_keyword_in_stats_line_does_not_place_order():
    svc, _bus, _db, broker = _build_pipeline()
    assert svc.start()["started"] is True
    try:
        out = svc.listener.handle_incoming(MSG_STATS_COLLISION, chat_id=-100999)
        assert out is None
        assert broker.placed_orders == []
    finally:
        svc.stop()
