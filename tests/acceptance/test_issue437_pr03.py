"""PR03 (#461/#437, pacchetto P02 / H01) — il settlement SIM arriva fino al
netto di mercato, al saldo e al ciclo Roserpina.

FALSO SUCCESSO RIPRODOTTO in Phase 0 sul main `840fc1e`, con l'app headless
VERA (``HeadlessApp.build``, Database reale in una cartella temporanea,
EventBus reale, SimulationBroker reale dietro BetfairService). Mercato 1.1,
BACK 10 @ 3.0 sul vincitore e LAY 5 @ 2.0 sul perdente, book aperto e poi
CHIUSO consegnati dall'ingresso reale ``MarketTracker.on_market_book``,
``settlement.poll_enabled`` acceso, un giro del poller::

    poller settlement vivo: False
    dopo i fill:           available=985.000 exposure=15.000
    dopo il mercato CHIUSO: available=985.000 exposure=15.000
    eventi RUNTIME_CLOSE_POSITION: 0
    risk_desk.realized_pnl: 0.0
    checkpoint ciclo presenti: False
    ATTESO: available=1023.875 exposure=0 netto=23.875 un evento di chiusura
    helper isolato: net=23.875 balance=1023.875
    helper sul broker reale dopo i fill: available=1008.875 exposure=15.000

Il test del calcolo passava chiamando ``record_realized_settlement`` con un
lordo scritto a mano e nessuna puntata; nessun codice di produzione lo
chiamava, e sul broker con i fill contava due volte stake e liability. In SIM
il poller non partiva nemmeno se abilitato.

Qui si parte dagli ingressi reali e si confronta ogni numero con un calcolo
indipendente scritto a mano. Unico stub: ``telegram_service.start/stop``
(niente rete).
"""
from __future__ import annotations

import threading
import time
from typing import Any, Dict, List

import pytest

from core.pnl_engine import PnLEngine
from core.runtime_controller import RuntimeController
from core.system_state import RoserpinaConfig, RuntimeMode
from simulation_broker import SimulationBroker

COMMISSIONE = 4.5


# --------------------------------------------------------------------------
# L'applicazione vera
# --------------------------------------------------------------------------
class _App:
    """HeadlessApp reale su un Database reale nella cartella del test."""

    def __init__(self, cartella, monkeypatch, impostazioni: Dict[str, Any] | None = None):
        import headless_main
        from database import Database

        percorso = str(cartella / "pickfair.db")
        monkeypatch.setattr(headless_main, "Database", lambda: Database(percorso))
        self.app = headless_main.HeadlessApp()
        self.app.build(start_services=False)
        self.app.telegram_service.start = lambda *a, **k: {"started": False, "stub": True}
        self.app.telegram_service.stop = lambda *a, **k: None
        base = {"settlement.poll_enabled": True}
        base.update(impostazioni or {})
        self.app.settings_service.save_settings(base)
        self.chiusure: List[dict] = []
        self.eventi: List[tuple] = []
        self.app.bus.subscribe("RUNTIME_CLOSE_POSITION", lambda p: self.chiusure.append(dict(p)))
        self.app.bus.subscribe("ROSERPINA_AUTO_RESET", lambda p: self.eventi.append(("ROSERPINA_AUTO_RESET", dict(p))))

    @property
    def rt(self) -> RuntimeController:
        return self.app.runtime

    @property
    def broker(self) -> SimulationBroker:
        return self.app.betfair_service.get_simulation_broker()

    def avvia(self) -> None:
        esito = self.rt.start(execution_mode="SIMULATION")
        assert esito.get("started") is True
        assert self.rt.simulation_mode is True
        assert self.rt.mode is RuntimeMode.ACTIVE

    def book(self, book: dict) -> None:
        """Ingresso reale dei book in SIM: tracker -> service -> broker."""
        self.rt.market_tracker.on_market_book(book)

    def giro(self) -> None:
        """Un giro del poller: la stessa funzione del thread, sotto il suo
        stesso lock (attende un giro del thread gia' in corso)."""
        with self.rt._settlement_poll_round_lock:
            self.rt._poll_cleared_settlements_locked(
                generation=self.rt._settlement_poll_generation
            )
        self.svuota_bus()

    def svuota_bus(self, timeout: float = 10.0) -> None:
        fatto = threading.Event()

        def _join():
            self.app.bus._queue.join()
            fatto.set()

        threading.Thread(target=_join, daemon=True).start()
        assert fatto.wait(timeout), "EventBus non drenato entro il timeout"

    def ferma(self) -> None:
        self.app.stop()


def _aperto(market_id: str, prezzi: Dict[int, tuple]) -> dict:
    return {
        "marketId": market_id,
        "status": "OPEN",
        "runners": [
            {
                "selectionId": sel,
                "status": "ACTIVE",
                "ex": {
                    "availableToBack": [{"price": back, "size": size}],
                    "availableToLay": [{"price": lay, "size": size}],
                },
            }
            for sel, (back, lay, size) in prezzi.items()
        ],
    }


def _chiuso(market_id: str, stati: Dict[int, str]) -> dict:
    return {
        "marketId": market_id,
        "status": "CLOSED",
        "runners": [{"selectionId": sel, "status": st} for sel, st in stati.items()],
    }


def _tre_selezioni(app: _App, market_id: str) -> None:
    """Calcolo indipendente per l'esito 11 WINNER, 22 LOSER, 33 LOSER:
    BACK 10 @ 3.0 = +20; LAY 5 @ 2.0 sul perdente = +5; BACK 4 @ 5.0 = -4.
    Lordo +21, commissione 4,5% una volta = 0,945, netto 20,055.
    Ai fill il saldo scende di 19; a settlement chiude a 1000 + 20,055."""
    app.book(_aperto(market_id, {11: (2.9, 3.0, 100.0), 22: (2.0, 2.1, 100.0), 33: (4.9, 5.0, 100.0)}))
    app.broker.place_bet(market_id=market_id, selection_id=11, side="BACK", price=3.0, size=10.0)
    app.broker.place_bet(market_id=market_id, selection_id=22, side="LAY", price=2.0, size=5.0)
    app.broker.place_bet(market_id=market_id, selection_id=33, side="BACK", price=5.0, size=4.0)


ESITO_TRE = {11: "WINNER", 22: "LOSER", 33: "LOSER"}
NETTO_TRE = 21.0 - 21.0 * COMMISSIONE / 100.0  # 20.055


def _chiave(app: _App, market_id: str) -> str:
    rec = [r for r in app.broker.list_settlements() if r["market_id"] == market_id]
    assert len(rec) == 1, f"atteso un settlement per {market_id}, trovati {rec}"
    return PnLEngine.simulated_settlement_key(market_id, rec[0]["settlement_id"])


@pytest.fixture
def app(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    istanza = _App(tmp_path, monkeypatch)
    yield istanza
    istanza.ferma()


# --------------------------------------------------------------------------
# PASS — la catena completa
# --------------------------------------------------------------------------
def test_catena_sim_dal_book_chiuso_al_ciclo(app, monkeypatch):
    """Book CHIUSO dall'ingresso reale => broker regola una volta => poller
    => PnLEngine => consumer: saldo, esposizione, realizzato, bankroll,
    daily-loss e checkpoint del ciclo coincidono col calcolo indipendente.
    L'helper che contava due volte non e' sul percorso: se venisse chiamato,
    il test esploderebbe."""
    monkeypatch.setattr(
        SimulationBroker, "record_realized_settlement",
        lambda *a, **k: pytest.fail("il percorso di produzione non deve usare l'helper"),
    )
    app.avvia()
    _tre_selezioni(app, "5.1")
    assert app.broker.get_account_funds()["available"] == pytest.approx(981.0)

    app.book(_chiuso("5.1", ESITO_TRE))
    app.giro()

    assert len(app.chiusure) == 1
    chiusura = app.chiusure[0]
    assert chiusura["gross_pnl"] == pytest.approx(21.0)
    assert chiusura["commission_amount"] == pytest.approx(0.945)
    assert chiusura["net_pnl"] == pytest.approx(NETTO_TRE)
    assert chiusura["settlement_source"] == "simulation_broker"

    fondi = app.broker.get_account_funds()
    assert fondi["available"] == pytest.approx(1000.0 + NETTO_TRE)
    assert fondi["exposure"] == pytest.approx(0.0)
    assert app.rt.risk_desk.realized_pnl == pytest.approx(NETTO_TRE)
    assert app.rt.risk_desk.bankroll_current == pytest.approx(1000.0 + NETTO_TRE)
    assert app.rt._daily_loss_monitor_state["realized_pnl"] == pytest.approx(NETTO_TRE)

    stato = app.rt.db.get_cycle_recovery_state(_chiave(app, "5.1"))
    assert stato["exists"] and stato["processed"] and stato["bankroll_synced"]
    assert not stato["ambiguous"]

    # Lo stato SIM persistito dal service contiene saldo e settlement.
    persistito = app.app.settings_service.load_simulation_state(state_key="default")
    assert persistito["balance"] == pytest.approx(1000.0 + NETTO_TRE)
    assert "5.1" in persistito["settlements"]


# --------------------------------------------------------------------------
# BLOCK — replay, duplicati, fuori ordine
# --------------------------------------------------------------------------
def test_replay_e_duplicati_non_cambiano_saldo_netto_ciclo(app):
    app.avvia()
    _tre_selezioni(app, "5.2")
    app.book(_chiuso("5.2", ESITO_TRE))
    app.giro()
    chiave = _chiave(app, "5.2")
    prima = (
        app.broker.get_account_funds()["available"],
        app.rt.risk_desk.realized_pnl,
        app.rt.db.get_cycle_recovery_checkpoint(chiave),
    )

    app.book(_chiuso("5.2", ESITO_TRE))  # book CHIUSO ripetuto dal feed
    app.giro()
    app.giro()

    assert len(app.chiusure) == 1
    dopo = (
        app.broker.get_account_funds()["available"],
        app.rt.risk_desk.realized_pnl,
        app.rt.db.get_cycle_recovery_checkpoint(chiave),
    )
    assert dopo[0] == pytest.approx(prima[0])
    assert dopo[1] == pytest.approx(prima[1])
    assert dopo[2]["checkpoint_stage"] == prima[2]["checkpoint_stage"]


def test_mercati_chiusi_fuori_ordine_ognuno_una_volta(app):
    """Il mercato B chiude prima di A. Calcolo indipendente: A come sopra
    (+20,055); B = BACK 6 @ 2.0 perdente = -6 (commissione 0). Totale
    realizzato 14,055; saldo 1014,055."""
    app.avvia()
    _tre_selezioni(app, "5.3")
    app.book(_aperto("5.4", {11: (1.9, 2.0, 100.0)}))
    app.broker.place_bet(market_id="5.4", selection_id=11, side="BACK", price=2.0, size=6.0)

    app.book(_chiuso("5.4", {11: "LOSER"}))
    app.giro()
    app.book(_chiuso("5.3", ESITO_TRE))
    app.giro()

    assert sorted(c["market_id"] for c in app.chiusure) == ["5.3", "5.4"]
    assert app.rt.risk_desk.realized_pnl == pytest.approx(NETTO_TRE - 6.0)
    assert app.broker.get_account_funds()["available"] == pytest.approx(1000.0 + NETTO_TRE - 6.0)
    assert app.broker.get_account_funds()["exposure"] == pytest.approx(0.0)


# --------------------------------------------------------------------------
# RECOVERY — riavvio fra persistenza e aggiornamento del ciclo
# --------------------------------------------------------------------------
def test_restart_fra_persistenza_e_aggiornamento_ciclo(tmp_path, monkeypatch):
    """Processo 1: il broker regola il mercato e il service persiste lo stato
    SIM, ma il runtime non aggiorna il ciclo (nessun giro del poller).
    Processo 2 sullo stesso DB: il THREAD del poller, al suo primo giro,
    consegna il settlement persistito una volta; il saldo SIM non cambia.
    Processo 3: nessuna riconsegna (checkpoint durevole)."""
    monkeypatch.chdir(tmp_path)

    p1 = _App(tmp_path, monkeypatch)
    p1.avvia()
    _tre_selezioni(p1, "6.1")
    p1.book(_chiuso("6.1", ESITO_TRE))
    saldo = p1.broker.get_account_funds()["available"]
    assert saldo == pytest.approx(1000.0 + NETTO_TRE)
    chiave = _chiave(p1, "6.1")
    assert p1.chiusure == []
    assert p1.rt.db.get_cycle_recovery_checkpoint(chiave) is None
    p1.ferma()

    p2 = _App(tmp_path, monkeypatch)
    try:
        p2.avvia()  # il thread del poller fa subito il primo giro
        fine = time.monotonic() + 10.0
        while not p2.chiusure and time.monotonic() < fine:
            time.sleep(0.02)
        p2.svuota_bus()
        p2.giro()  # un giro in piu' non riconsegna

        assert len(p2.chiusure) == 1
        assert p2.chiusure[0]["event_key"] == chiave
        assert p2.chiusure[0]["net_pnl"] == pytest.approx(NETTO_TRE)
        assert p2.broker.get_account_funds()["available"] == pytest.approx(saldo)
        assert p2.rt.risk_desk.realized_pnl == pytest.approx(NETTO_TRE)
        assert p2.rt.db.get_cycle_recovery_state(chiave)["processed"] is True
    finally:
        p2.ferma()

    p3 = _App(tmp_path, monkeypatch)
    try:
        p3.avvia()
        p3.giro()
        assert p3.chiusure == []
        assert p3.broker.get_account_funds()["available"] == pytest.approx(saldo)
    finally:
        p3.ferma()


# --------------------------------------------------------------------------
# PARITA' SIM/LIVE (H01) — stesso portafoglio, stesso netto
# --------------------------------------------------------------------------
class _BusSincrono:
    def __init__(self):
        self.eventi: List[tuple] = []

    def subscribe(self, *_a):
        return None

    def publish(self, topic, payload=None):
        self.eventi.append((topic, dict(payload or {})))


class _DbLive:
    def __init__(self, ordini):
        self.ordini = ordini

    def get_bot_active_orders(self):
        return list(self.ordini)

    def get_cycle_recovery_state(self, _chiave):
        return None

    def _execute(self, *_a, **_k):
        return None

    def _fetch_one(self, *_a, **_k):
        return None

    def _fetch_all(self, *_a, **_k):
        return []


class _ServizioLive:
    """Adapter LIVE con risposte controllate: le righe cleared che Betfair
    restituirebbe per il portafoglio, una per bet, stesso settledDate."""

    def __init__(self, righe):
        self.righe = righe

    def set_simulation_mode(self, _flag):
        return None

    def list_cleared_orders(self, **_kwargs):
        return [dict(r) for r in self.righe]

    def get_account_funds(self):
        return {"available": 1000.0}


class _ImpostazioniLive:
    def load_roserpina_config(self):
        return RoserpinaConfig()

    def load_market_data_config(self):
        return {"market_data_mode": "poll", "enabled": False, "market_ids": []}

    def get_all_settings(self):
        return {}


class _Telegram:
    def start(self):
        return {}

    def stop(self):
        return None


def test_parita_sim_live_stesso_portafoglio_stesso_netto(app):
    """H01: stesso portafoglio BACK/LAY con vincita, perdita e match
    parziale. LIVE: le righe cleared per-bet passano dal poller LIVE vero.
    SIM: il mercato chiuso passa dal percorso SIM vero. Il netto di mercato
    coincide fra i due e col calcolo indipendente."""
    # SIM
    app.avvia()
    app.book(_aperto("7.1", {11: (2.9, 3.0, 100.0), 22: (2.0, 2.1, 100.0), 33: (4.9, 5.0, 2.0)}))
    app.broker.place_bet(market_id="7.1", selection_id=11, side="BACK", price=3.0, size=10.0)
    app.broker.place_bet(market_id="7.1", selection_id=22, side="LAY", price=2.0, size=5.0)
    app.broker.place_bet(market_id="7.1", selection_id=33, side="BACK", price=5.0, size=4.0)  # 2 abbinati
    app.book(_chiuso("7.1", ESITO_TRE))
    app.giro()
    netto_sim = sum(c["net_pnl"] for c in app.chiusure if c["market_id"] == "7.1")

    # Calcolo indipendente: +20 + 5 - 2 = +23; commissione 1,035; netto 21,965.
    atteso = 23.0 - 23.0 * COMMISSIONE / 100.0
    assert netto_sim == pytest.approx(atteso)

    # LIVE: stesse bet, profitti per-bet come li riporterebbe Betfair.
    righe = [
        {"betId": "901", "marketId": "7.1", "profit": 20.0, "settledDate": "2026-09-28T20:00:00Z"},
        {"betId": "902", "marketId": "7.1", "profit": 5.0, "settledDate": "2026-09-28T20:00:00Z"},
        {"betId": "903", "marketId": "7.1", "profit": -2.0, "settledDate": "2026-09-28T20:00:00Z"},
    ]
    live = RuntimeController(
        bus=_BusSincrono(),
        db=_DbLive([{"bet_id": r["betId"], "market_id": "7.1"} for r in righe]),
        settings_service=_ImpostazioniLive(),
        betfair_service=_ServizioLive(righe),
        telegram_service=_Telegram(),
    )
    live.mode = RuntimeMode.ACTIVE
    live.simulation_mode = False
    live._settlement_poll_cfg = {"enabled": True, "poll_sec": 60.0, "lookback_hours": 24.0}
    live._settlement_sweep_done_generation = live._settlement_poll_generation
    live._poll_cleared_settlements()
    chiusure_live = [p for t, p in live.bus.eventi if t == "RUNTIME_CLOSE_POSITION"]
    for payload in chiusure_live:
        contratto = RuntimeController._extract_settlement_contract(payload)
        assert contratto["settlement_validation"] == "accepted", contratto["reason"]

    assert len(chiusure_live) == 3
    assert sum(p["net_pnl"] for p in chiusure_live) == pytest.approx(netto_sim)


# --------------------------------------------------------------------------
# CICLO ROSERPINA — la perdita SIM arriva al kill-switch e al reset
# --------------------------------------------------------------------------
def test_perdita_sim_arriva_al_kill_switch_giornaliero(tmp_path, monkeypatch):
    """Limite owner: perdita giornaliera 10 EUR. Calcolo indipendente:
    BACK 15 @ 2.0 perdente = -15 >= 10 => hard-stop."""
    monkeypatch.chdir(tmp_path)
    istanza = _App(tmp_path, monkeypatch, {"roserpina.max_daily_loss": 10.0})
    try:
        istanza.avvia()
        istanza.book(_aperto("8.1", {11: (1.9, 2.0, 100.0)}))
        istanza.broker.place_bet(market_id="8.1", selection_id=11, side="BACK", price=2.0, size=15.0)
        istanza.book(_chiuso("8.1", {11: "LOSER"}))
        istanza.giro()

        assert len(istanza.chiusure) == 1
        assert istanza.rt._daily_loss_monitor_state["daily_loss_amount"] == pytest.approx(15.0)
        assert istanza.rt._daily_loss_monitor_state["breached"] is True
        assert istanza.rt._emergency_stopped is True
    finally:
        istanza.ferma()


def test_drawdown_da_settlement_sim_resetta_il_ciclo_roserpina(app):
    """Soglia di auto-reset Roserpina di default 15%. Calcolo indipendente:
    BACK 160 @ 2.0 perdente = -160 su 1000 => drawdown 16% => reset del
    ciclo (ROSERPINA_AUTO_RESET), sotto il 20% del lockdown."""
    app.avvia()
    app.book(_aperto("9.1", {11: (1.9, 2.0, 1000.0)}))
    app.broker.place_bet(market_id="9.1", selection_id=11, side="BACK", price=2.0, size=160.0)
    app.book(_chiuso("9.1", {11: "LOSER"}))
    app.giro()

    assert len(app.chiusure) == 1
    assert app.broker.get_account_funds()["available"] == pytest.approx(840.0)
    reset = [p for t, p in app.eventi if t == "ROSERPINA_AUTO_RESET"]
    assert len(reset) == 1
    assert reset[0]["drawdown_pct"] == pytest.approx(16.0)
    assert app.rt.risk_desk.bankroll_start == pytest.approx(840.0)
    assert app.rt.risk_desk.realized_pnl == pytest.approx(0.0)
    assert app.rt.mode is not RuntimeMode.LOCKDOWN
