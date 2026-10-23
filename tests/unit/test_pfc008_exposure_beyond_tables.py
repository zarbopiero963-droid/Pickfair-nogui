"""PFC-008, controllo pre-merge owner su dd0b02b (#547): esposizione oltre i tavoli e hedge protettivo.

Rilievo Codex P1: sottrarre dall'impegnato della lettura TUTTA l'esposizione dei tavoli fa "assorbire"
l'impegnato esterno dalle riserve del bot non ancora a mercato (cassa 100, esterno 20, riserva pendente 10,
cap 35%: venivano ammessi 15 invece di 5). Ora il cap totale vede il maggiore fra tavoli adesso e impegnato
della lettura + riserve non riflesse in quella lettura (`pending_exposure(since=read_at)`), sui percorsi reali
(segnale BACK/LAY, auto-next, Dutching precheck e manual_bet) con TableManager e Money Management veri.

Hedge (#548, opzione B confermata dall'owner): dal cashout (`OrderRouter`) il flag `protective` arriva fino al
cap temporaneo e l'hedge resta nei fondi disponibili, mentre un ingresso nelle stesse condizioni e' bloccato.
"""
from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

from controllers.dutching_controller import DutchingController
from core import validators
from core.order_router import OrderRouter
from core.risk_desk import RiskDesk
from core.system_state import RuntimeMode
from core.table_manager import TableManager
from tests.failure.test_dutching_controller_failures import FakeRuntime
from tests.integration.test_runtime_controller_cycle_executor import _make_controller
from tests.risk_config_fixtures import with_explicit_risk_pct


def _pct(cfg):
    return with_explicit_risk_pct(cfg, max_total_exposure_pct=35.0, max_single_bet_pct=100.0,
                                  max_event_exposure_pct=100.0, min_stake=0.01)


# --- runtime, segnale BACK e LAY: l'esempio Codex sul percorso reale ---------------------------------------------

def _rc_codex(*, pendente=10.0, placed_before_read=False):
    rc, bus = _make_controller(responses=[])
    _pct(rc.mm.config)
    rc.config.anti_duplication_enabled = False
    rc.table_manager.activate(table_id=2, event_key="evt-pendente", exposure=pendente, market_id="1.9",
                              selection_id=9)
    if placed_before_read:
        rc.table_manager.mark_placed(2, "evt-pendente", at=time.monotonic() - 1.0)
    lettura = time.monotonic()
    # Cassa 100 in entrambi i casi: se la riserva e' gia' a mercato, l'exchange la riflette nell'impegnato.
    gia = pendente if placed_before_read else 0.0
    rc._applica_fondi_avvio({"available": 80.0 - gia, "exposure": -20.0 - gia}, read_at=lettura)
    rc.mode = RuntimeMode.ACTIVE
    decisioni = []
    calcola = rc.mm.calculate

    def _spia(**kwargs):
        d = calcola(**kwargs)
        decisioni.append(d)
        return d

    rc.mm.calculate = _spia
    rc._reject_signal = lambda _s, _motivo: None
    return rc, decisioni


def _segnale(side, stake, price=3.0):
    return {"market_id": "1.1", "selection_id": 1, "price": price, "odds": price, "side": side, "bet_type": side,
            "stake": stake, "event_name": "A v B", "event_id": "E1", "simulation_mode": True}


@pytest.mark.parametrize("side,stake,price,ammesso", [
    ("BACK", 15.0, 3.0, False),     # 10 tavoli + 20 esterni + 15 = 45 > 35 (prima di questa fix: ammesso)
    ("BACK", 5.0, 3.0, True),       # 10 + 20 + 5 = 35 <= 35
    ("LAY", 7.5, 3.0, False),       # liability 15
    ("LAY", 2.5, 3.0, True),        # liability 5
])
def test_runtime_segnale_riserva_pendente_e_impegnato_esterno(side, stake, price, ammesso):
    rc, decisioni = _rc_codex()
    rc._on_signal_received_serial(_segnale(side, stake, price))
    assert decisioni, "il segnale deve arrivare al Money Management"
    d = decisioni[-1]
    assert d.approved is ammesso, d
    if not ammesso:
        assert d.reason == "supera_max_total_exposure"


@pytest.mark.parametrize("stake,ammesso", [(5.0, True), (5.5, False)])
def test_runtime_riserva_piazzata_prima_della_lettura_non_conta_due_volte(stake, ammesso):
    # La riserva era gia' a mercato prima della lettura: l'impegnato letto (30) la comprende gia'.
    # 10 tavoli + 20 oltre (30 - 10) + 5 = 35 <= 35, non 45 (nessun doppio conteggio).
    rc, decisioni = _rc_codex(placed_before_read=True)
    rc._on_signal_received_serial(_segnale("BACK", stake))
    assert decisioni[-1].approved is ammesso, decisioni[-1]


# --- ACK durante la lettura ----------------------------------------------------------------------------------------

def test_ack_durante_la_lettura_resta_pendente_prudente():
    tm = TableManager(table_count=2)
    tm.activate(table_id=1, event_key="E1", exposure=10.0, market_id="1.1", selection_id=1)
    desk = RiskDesk()
    desk.sync_bankroll(80.0)
    lettura = 100.0
    tm.mark_placed(1, "E1", at=lettura + 0.001)                   # ACK arrivato a lettura gia' iniziata
    desk.sync_cash_equity(100.0, 80.0, committed_exposure=20.0, read_at=lettura)
    # Non si sa se la lettura la contiene: conta come pendente (mai sotto-stima), 20 + 10 = 30.
    assert validators.desk_cash_basis(desk, 80.0, tm) == (100.0, 20.0)


def test_lettura_concorrente_piu_vecchia_non_altera_impegnato():
    desk = RiskDesk()
    desk.sync_bankroll(80.0)
    desk.sync_cash_equity(100.0, 80.0, committed_exposure=20.0, read_at=2.0)
    desk.sync_cash_equity(100.0, 80.0, committed_exposure=0.0, read_at=1.0)     # arrivata tardi: ignorata
    assert desk.cash_basis() == (100.0, 20.0, 2.0)


@pytest.mark.parametrize("tm", [
    SimpleNamespace(total_exposure=lambda: float("nan"), pending_exposure=lambda since: 0.0),
    SimpleNamespace(total_exposure=lambda: 0.0, pending_exposure=lambda since: float("inf")),
    SimpleNamespace(total_exposure=lambda: (_ for _ in ()).throw(RuntimeError("x"))),
])
def test_tavoli_illeggibili_fail_closed(tm):
    desk = RiskDesk()
    desk.sync_bankroll(80.0)
    desk.sync_cash_equity(100.0, 80.0, committed_exposure=20.0, read_at=1.0)
    assert validators.desk_cash_basis(desk, 80.0, tm) == (None, None)


# --- auto-next post-settlement --------------------------------------------------------------------------------------

@pytest.mark.parametrize("stake,ammesso", [(15.0, False), (4.0, True)])
def test_auto_next_riserva_pendente_e_impegnato_esterno(stake, ammesso):
    from tests.acceptance.test_issue461_pr28e_funds_gate import _auto_next
    rc, bus, esito = _auto_next(
        {"market_id": "1.2", "selection_id": 2, "price": 2.0, "stake": stake, "bet_type": "BACK"},
        funds={"available": 80.0, "exposure": 20.0}, riservato=10.0, pct_totale=35.0)
    inviati = [e for e in bus.events if e[0] == "CMD_QUICK_BET"]
    if ammesso:
        assert inviati, esito                                     # 10 + 20 + 4 = 34 <= 35
    else:
        assert inviati == [] and esito["reason"] == "supera_max_total_exposure", esito


# --- Dutching precheck e manual_bet ---------------------------------------------------------------------------------

def _dutching_runtime():
    runtime = FakeRuntime()
    runtime.config.max_total_exposure_pct = 35.0
    tm = TableManager(table_count=3)
    tm.activate(table_id=3, event_key="evt-pendente", exposure=10.0, market_id="1.9", selection_id=9)
    runtime.table_manager = tm
    desk = RiskDesk()
    desk.sync_bankroll(80.0)
    desk.sync_cash_equity(100.0, 80.0, committed_exposure=20.0, read_at=time.monotonic())
    runtime.risk_desk = desk
    return runtime


@pytest.mark.parametrize("legs,ammesso", [([7.5, 7.5], False), ([2.5, 2.5], True)])
def test_dutching_precheck_riserva_pendente_e_impegnato_esterno(monkeypatch, legs, ammesso):
    def fake_calculate_dutching(selections, total_stake):
        _ = selections, total_stake
        return ([{"selectionId": i + 1, "price": 3.0, "stake": s, "side": "BACK"} for i, s in enumerate(legs)],
                1.0, 90.0)
    monkeypatch.setattr("controllers.dutching_controller.calculate_dutching", fake_calculate_dutching)
    ctrl = DutchingController(bus=SimpleNamespace(publish=lambda *a, **k: None), runtime_controller=_dutching_runtime())
    out = ctrl.precheck({"market_id": "1.300", "total_stake": sum(legs), "simulation_mode": True,
                         "selections": [{"selectionId": i + 1, "price": 3.0, "side": "BACK"}
                                        for i in range(len(legs))]})
    if ammesso:
        assert out["ok"] is True, out
    else:
        assert out["ok"] is False and out["error"] == "Esposizione globale oltre limite", out


@pytest.mark.parametrize("stake,ammesso", [(15.0, False), (5.0, True)])
def test_manual_bet_riserva_pendente_e_impegnato_esterno(stake, ammesso):
    runtime = _dutching_runtime()
    ctrl = DutchingController(bus=SimpleNamespace(publish=lambda *a, **k: None), runtime_controller=runtime)
    out = ctrl.manual_bet({"market_id": "1.1", "selection_id": 1, "price": 3.0, "stake": stake, "side": "BACK",
                           "bet_type": "BACK", "event_name": "A v B", "simulation_mode": True})
    if ammesso:
        assert out.get("error") != "Esposizione globale oltre limite", out
    else:
        assert out["ok"] is False and out["error"] == "Esposizione globale oltre limite", out


# --- hedge dal cashout fino al cap temporaneo (end-to-end) -----------------------------------------------------------

def _engine_al_cap(*, cassa_verificata):
    from tests.test_microstake_engine_native import _engine
    engine, broker, db = _engine(bankroll=100.0, open_exposure=35.0)     # esposizione al cap 35% di 100
    desk = RiskDesk()
    desk.sync_bankroll(100.0)
    if cassa_verificata:
        desk.sync_cash_equity(100.0, 100.0, committed_exposure=35.0)
    engine.runtime_controller.risk_desk = desk
    return engine, broker


def _hedge(ref):
    return {"market_id": "1.100", "selection_id": 7, "bet_type": "BACK", "price": 2.0, "stake": 0.10,
            "customer_ref": ref}


@pytest.mark.parametrize("cassa_verificata", [True, False])
def test_hedge_dal_cashout_passa_mentre_l_ingresso_e_bloccato(cassa_verificata):
    from tests.test_microstake_engine_native import _orders, _payload
    engine, broker = _engine_al_cap(cassa_verificata=cassa_verificata)
    ingresso = engine.submit_quick_bet(_payload("entry", size=0.10, price=2.0))
    assert ingresso["ok"] is False and _orders(broker) == [], ingresso
    router = OrderRouter(betfair_service=None, microstake_executor_provider=engine.native_microstake_executor)
    esito = router.place(_hedge("cashout-1"))
    # Esito nativo: placed=True/SUCCESS (ok del router = placed e abbinato; l'ordine economico resta a riposo).
    assert esito["placed"] is True and esito["status"] == "SUCCESS" and esito["order_unknown"] is False, esito
    assert len(_orders(broker)) == 1


def test_hedge_resta_nei_fondi_disponibili():
    # Opzione B con vincolo owner: fondi disponibili verificati. Con disponibile 0 nemmeno l'hedge passa.
    from tests.test_microstake_engine_native import _engine, _orders
    engine, broker, _ = _engine(bankroll=100.0, available=0.0, open_exposure=35.0)
    router = OrderRouter(betfair_service=None, microstake_executor_provider=engine.native_microstake_executor)
    esito = router.place(_hedge("cashout-2"))
    assert esito["placed"] is False and _orders(broker) == [], esito


def test_protective_solo_dalla_catena_del_cashout():
    # Identita' e natura protettiva: l'OrderRouter (unico che marca `protective`) e' costruito solo dalla catena
    # del cashout, che calcola l'hedge opposto a una posizione esistente. Un nuovo costruttore va valutato.
    import pathlib
    import re
    root = pathlib.Path(__file__).resolve().parents[2]
    costruttori = sorted(
        str(p.relative_to(root)) for p in root.rglob("*.py")
        if "tests" not in p.parts and ".git" not in p.parts and "venv" not in p.parts
        and re.search(r"\bOrderRouter\(", p.read_text(encoding="utf-8", errors="ignore")))
    assert costruttori == ["cashout_wiring.py"], costruttori
    sorgente = (root / "core" / "order_router.py").read_text(encoding="utf-8")
    assert sorgente.count('"protective": True') == 1


# --- lotto Dutching con ACK parziale (review indipendente, S4) -------------------------------------------------------

def _lotto():
    tm = TableManager(table_count=2)
    tm.activate(table_id=1, event_key="lotto", exposure=30.0, market_id="1.1", selection_id=0,
                meta={"type": "dutching_batch", "legs_total": 3})
    return tm


def test_lotto_a_mercato_solo_con_tutte_le_gambe():
    tm = _lotto()
    assert tm.mark_placed(1, "lotto", at=10.0, customer_ref="g1") is False
    assert tm.mark_placed(1, "lotto", at=11.0, customer_ref="g1") is False      # stesso esito ripetuto
    assert tm.mark_placed(1, "lotto", at=12.0, customer_ref="") is False        # esito senza ref: prudente
    assert tm.mark_placed(1, "lotto", at=13.0, customer_ref="g2") is False
    assert tm.pending_exposure(since=100.0, now=100.0) == 30.0                 # ancora tutto pendente
    assert tm.mark_placed(1, "lotto", at=14.0, customer_ref="g3") is True
    assert tm.pending_exposure(since=100.0, now=100.0) == 0.0


def test_lotto_con_ack_parziale_non_assorbe_l_esterno():
    # Lotto 3 x 10, esterno 20, gamba 1 a mercato prima della lettura, gambe 2 e 3 ancora in volo.
    # Prima: cap vedeva 30 (esterno assorbito). Ora almeno 50 (stima prudente 60: la gamba 1 e' gia' letta).
    tm = _lotto()
    tm.mark_placed(1, "lotto", at=50.0, customer_ref="g1")
    desk = RiskDesk()
    desk.sync_bankroll(70.0)
    desk.sync_cash_equity(100.0, 70.0, committed_exposure=30.0, read_at=100.0)
    base, oltre = validators.desk_cash_basis(desk, 70.0, tm)
    assert base == 100.0 and tm.total_exposure() + oltre >= 50.0


# --- race rilascio fra le letture (rilievo Sol su 7925fa7) ---------------------------------------------------------

class _TavoliCheRilasciano:
    """Riserva da 10 (pendente) rilasciata mentre si leggono le riserve pendenti."""

    def __init__(self):
        self.totale = 10.0

    def total_exposure(self):
        return self.totale

    def pending_exposure(self, *, since):
        self.totale = 0.0                       # rilascio concorrente: la riserva sparisce
        return 0.0


def test_rilascio_fra_le_letture_non_sottostima():
    # Esterno 20 (cassa 100). Il chiamante legge i tavoli (10), poi la riserva viene rilasciata.
    # Il cap deve vedere almeno l'esterno vero (20): totale letto dal chiamante + parte oltre i tavoli.
    desk = RiskDesk()
    desk.sync_bankroll(80.0)
    desk.sync_cash_equity(100.0, 80.0, committed_exposure=20.0, read_at=1.0)
    tm = _TavoliCheRilasciano()
    tavoli_letti = tm.total_exposure()
    base, oltre = validators.desk_cash_basis(desk, 80.0, tm, tables_total=tavoli_letti)
    assert base == 100.0 and tavoli_letti + oltre >= 20.0


def test_rilascio_prima_della_lettura_del_chiamante_resta_prudente():
    # Ordine opposto: riserve pendenti lette (10) prima del rilascio, tavoli letti dopo (0): sovrastima prudente.
    desk = RiskDesk()
    desk.sync_bankroll(80.0)
    desk.sync_cash_equity(100.0, 80.0, committed_exposure=20.0, read_at=1.0)
    tm = SimpleNamespace(total_exposure=lambda: 0.0, pending_exposure=lambda since: 10.0)
    base, oltre = validators.desk_cash_basis(desk, 80.0, tm, tables_total=0.0)
    assert (base, oltre) == (100.0, 30.0)


def test_riserva_nuova_fra_le_letture_vista_per_intero():
    # Il chiamante legge i tavoli (0); subito dopo compare una riserva pendente da 10. Esterno 20.
    # Il cap deve vedere 30 (0 letto + 30 oltre), non 20 (che darebbe una rilettura interna dei tavoli).
    desk = RiskDesk()
    desk.sync_bankroll(80.0)
    desk.sync_cash_equity(100.0, 80.0, committed_exposure=20.0, read_at=1.0)
    tm = SimpleNamespace(total_exposure=lambda: 10.0, pending_exposure=lambda since: 10.0)
    base, oltre = validators.desk_cash_basis(desk, 80.0, tm, tables_total=0.0)
    assert base == 100.0 and 0.0 + oltre == 30.0


def test_lotto_reale_a_mercato_dopo_l_ultima_gamba(tmp_path):
    # Catena reale DutchingController -> RuntimeController -> TradingEngine -> SimulationBroker: gli esiti delle
    # gambe portano customer_ref/table_id/event_key della gamba; il tavolo del lotto (senza ref proprio) e' a
    # mercato solo dopo l'ultima. Una gamba senza esito lascia il lotto pendente: sovrastima prudente.
    from tests.acceptance.test_issue523_dutching_manual_bet import _batch, _busy_tables, _catena, _dutching, _leg_event
    c = _catena(tmp_path)
    publish = c.bus.publish            # esiti delle gambe pilotati dal test: niente ACK sincrono dell'engine
    c.bus.publish = lambda topic, payload=None: None if topic == "QUICK_BET_SUBMIT_ACK" else publish(topic, payload)
    assert _dutching(c).submit_dutching(_batch(sides=("BACK", "BACK", "BACK"), total_stake=15.0))["ok"] is True
    legs = c.bus.payloads("CMD_QUICK_BET")
    tavolo = _busy_tables(c)[0]
    for leg in legs[:2]:
        _leg_event(c, leg, "QUICK_BET_SUCCESS")
        assert "placed_at" not in tavolo.meta
    _leg_event(c, legs[0], "QUICK_BET_SUCCESS")                 # esito ripetuto della stessa gamba
    assert "placed_at" not in tavolo.meta
    _leg_event(c, legs[2], "QUICK_BET_SUCCESS")
    assert "placed_at" in tavolo.meta


def test_lotto_reale_a_mercato_con_l_ack_sincrono_di_ogni_gamba(tmp_path):
    # Senza esiti pilotati: l'ACK sincrono del TradingEngine (QUICK_BET_SUBMIT_ACK) di ogni gamba accettata dal
    # broker porta il lotto a mercato, una volta per gamba distinta.
    from tests.acceptance.test_issue523_dutching_manual_bet import _batch, _busy_tables, _catena, _dutching
    c = _catena(tmp_path)
    assert _dutching(c).submit_dutching(_batch(sides=("BACK", "BACK", "BACK"), total_stake=15.0))["ok"] is True
    acks = c.bus.payloads("QUICK_BET_SUBMIT_ACK")
    assert len({a["customer_ref"] for a in acks}) == 3
    assert "placed_at" in _busy_tables(c)[0].meta


# --- lettura fresca d'ingresso (rilievo Codex P1 su 45c9aa2) --------------------------------------------------------

def _rc_lettura_fresca(fresca):
    """Cassa registrata 100 con 20 impegnati; la lettura fresca d'ingresso restituisce `fresca`."""
    rc, bus = _make_controller(responses=[fresca])
    _pct(rc.mm.config)
    rc.config.anti_duplication_enabled = False
    rc._applica_fondi_avvio({"available": 80.0, "exposure": -20.0}, read_at=time.monotonic())
    rc.mode = RuntimeMode.ACTIVE
    rifiuti = []
    rc._reject_signal = lambda _s, motivo: rifiuti.append(motivo)
    return rc, bus, rifiuti


def _ordini(bus):
    return [t for t, _p in bus.events if t == "CMD_QUICK_BET"]


@pytest.mark.parametrize("side,stake,ammesso", [
    ("BACK", 15.0, False),      # fresca: 40 impegnati + 15 = 55 > 35 (prima: CMD_QUICK_BET pubblicato)
    ("LAY", 7.5, False),        # liability 15, idem
    ("BACK", 4.0, False),       # 40 + 4 = 44 > 35: il cap e' gia' superato dall'esterno
])
def test_lettura_fresca_con_impegnato_cresciuto_ricontrolla_il_cap(side, stake, ammesso):
    rc, bus, rifiuti = _rc_lettura_fresca({"available": 60.0, "exposure": -40.0})
    rc._on_signal_received_serial(_segnale(side, stake))
    assert not _ordini(bus)
    assert "supera_max_total_exposure" in rifiuti, rifiuti


def test_lettura_fresca_coerente_ammette_entro_il_cap():
    # Lettura fresca identica alla registrata: 20 + 15 = 35 <= 35 => l'ordine parte.
    rc, bus, rifiuti = _rc_lettura_fresca({"available": 80.0, "exposure": -20.0})
    rc._on_signal_received_serial(_segnale("BACK", 15.0))
    assert _ordini(bus), rifiuti
    assert not rifiuti


def test_lettura_fresca_cassa_scesa_riduce_il_limite():
    # Prelievo: cassa fresca 60 (40 + 20), cap 21; 20 + 15 = 35 > 21 => rifiuto.
    rc, bus, rifiuti = _rc_lettura_fresca({"available": 40.0, "exposure": -20.0})
    rc._on_signal_received_serial(_segnale("BACK", 15.0))
    assert not _ordini(bus)
    assert "supera_max_total_exposure" in rifiuti, rifiuti


@pytest.mark.parametrize("fresca,motivo", [
    ({"available": 80.0, "exposure": -20.0, "simulated": True, "total": 150.0}, "CASH_TOTAL_MISMATCH"),
    ({"available": 80.0, "exposure": -20.0, "simulated": True, "total": 100.0, "reserved_unmatched": 50.0},
     "CASH_RESERVE_INCOHERENT"),
])
def test_lettura_fresca_cassa_non_verificabile_blocca(fresca, motivo):
    # I fondi bastano (gate fondi superato) ma la lettura fresca non e' una cassa verificata: nessun ordine.
    rc, bus, rifiuti = _rc_lettura_fresca(fresca)
    rc._on_signal_received_serial(_segnale("BACK", 5.0))
    assert not _ordini(bus)
    assert rifiuti == ["cassa_non_verificata:" + motivo], rifiuti


# --- ACK sincrono del TradingEngine (SUBMITTED) -----------------------------------------------------------------------

def test_ack_sincrono_marca_la_riserva_a_mercato_senza_rilasciarla(tmp_path):
    # Senza ACK l'ordine accettato dal broker restava "pendente" per sempre sul percorso SIM del TradingEngine: una
    # lettura successiva lo contava nell'impegnato E fra le riserve pendenti (doppio conteggio nel cap totale).
    from tests.acceptance.test_issue437_pr27 import _tavoli_occupati
    from tests.integration.test_issue461_pr26a_customer_ref_provenance import _chain, _signal
    c = _chain(tmp_path, stake=4.0, anti_duplication=True)
    c.rc.betfair_service.get_account_funds = lambda: {"available": 100.0, "exposure": 0.0}
    c.rc._on_signal_received(_signal())
    (cmd,) = c.bus.payloads("CMD_QUICK_BET")
    (ack,) = c.bus.payloads("QUICK_BET_SUBMIT_ACK")
    assert (ack["table_id"], ack["event_key"], ack["customer_ref"]) == (
        cmd["table_id"], cmd["event_key"], cmd["customer_ref"])
    (tavolo,) = _tavoli_occupati(c)                         # nessun rilascio: chiude solo l'incertezza "a mercato"
    assert "placed_at" in c.rc.table_manager.get_table(cmd["table_id"]).meta
    assert c.rc.table_manager.pending_exposure(since=time.monotonic()) == 0.0
    assert c.rc.table_manager.total_exposure() == 4.0


# --- lettura fresca: singolo ordine ed evento (rilievo Codex P1 su 4ce44a7) -----------------------------------------

def _rc_tre_cap(fresca):
    """Cassa registrata 100 (80 + 20); cap 18% singolo ed evento, 35% totale; lettura fresca `fresca`."""
    rc, bus = _make_controller(responses=[fresca])
    with_explicit_risk_pct(rc.mm.config, max_total_exposure_pct=35.0, max_single_bet_pct=18.0,
                           max_event_exposure_pct=18.0, min_stake=0.01)
    rc.config.anti_duplication_enabled = False
    rc._applica_fondi_avvio({"available": 80.0, "exposure": -20.0}, read_at=time.monotonic())
    rc.mode = RuntimeMode.ACTIVE
    rifiuti = []
    rc._reject_signal = lambda _s, motivo: rifiuti.append(motivo)
    return rc, bus, rifiuti


@pytest.mark.parametrize("side,stake", [("BACK", 15.0), ("LAY", 7.5)])      # esposizione 15
def test_lettura_fresca_cassa_scesa_ricontrolla_singolo_ordine(side, stake):
    # Esempio Codex: 15 <= 18% di 100 (registrata), ma > 18% di 50 (fresca, 50 + 0) = 9. Il totale fresco regge
    # (15 <= 17.5): senza il ricontrollo del singolo ordine partiva CMD_QUICK_BET.
    rc, bus, rifiuti = _rc_tre_cap({"available": 50.0, "exposure": 0.0})
    rc._on_signal_received_serial(_segnale(side, stake))
    assert not _ordini(bus)
    assert rifiuti == ["supera_max_single_bet"], rifiuti


def test_lettura_fresca_cassa_scesa_ricontrolla_l_evento():
    # Stesso evento gia' esposto per 5 (tavolo del bot). Nuovo BACK 6: singolo 6 <= 9 (18% di 50), totale
    # 5 + 6 = 11 <= 17.5, evento 5 + 6 = 11 > 9 => rifiutato sulla cassa fresca (sulla registrata 11 <= 18).
    rc, bus, rifiuti = _rc_tre_cap({"available": 45.0, "exposure": -5.0})
    rc.table_manager.activate(table_id=2, event_key="evt-stesso", exposure=5.0, market_id="1.2", selection_id=2,
                              meta={"event_id": "E1", "event_name": "A v B", "event_identity": "id:E1"})
    rc.table_manager.mark_placed(2, "evt-stesso", at=time.monotonic() - 1.0)
    rc._on_signal_received_serial(_segnale("BACK", 6.0))
    assert not _ordini(bus)
    assert rifiuti == ["supera_max_event_exposure"], rifiuti


def test_lettura_fresca_tre_cap_entro_i_limiti_ammette():
    rc, bus, rifiuti = _rc_tre_cap({"available": 80.0, "exposure": -20.0})
    rc._on_signal_received_serial(_segnale("BACK", 9.0))
    assert _ordini(bus) and not rifiuti, rifiuti


# --- letture fuori ordine: bankroll e cassa insieme (rilievo Codex P2 su 4ce44a7) ------------------------------------

def test_lettura_vecchia_arrivata_tardi_non_tocca_bankroll_ne_cassa():
    # Lettura piu' recente applicata (60 + 40); una piu' vecchia (80 + 20) finisce dopo: prima riscriveva il
    # bankroll a 80 e invalidava la cassa (CASH_NOT_READ) senza poterla ripristinare.
    rc, _bus = _make_controller(responses=[])
    rc._applica_fondi_avvio({"available": 60.0, "exposure": -40.0}, read_at=10.0)
    vecchia = 5.0
    rc.betfair_service.get_account_funds = lambda: {"available": 80.0, "exposure": -20.0}
    clock = iter([vecchia] + [vecchia + 0.01] * 50)
    import core.runtime_controller as modulo
    originale = modulo.time.monotonic
    modulo.time.monotonic = lambda: next(clock)
    try:
        rc.get_status()
    finally:
        modulo.time.monotonic = originale
    assert rc.risk_desk.bankroll_current == 60.0
    assert rc.risk_desk.cash_basis() == (100.0, 40.0, 10.0)
    assert rc.risk_desk.cash_equity_reason == ""


def test_sync_funds_reading_atomico_ignora_la_lettura_vecchia():
    desk = RiskDesk()
    assert desk.sync_funds_reading(60.0, 100.0, 60.0, committed_exposure=40.0, read_at=2.0) is True
    assert desk.sync_funds_reading(80.0, 100.0, 80.0, committed_exposure=20.0, read_at=1.0) is False
    assert desk.bankroll_current == 60.0 and desk.cash_basis() == (100.0, 40.0, 2.0)
    assert desk.sync_funds_reading(None, 100.0, 60.0, committed_exposure=40.0, read_at=3.0) is True
    assert desk.cash_basis() == (100.0, 40.0, 3.0)


# --- rilievi Grok su 4ce44a7: riserva del bot pendente e ACK durante la lettura fresca ------------------------------

def _rc_con_riserva_bot(fresca_fn):
    # Lettura registrata 100 + 0: il MM ammette (tavoli 20 + 1 <= 35). L'impegnato esterno 20 compare solo nella
    # lettura fresca d'ingresso, quindi il rifiuto, se c'e', viene dal ricontrollo fresco.
    rc, bus = _make_controller(responses=[])
    _pct(rc.mm.config)
    rc.config.anti_duplication_enabled = False
    rc._applica_fondi_avvio({"available": 100.0, "exposure": 0.0}, read_at=time.monotonic())
    rc.mode = RuntimeMode.ACTIVE
    rifiuti = []
    rc._reject_signal = lambda _s, motivo: rifiuti.append(motivo)
    rc.table_manager.activate(table_id=2, event_key="evt-bot", exposure=20.0, market_id="1.9", selection_id=9)
    rc.betfair_service.get_account_funds = fresca_fn(rc)
    return rc, bus, rifiuti


def test_lettura_fresca_conta_la_riserva_del_bot_non_ancora_a_mercato():
    # Cassa 100, impegnato esterno 20 nella lettura fresca, riserva del bot 20 non a mercato, cap 35%: la riserva
    # e' fra le pendenti (placed_at assente) e si somma all'impegnato: 20 + 20 + 1 = 41 > 35 => rifiuto.
    rc, bus, rifiuti = _rc_con_riserva_bot(lambda _rc: lambda: {"available": 80.0, "exposure": -20.0})
    rc._on_signal_received_serial(_segnale("BACK", 1.0))
    assert not _ordini(bus)
    assert "supera_max_total_exposure" in rifiuti, rifiuti


def test_ack_arrivato_durante_la_lettura_fresca_resta_fra_le_pendenti():
    # L'ACK della riserva del bot arriva a lettura gia' iniziata e la lettura non la riflette: placed_at > read_at,
    # quindi resta fra le pendenti (mai sotto-stima): 20 + 20 + 1 > 35 => rifiuto.
    def lettura(rc):
        def _f():
            time.sleep(0.001)
            rc.table_manager.mark_placed(2, "evt-bot", at=time.monotonic())
            return {"available": 80.0, "exposure": -20.0}
        return _f
    rc, bus, rifiuti = _rc_con_riserva_bot(lettura)
    rc._on_signal_received_serial(_segnale("BACK", 1.0))
    assert "placed_at" in rc.table_manager.get_table(2).meta
    assert not _ordini(bus)
    assert "supera_max_total_exposure" in rifiuti, rifiuti


# --- audit PF-FINANCIAL-IMPACT-GATE-01: lettura iniziata prima del settlement ---------------------------------------

def test_lettura_iniziata_prima_del_p_l_realizzato_non_rivalida_la_cassa():
    # La cassa 100 (80 + 20) e' invalidata da un settlement in perdita (P&L applicato senza lettura del conto).
    # Una lettura iniziata PRIMA del settlement e applicata dopo non deve rimettere in vigore 100: i cap singolo ed
    # evento si calcolerebbero sulla cassa superata (18 invece di 14.40 con cap 18%).
    desk = RiskDesk()
    desk.sync_funds_reading(80.0, 100.0, 80.0, committed_exposure=20.0, read_at=1.0)
    prima_del_settlement = time.monotonic()
    prima = desk.bankroll_current
    desk.apply_closed_pnl(-20.0)
    desk.sync_bankroll(prima)
    assert desk.cash_basis()[0] is None
    assert desk.sync_funds_reading(None, 100.0, 80.0, committed_exposure=20.0,
                                   read_at=prima_del_settlement) is False
    assert desk.cash_basis()[0] is None
    # una lettura iniziata dopo il settlement vale
    assert desk.sync_funds_reading(None, 100.0, 80.0, committed_exposure=20.0, read_at=time.monotonic()) is True
    assert desk.cash_basis()[0] == 100.0


def test_runtime_lettura_di_stato_in_volo_durante_il_settlement_non_rivalida():
    rc, _bus = _make_controller(responses=[])
    _pct(rc.mm.config)
    rc._applica_fondi_avvio({"available": 80.0, "exposure": -20.0}, read_at=time.monotonic())
    in_volo = time.monotonic()                                  # get_status inizia la sua lettura ...
    rc._apply_realized_pnl_without_mutating_bankroll(-20.0)     # ... intanto il settlement applica il P&L
    rc._sync_cash_equity_from_funds({"available": 80.0, "exposure": -20.0}, 80.0, read_at=in_volo)
    assert validators.desk_cash_basis(rc.risk_desk, rc.risk_desk.bankroll_current, rc.table_manager)[0] is None


@pytest.mark.parametrize("cambia", [
    lambda d: d.apply_open_exposure_snapshot(80.0),
    lambda d: d.load_from_dict({"bankroll_start": 80.0, "bankroll_current": 80.0, "equity_peak": 100.0}),
])
def test_snapshot_o_ripristino_spostano_la_soglia_delle_letture(cambia):
    desk = RiskDesk()
    desk.sync_funds_reading(80.0, 100.0, 80.0, committed_exposure=20.0, read_at=1.0)
    iniziata = time.monotonic()
    cambia(desk)
    assert desk.sync_funds_reading(None, 100.0, 80.0, committed_exposure=20.0, read_at=iniziata) is False
