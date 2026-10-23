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
