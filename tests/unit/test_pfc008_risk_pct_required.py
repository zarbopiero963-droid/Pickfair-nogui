"""PFC-008 (roadmap #461, M019 C3 + M020 C1; decisione owner #426 c.6369802091).

M019: le quattro percentuali di rischio Roserpina sono obbligatorie. Una chiave assente non riceve un valore di
codice: il loader la passa come None e Money Management, Dutching e cap temporaneo Microstake negano ogni nuovo
ordine con un motivo esplicito; un valore illeggibile, NaN/inf, <= 0 o > 100 e' non valido (prima un NaN rendeva
sempre falso il confronto del Dutching: fail-open). Chiavi recovery assenti => nessun recovery.

Decisione owner (cassa complessiva, precisata nel mandato PFC-008): i cap percentuali di capacita'/esposizione si
calcolano sulla cassa complessiva VERIFICATA (disponibile + impegnato di uno snapshot coerente col contratto del
broker: riserve unmatched e liability LAY gia' nell'esposizione, SIM con `total` coincidente) e l'esposizione
corrente si sottrae una sola volta. Esempio owner: cassa 100, esposizione 20, cap 35% => nuova esposizione 15.
Cassa non verificabile => BLOCKED con diagnostica, nessun ripiego (neppure sul disponibile); le uscite protettive
(hedge del cashout) non vengono bloccate.

M020: nessun cap monetario fisso nei default (config.example + fallback del loader).
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from types import SimpleNamespace

import pytest

from controllers.dutching_controller import DutchingController
from core import validators
from core.money_management import RoserpinaMoneyManagement
from core.risk_desk import RiskDesk
from core.system_state import RoserpinaConfig, RuntimeMode
from core.trading_engine import TradingEngine
from database import Database
from services.setting_service import SettingsService
from simulation_broker import SimulationBroker
from tests.failure.test_dutching_controller_failures import FakeRuntime
from tests.risk_config_fixtures import with_explicit_risk_pct
from tests.integration.test_runtime_controller_cycle_executor import _make_controller

ROOT = Path(__file__).resolve().parents[2]
PCT = ("target_profit_cycle_pct", "max_single_bet_pct", "max_total_exposure_pct", "max_event_exposure_pct")


def _svc(tmp_path):
    return SettingsService(Database(str(tmp_path / "pfc008.db")))


def _cfg(**over):
    cfg = RoserpinaConfig()
    cfg.target_profit_cycle_pct = 3.0
    cfg.max_single_bet_pct = cfg.max_total_exposure_pct = cfg.max_event_exposure_pct = 100.0
    cfg.min_stake = 0.01
    for k, v in over.items():
        setattr(cfg, k, v)
    return cfg


def _calc(cfg, *, available=100.0, cash=None, total=0.0, event=0.0, stake=None, price=3.0, side="BACK"):
    signal = {"price": price, "side": side, "bet_type": side}
    if stake is not None:
        signal["stake"] = stake
    return RoserpinaMoneyManagement(cfg).calculate(
        signal=signal, bankroll_current=available, equity_peak=available, current_total_exposure=total,
        event_current_exposure=event, table=None, cap_base_cash=cash)


# --- validatore --------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("raw", ["abc", True, False, float("nan"), float("inf"), -float("inf"), 0, 0.0, -1, 100.01, "101"])
def test_percentuale_non_valida(raw):
    assert validators.risk_pct_reason("max_single_bet_pct", raw) == "max_single_bet_pct_non_valido"


def test_percentuale_assente_non_configurata():
    assert validators.risk_pct_reason("max_single_bet_pct", None) == "max_single_bet_pct_non_configurato"


@pytest.mark.parametrize("raw", [0.01, 18.0, 100, "35"])
def test_percentuale_valida(raw):
    assert validators.risk_pct_reason("max_single_bet_pct", raw) == ""


@pytest.mark.parametrize("cash,available,atteso", [
    (100.0, 80.0, 100.0),          # cassa verificata
    (100.0, 0.0, 100.0),           # tutto impegnato: la cassa resta la base
    (None, 80.0, None),            # cassa non verificata => BLOCKED, nessun ripiego sul disponibile
    (float("nan"), 80.0, None),
    (float("inf"), 80.0, None),
    (True, 80.0, None),
    (70.0, 80.0, None),            # cassa < disponibile: snapshot incoerente
    (0.0, 0.0, None),
    (100.0, float("nan"), None),
    (100.0, -1.0, None),
])
def test_base_dei_cap(cash, available, atteso):
    assert validators.verified_cap_base(cash, available) == atteso


# --- loader (SQLite reale) ----------------------------------------------------------------------------------------

def test_loader_db_vuoto_nessun_default_di_rischio_e_nessun_recovery(tmp_path):
    cfg = _svc(tmp_path).load_roserpina_config()
    assert all(getattr(cfg, k) is None for k in PCT)
    assert cfg.allow_recovery is False and cfg.max_recovery_tables == 0


@pytest.mark.parametrize("valore", ["abc", True, "", None])
def test_loader_valore_illeggibile_o_vuoto_mai_un_default(tmp_path, valore):
    svc = _svc(tmp_path)
    svc.save_settings({"roserpina.max_single_bet_pct": valore})
    v = svc.load_roserpina_config().max_single_bet_pct
    assert v is None or math.isnan(v)


@pytest.mark.parametrize("valore,motivo", [
    ("nan", "max_total_exposure_pct_non_valido"),
    ("inf", "max_total_exposure_pct_non_valido"),
    ("0", "max_total_exposure_pct_non_valido"),
    ("250", "max_total_exposure_pct_non_valido"),
])
def test_loader_poi_mm_nega_con_motivo_esplicito(tmp_path, valore, motivo):
    svc = _svc(tmp_path)
    svc.save_settings({"roserpina.target_profit_cycle_pct": 3.0, "roserpina.max_single_bet_pct": 18.0,
                       "roserpina.max_event_exposure_pct": 18.0, "roserpina.max_total_exposure_pct": valore,
                       "roserpina.min_stake": 0.01})
    d = _calc(svc.load_roserpina_config())
    assert d.approved is False and d.reason == motivo and d.recommended_stake == 0.0


def test_loader_valori_salvati_restano_quelli_dell_utente(tmp_path):
    svc = _svc(tmp_path)
    svc.save_settings({f"roserpina.{k}": v for k, v in zip(PCT, (4.0, 12.0, 40.0, 9.0))})
    svc.save_settings({"roserpina.allow_recovery": 1, "roserpina.max_recovery_tables": 3})
    cfg = svc.load_roserpina_config()
    assert [getattr(cfg, k) for k in PCT] == [4.0, 12.0, 40.0, 9.0]
    assert cfg.allow_recovery is True and cfg.max_recovery_tables == 3


# --- Money Management ----------------------------------------------------------------------------------------------

@pytest.mark.parametrize("campo", PCT)
def test_mm_percentuale_assente_nega(campo):
    d = _calc(_cfg(**{campo: None}))
    assert d.approved is False and d.reason == f"{campo}_non_configurato" and d.recommended_stake == 0.0


def test_mm_esempio_owner_cassa_100_esposizione_20_cap_35():
    # Disponibile 80 = cassa 100 - impegnato 20. Cap 35% della cassa = 35 => nuova esposizione ammessa 15.
    cfg = _cfg(max_total_exposure_pct=35.0)
    ammesso = _calc(cfg, available=80.0, cash=100.0, total=20.0, stake=15.0)
    assert ammesso.approved is True and ammesso.recommended_stake == pytest.approx(15.0)
    assert ammesso.metadata["cap_base"] == 100.0
    oltre = _calc(cfg, available=80.0, cash=100.0, total=20.0, stake=15.5)
    assert oltre.approved is False and oltre.reason == "supera_max_total_exposure"


@pytest.mark.parametrize("cash", [None, float("nan"), 70.0, 0.0])
def test_mm_senza_cassa_verificata_blocca(cash):
    # Nessun ripiego: neppure uno stake minuscolo, che starebbe sotto qualunque cap, passa senza cassa verificata.
    d = _calc(_cfg(max_total_exposure_pct=35.0), available=80.0, cash=cash, total=20.0, stake=0.5)
    assert d.approved is False and d.reason == "cassa_non_verificata" and d.recommended_stake == 0.0


def test_mm_lay_usa_la_liability_sulla_cassa():
    # LAY 10 @ 2.5 => liability 15: stessa soglia dell'esempio owner.
    cfg = _cfg(max_total_exposure_pct=35.0)
    assert _calc(cfg, available=80.0, cash=100.0, total=20.0, stake=10.0, price=2.5, side="LAY").approved is True
    assert _calc(cfg, available=80.0, cash=100.0, total=20.0, stake=10.1, price=2.5, side="LAY").approved is False


def test_mm_cap_singolo_ed_evento_sulla_cassa():
    cfg = _cfg(max_single_bet_pct=10.0, max_event_exposure_pct=12.0)
    assert _calc(cfg, available=80.0, cash=100.0, stake=10.0).approved is True        # 10% di 100
    assert _calc(cfg, available=80.0, cash=100.0, stake=10.5).reason == "supera_max_single_bet"
    assert _calc(cfg, available=80.0, cash=100.0, event=5.0, stake=7.5).reason == "supera_max_event_exposure"


def test_mm_target_resta_sul_disponibile():
    # Solo i cap cambiano base: lo stake target (3% del disponibile / (quota - 1)) e' invariato.
    d = _calc(_cfg(), available=80.0, cash=100.0)
    assert d.recommended_stake == pytest.approx(80.0 * 0.03 / 2.0)


def test_mm_post_settlement_passa_la_cassa():
    cfg = _cfg(max_total_exposure_pct=35.0)
    mm = RoserpinaMoneyManagement(cfg)
    kw = dict(signal={"price": 3.0, "side": "BACK", "stake": 15.0}, bankroll_current=80.0, equity_peak=80.0,
              current_total_exposure=20.0, event_current_exposure=0.0, table=None)
    assert mm.evaluate_next_trade_after_settlement(cap_base_cash=100.0, **kw).progression_allowed is True
    assert mm.evaluate_next_trade_after_settlement(**kw).progression_allowed is False


# --- Dutching ------------------------------------------------------------------------------------------------------

def _dutching(monkeypatch, *, legs, cash=None, available=80.0, total=20.0, **pct):
    def fake_calculate_dutching(selections, total_stake):
        _ = selections, total_stake
        return ([{"selectionId": i + 1, "price": 3.0, "stake": s, "side": "BACK"} for i, s in enumerate(legs)],
                1.0, 90.0)

    monkeypatch.setattr("controllers.dutching_controller.calculate_dutching", fake_calculate_dutching)
    runtime = FakeRuntime()
    runtime.risk_desk = SimpleNamespace(bankroll_current=available, cash_equity_current=cash, cash_committed_exposure=0.0)
    runtime.table_manager.total_exposure = lambda: total
    for k, v in pct.items():
        setattr(runtime.config, k, v)
    ctrl = DutchingController(bus=SimpleNamespace(publish=lambda *a, **k: None), runtime_controller=runtime)
    return ctrl.precheck({"market_id": "1.300", "total_stake": sum(legs), "simulation_mode": True,
                          "event_name": "Casa v Ospite",  # #554: identita' evento per il cap evento
                          "selections": [{"selectionId": i + 1, "price": 3.0, "side": "BACK"}
                                         for i in range(len(legs))]})


@pytest.mark.parametrize("valore,motivo", [(None, "max_total_exposure_pct_non_configurato"),
                                            (float("nan"), "max_total_exposure_pct_non_valido"),
                                            (float("inf"), "max_total_exposure_pct_non_valido")])
def test_dutching_percentuale_assente_o_nan_nega(monkeypatch, valore, motivo):
    out = _dutching(monkeypatch, legs=[5.0, 5.0], max_total_exposure_pct=valore)
    assert out["ok"] is False and out["reason"] == motivo


def test_dutching_esempio_owner_sulla_cassa(monkeypatch):
    assert _dutching(monkeypatch, legs=[7.5, 7.5], cash=100.0, max_total_exposure_pct=35.0)["ok"] is True
    oltre = _dutching(monkeypatch, legs=[7.5, 8.0], cash=100.0, max_total_exposure_pct=35.0)
    assert oltre["ok"] is False and oltre["error"] == "Esposizione globale oltre limite"
    # Senza cassa verificata nessun ingresso, anche minuscolo.
    senza = _dutching(monkeypatch, legs=[0.5, 0.5], cash=None, max_total_exposure_pct=35.0)
    assert senza["ok"] is False and senza["reason"] == "cassa_non_verificata"


@pytest.mark.parametrize("bankroll", [0.0, -5.0])
def test_dutching_bankroll_non_positivo_non_salta_i_controlli(monkeypatch, bankroll):
    # Prima con bankroll <= 0 i cap di esposizione venivano saltati del tutto (fail-open).
    out = _dutching(monkeypatch, legs=[50.0, 50.0], cash=None, available=bankroll, max_total_exposure_pct=35.0)
    assert out["ok"] is False and out["reason"] == "cassa_non_verificata"


def test_dutching_config_assente_nega(monkeypatch):
    def fake_calculate_dutching(selections, total_stake):
        _ = selections, total_stake
        return ([{"selectionId": 1, "price": 3.0, "stake": 1.0, "side": "BACK"}], 1.0, 90.0)

    monkeypatch.setattr("controllers.dutching_controller.calculate_dutching", fake_calculate_dutching)
    runtime = FakeRuntime()
    runtime.config = None
    runtime.risk_desk = SimpleNamespace(bankroll_current=80.0, cash_equity_current=100.0, cash_committed_exposure=0.0)
    ctrl = DutchingController(bus=SimpleNamespace(publish=lambda *a, **k: None), runtime_controller=runtime)
    out = ctrl.precheck({"market_id": "1.300", "total_stake": 1.0, "simulation_mode": True,
                         "event_name": "Casa v Ospite",  # #554: identita' evento per il cap evento
                         "selections": [{"selectionId": 1, "price": 3.0, "side": "BACK"}]})
    assert out["ok"] is False and out["reason"].endswith("_non_configurato")


def test_dutching_recovery_solo_se_configurato():
    class _TM:
        def __init__(self):
            self.chiamate = []

        def allocate(self, *, event_key, allow_recovery):
            self.chiamate.append(allow_recovery)

    runtime = FakeRuntime()
    runtime.table_manager = _TM()
    ctrl = DutchingController(bus=SimpleNamespace(publish=lambda *a, **k: None), runtime_controller=runtime)
    for valore, atteso in ((True, True), (False, False), (None, False), (1, False)):
        runtime.config = SimpleNamespace(allow_recovery=valore)
        ctrl._allocate_table(event_key="E", batch_exposure=1.0, meta={})
        assert runtime.table_manager.chiamate[-1] is atteso


# --- RiskDesk ------------------------------------------------------------------------------------------------------

def test_riskdesk_cassa_solo_dalla_stessa_lettura():
    desk = RiskDesk()
    assert desk.cash_equity_current is None and desk.cash_equity_reason == "CASH_NOT_READ"
    desk.sync_bankroll(80.0)
    assert desk.cash_equity_current is None and desk.cash_equity_reason == "CASH_NOT_READ"
    desk.sync_cash_equity(100.0, 80.0)
    assert desk.cash_equity_current == 100.0 and desk.cash_equity_reason == ""
    desk.sync_cash_equity(100.0, 75.0)          # disponibile di un'altra lettura
    assert desk.cash_equity_current is None and desk.cash_equity_reason == "CASH_READING_MISMATCH"
    desk.sync_cash_equity(70.0, 80.0)           # cassa < disponibile: incoerente
    assert desk.cash_equity_current is None and desk.cash_equity_reason == "CASH_INCOHERENT"
    desk.sync_cash_equity(float("inf"), 80.0)
    assert desk.cash_equity_current is None
    desk.sync_cash_equity(100.0, 80.0, reason="CASH_TOTAL_MISMATCH")   # verifica dello snapshot fallita
    assert desk.cash_equity_current is None and desk.cash_equity_reason == "CASH_TOTAL_MISMATCH"


@pytest.mark.parametrize("mutazione", [
    lambda d: d.sync_bankroll(90.0),
    lambda d: d.apply_closed_pnl(-5.0),
    lambda d: d.apply_open_exposure_snapshot(70.0),
    lambda d: d.load_from_dict({"bankroll_current": 80.0}),
])
def test_riskdesk_bankroll_cambiato_senza_lettura_invalida_la_cassa(mutazione):
    desk = RiskDesk()
    desk.sync_bankroll(80.0)
    desk.sync_cash_equity(100.0, 80.0)
    mutazione(desk)
    assert desk.cash_equity_current is None


# --- verifica della cassa (snapshot fondi) -----------------------------------------------------------------------

def _verifica(funds):
    from core.runtime_controller import RuntimeController
    return RuntimeController._cash_equity_verification(funds)


def test_verifica_live_contratto_betfair():
    # LIVE: availableToBetBalance + |exposure| (Betfair riporta l'exposure negativa, unmatched gia' inclusi).
    assert _verifica({"available": 80.0, "exposure": -20.0}) == {"ok": True, "cash": 100.0, "reason": ""}


@pytest.mark.parametrize("funds,motivo", [
    ({"available": 80.0, "exposure": -20.0, "error": "FILL_LEDGER_INCONSISTENT"}, "BALANCE_ERROR:FILL_LEDGER_INCONSISTENT"),
    ({"available": 80.0}, "BALANCE_FIELD_MISSING"),
    ({"available": float("nan"), "exposure": 0.0}, "BALANCE_NOT_FINITE_OR_NEGATIVE"),
    ({"available": 0.0, "exposure": 0.0}, "CASH_NOT_POSITIVE"),
    # SIM: disponibile + impegnato deve coincidere con la cassa del broker.
    ({"available": 80.0, "exposure": 20.0, "total": 90.0, "simulated": True}, "CASH_TOTAL_MISMATCH"),
    ({"available": 80.0, "exposure": 20.0, "simulated": True}, "CASH_TOTAL_MISMATCH"),
    # Le riserve unmatched sono PARTE dell'esposizione: oltre l'esposizione = contate due volte o incoerenti.
    ({"available": 80.0, "exposure": 20.0, "total": 100.0, "reserved_unmatched": 25.0, "simulated": True},
     "CASH_RESERVE_INCOHERENT"),
    ({"available": 80.0, "exposure": 20.0, "total": 100.0, "reserved_unmatched": float("nan"), "simulated": True},
     "CASH_RESERVE_INCOHERENT"),
])
def test_verifica_cassa_non_verificabile(funds, motivo):
    out = _verifica(funds)
    assert out["ok"] is False and out["cash"] is None and out["reason"] == motivo


def test_verifica_sim_reale_nessun_doppio_conteggio_unmatched_e_lay():
    # BACK 10 abbinato + LAY 10 @ 3.0 a riposo (liability 20, non abbinata): la cassa resta quella del broker.
    b = SimulationBroker(starting_balance=100.0)
    b.update_market_book("1.1", {"status": "OPEN", "runners": [
        {"selectionId": 1, "status": "ACTIVE", "ex": {"availableToBack": [{"price": 2.0, "size": 100.0}]}},
        {"selectionId": 2, "status": "ACTIVE", "ex": {"availableToLay": [{"price": 5.0, "size": 100.0}]}}]})
    assert b.place_bet(market_id="1.1", selection_id=1, side="BACK", price=2.0, size=10.0)["status"] == "SUCCESS"
    assert b.place_bet(market_id="1.1", selection_id=2, side="LAY", price=3.0, size=10.0)["status"] == "SUCCESS"
    funds = dict(b.get_account_funds(), simulated=True)
    assert funds["reserved_unmatched"] > 0                 # la LAY e' davvero a riposo
    out = _verifica(funds)
    assert out == {"ok": True, "cash": pytest.approx(100.0), "reason": ""}
    # La base dei cap e' la cassa, non cassa + riserve: riserve e liability non si sommano di nuovo.
    assert out["cash"] == pytest.approx(funds["available"] + funds["exposure"])


# --- runtime -------------------------------------------------------------------------------------------------------

def test_runtime_avvio_registra_la_cassa_della_lettura():
    rc, _bus = _make_controller(responses=[])
    rc._applica_fondi_avvio({"available": 80.0, "exposure": -20.0})
    assert rc.risk_desk.bankroll_current == 80.0 and rc.risk_desk.cash_equity_current == 100.0


def test_runtime_avvio_fondi_non_verificabili_nessuna_cassa():
    rc, _bus = _make_controller(responses=[])
    rc._applica_fondi_avvio({"available": 80.0, "exposure": -20.0, "error": "FILL_LEDGER_INCONSISTENT"})
    assert rc.risk_desk.cash_equity_current is None


def test_runtime_avvio_sim_incoerente_nessuna_cassa():
    rc, _bus = _make_controller(responses=[])
    rc._applica_fondi_avvio({"available": 80.0, "exposure": 20.0, "total": 95.0, "simulated": True})
    assert rc.risk_desk.cash_equity_current is None and rc.risk_desk.cash_equity_reason == "CASH_TOTAL_MISMATCH"


def test_runtime_get_status_aggiorna_e_poi_invalida_la_cassa():
    rc, _bus = _make_controller(responses=[{"available": 80.0, "exposure": -20.0},
                                           {"available": 80.0, "exposure": -20.0, "error": "X"}])
    stato = rc.get_status()
    assert rc.risk_desk.cash_equity_current == 100.0
    assert stato["cash_equity_verified"] == 100.0 and stato["cash_equity_reason"] == ""
    stato = rc.get_status()                                   # lettura fallita: mai la cassa precedente
    assert rc.risk_desk.cash_equity_current is None
    assert stato["cash_equity_verified"] is None and stato["cash_equity_reason"] == "BALANCE_ERROR:X"


_SEGNALE = {"market_id": "1.1", "selection_id": 1, "price": 3.0, "odds": 3.0, "side": "BACK", "bet_type": "BACK",
            "event_name": "A v B", "event_id": "E1", "simulation_mode": True}


def test_runtime_passa_la_cassa_al_money_management(monkeypatch):
    rc, _bus = _make_controller(responses=[])
    rc._applica_fondi_avvio({"available": 80.0, "exposure": -20.0})
    rc.mode = RuntimeMode.ACTIVE
    visti = []

    def _calcola(**kwargs):
        visti.append(kwargs.get("cap_base_cash"))
        return SimpleNamespace(approved=False, recommended_stake=0.0, reason="test_stop", table_id=1,
                               desk_mode=rc._desk_mode(), metadata={})

    monkeypatch.setattr(rc.mm, "calculate", _calcola)
    rc._on_signal_received_serial(dict(_SEGNALE))
    assert visti == [100.0]


def test_runtime_senza_cassa_verificata_rifiuta_con_diagnostica():
    rc, _bus = _make_controller(responses=[])
    with_explicit_risk_pct(rc.mm.config)
    rc._applica_fondi_avvio({"available": 80.0, "exposure": 20.0, "total": 95.0, "simulated": True})
    rc.mode = RuntimeMode.ACTIVE
    rifiuti = []
    rc._reject_signal = lambda _s, motivo: rifiuti.append(motivo)
    rc._on_signal_received_serial(dict(_SEGNALE))
    assert rifiuti == ["cassa_non_verificata:CASH_TOTAL_MISMATCH"]


def test_runtime_percentuale_mancante_rifiuta_ma_resta_avviabile():
    # La config senza percentuali non impedisce al runtime di esistere e leggere i fondi: blocca solo le entrate.
    rc, _bus = _make_controller(responses=[])
    rc.mm.config.target_profit_cycle_pct = None                 # chiave mai salvata
    rc._applica_fondi_avvio({"available": 80.0, "exposure": -20.0})
    rc.mode = RuntimeMode.ACTIVE
    rifiuti = []
    rc._reject_signal = lambda _s, motivo: rifiuti.append(motivo)
    rc._on_signal_received_serial(dict(_SEGNALE))
    assert rifiuti == ["target_profit_cycle_pct_non_configurato"]


# --- cap temporaneo Microstake ------------------------------------------------------------------------------------

def _motore(pct, cash):
    desk = RiskDesk()
    desk.sync_bankroll(80.0)
    desk.sync_cash_equity(cash, 80.0)
    mm = RoserpinaMoneyManagement(_cfg(max_total_exposure_pct=pct))
    rc = SimpleNamespace(betfair_service=SimpleNamespace(get_account_funds=lambda: {"available": 80.0, "exposure": -20.0}),
                         mm=mm, risk_desk=desk, table_manager=SimpleNamespace(total_exposure=lambda: 20.0))
    return SimpleNamespace(runtime_controller=rc, microstake_executor=None)


def test_microstake_headroom_sulla_cassa():
    # min(disponibile 80, 35% di 100 - 20 = 15) = 15; prima 35% di 80 - 20 = 8.
    assert TradingEngine._native_transient_cap(_motore(35.0, 100.0), {}) == pytest.approx(15.0)


@pytest.mark.parametrize("pct,cash", [(None, 100.0), (float("nan"), 100.0), (35.0, None)])
def test_microstake_ingresso_bloccato_senza_percentuale_o_cassa(pct, cash):
    assert TradingEngine._native_transient_cap(_motore(pct, cash), {}) is None


@pytest.mark.parametrize("pct,cash", [(None, 100.0), (float("nan"), 100.0), (35.0, None)])
def test_microstake_hedge_protettivo_non_bloccato(pct, cash):
    # L'hedge del cashout riduce l'esposizione: resta il solo limite dei fondi disponibili (80).
    assert TradingEngine._native_transient_cap(_motore(pct, cash), {"protective": True}) == pytest.approx(80.0)


def test_microstake_hedge_protettivo_non_limitato_dall_headroom():
    # #548 / mandato PFC-008 punto 5: con esposizione al cap l'ingresso ha headroom 0, l'hedge resta nei fondi.
    m = _motore(35.0, 100.0)
    m.runtime_controller.table_manager = SimpleNamespace(total_exposure=lambda: 35.0)
    assert TradingEngine._native_transient_cap(m, {}) == pytest.approx(0.0)
    assert TradingEngine._native_transient_cap(m, {"protective": True}) == pytest.approx(80.0)


def test_order_router_marca_l_hedge_come_protettivo():
    from core.order_router import OrderRouter
    visti = []

    class _Exec:
        def execute(self, payload):
            visti.append(dict(payload))
            return {"ok": False, "status": "BLOCKED", "reason": "test"}

    router = OrderRouter(betfair_service=None, microstake_executor_provider=lambda: _Exec())
    try:
        router._place_native_microstake({"market_id": "1.1", "selection_id": 1, "bet_type": "LAY", "price": 2.0,
                                         "stake": 0.5, "customer_ref": "cashout-1"})
    except Exception:
        pass
    assert visti and visti[0]["protective"] is True


# --- M020: nessun cap monetario fisso nei default ---------------------------------------------------------------

# Importi in euro che limitano il rischio: nessuno puo' avere un valore di default (rettifica owner 18/10/2026).
MONETARY_CAPS = ("max_daily_loss", "max_open_exposure", "max_order_exposure", "max_market_exposure",
                 "max_event_exposure_abs", "max_drawdown_abs", "max_session_loss", "max_exposure_stop",
                 "max_recovery_chase_abs", "max_stake_abs", "max_win")


def test_m020_loader_db_vuoto_nessun_cap_monetario(tmp_path):
    cfg = _svc(tmp_path).load_roserpina_config()
    assert {k: getattr(cfg, k) for k in MONETARY_CAPS} == dict.fromkeys(MONETARY_CAPS)


def test_m020_dataclass_nessun_cap_monetario():
    cfg = RoserpinaConfig()
    assert {k: getattr(cfg, k) for k in MONETARY_CAPS} == dict.fromkeys(MONETARY_CAPS)


def _chiavi(obj, prefix=""):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from _chiavi(v, f"{prefix}{k}.")
            yield f"{prefix}{k}", v


def test_m020_config_example_senza_cap_monetari_ne_percentuali_di_rischio():
    data = json.loads((ROOT / "config.example.json").read_text(encoding="utf-8"))
    vietate = set(MONETARY_CAPS) | set(PCT) | {"allow_recovery", "max_recovery_tables"}
    trovate = [k for k, _ in _chiavi(data) if k.rsplit(".", 1)[-1] in vietate]
    assert trovate == []


def test_runtime_auto_next_senza_cassa_verificata_motivo_esplicito():
    # Settlement con snapshot SIM incoerente (disponibile + impegnato != totale): bankroll sincronizzato, cassa no.
    from tests.core.test_auto_trade_mm_gate import _close_payload, _make_controller as _make_auto
    rc, _ = _make_auto(responses=[{"available": 150.0, "exposure": 0.0, "total": 160.0, "simulated": True}])
    rc.mode = RuntimeMode.ACTIVE
    rc._on_close_position(_close_payload(
        auto_trade_enabled=True, cycle_executor_enabled=True,
        mm_context={"cycle_active": True, "cycle_id": "c-pfc008", "table": {"table_id": 1},
                    "next_signal": {"market_id": "1.2", "selection_id": 2, "price": 2.0, "side": "BACK"}}))
    esito = rc._last_auto_trade_result
    assert esito["auto_trade_status"] == "AUTO_TRADE_SKIPPED_MM_BLOCKED"
    assert esito["reason"] == "cassa_non_verificata:CASH_TOTAL_MISMATCH"


def test_runtime_disponibile_verificato_zero_non_usa_il_bankroll_precedente():
    # #546: get_status non riallinea il bankroll a un disponibile 0; con PFC-008 la cassa di quella lettura non
    # appartiene al bankroll corrente (100 della lettura precedente) => cassa non verificata, entrate bloccate.
    rc, _bus = _make_controller(responses=[{"available": 0.0, "exposure": -20.0}])
    with_explicit_risk_pct(rc.mm.config)
    rc._applica_fondi_avvio({"available": 100.0, "exposure": 0.0})
    assert rc.risk_desk.cash_equity_current == 100.0
    rc.get_status()
    assert rc.risk_desk.bankroll_current == 100.0                       # bankroll vecchio, non riallineato
    assert rc.risk_desk.cash_equity_current is None
    assert rc.risk_desk.cash_equity_reason == "CASH_READING_MISMATCH"
    rc.mode = RuntimeMode.ACTIVE
    rifiuti = []
    rc._reject_signal = lambda _s, motivo: rifiuti.append(motivo)
    rc._on_signal_received_serial(dict(_SEGNALE, stake=0.5))
    assert rifiuti == ["cassa_non_verificata:CASH_READING_MISMATCH"]


# --- review indipendente PFC-008 (rilievi corretti) --------------------------------------------------------------

def test_mm_esposizione_esterna_si_sottrae_dalla_cassa():
    # Cassa 100 di cui 20 impegnati fuori dai tavoli del bot (bet manuale): cap 35% => 15, non 35.
    cfg = _cfg(max_total_exposure_pct=35.0)
    mm = RoserpinaMoneyManagement(cfg)
    kw = dict(signal={"price": 3.0, "side": "BACK", "stake": 15.0}, bankroll_current=80.0, equity_peak=80.0,
              current_total_exposure=0.0, event_current_exposure=0.0, table=None, cap_base_cash=100.0)
    assert mm.calculate(external_exposure=20.0, **kw).approved is True
    kw["signal"] = {"price": 3.0, "side": "BACK", "stake": 30.0}
    d = mm.calculate(external_exposure=20.0, **kw)
    assert d.approved is False and d.reason == "supera_max_total_exposure"


@pytest.mark.parametrize("esterna", [float("nan"), -1.0, True, "x"])
def test_mm_esposizione_esterna_illeggibile_blocca(esterna):
    d = _calc(_cfg(), available=80.0, cash=100.0, stake=0.5)
    assert d.approved is True
    d = RoserpinaMoneyManagement(_cfg()).calculate(
        signal={"price": 3.0, "side": "BACK", "stake": 0.5}, bankroll_current=80.0, equity_peak=80.0,
        current_total_exposure=0.0, event_current_exposure=0.0, table=None, cap_base_cash=100.0,
        external_exposure=esterna)
    assert d.approved is False and d.reason == "cassa_non_verificata"


def test_runtime_registra_e_passa_l_esposizione_esterna(monkeypatch):
    # Avvio con 20 impegnati e nessun tavolo del bot: tutta l'esposizione e' esterna.
    rc, _bus = _make_controller(responses=[])
    rc._applica_fondi_avvio({"available": 80.0, "exposure": -20.0})
    assert rc.risk_desk.cash_committed_exposure == 20.0
    rc.mode = RuntimeMode.ACTIVE
    visti = []

    def _calcola(**kwargs):
        visti.append((kwargs.get("cap_base_cash"), kwargs.get("external_exposure")))
        return SimpleNamespace(approved=False, recommended_stake=0.0, reason="test_stop", table_id=1,
                               desk_mode=rc._desk_mode(), metadata={})

    monkeypatch.setattr(rc.mm, "calculate", _calcola)
    rc._on_signal_received_serial(dict(_SEGNALE))
    assert visti == [(100.0, 20.0)]


def test_runtime_esposizione_dei_tavoli_non_e_esterna():
    # I 20 impegnati sono del bot, gia' a mercato prima della lettura: nessuna esposizione oltre i tavoli.
    rc, _bus = _make_controller(responses=[])
    rc.table_manager.total_exposure = lambda: 20.0
    rc.table_manager.pending_exposure = lambda since: 0.0
    rc._applica_fondi_avvio({"available": 80.0, "exposure": -20.0}, read_at=5.0)
    assert rc.risk_desk.cash_committed_exposure == 20.0
    assert validators.desk_cash_basis(rc.risk_desk, 80.0, rc.table_manager) == (100.0, 0.0)


def test_riserva_pendente_non_assorbe_l_esposizione_esterna():
    # Rilievo Codex P1 su dd0b02b: cassa 100, 20 impegnati esterni, riserva del bot da 10 non ancora a mercato.
    # L'esposizione che il cap deve vedere e' 30 (non 20): con cap 35% restano 5, non 15.
    from core.table_manager import TableManager
    tm = TableManager(table_count=2)
    tm.activate(table_id=1, event_key="E1", exposure=10.0, market_id="1.1", selection_id=1)   # pendente
    desk = RiskDesk()
    desk.sync_bankroll(80.0)
    desk.sync_cash_equity(100.0, 80.0, committed_exposure=20.0, read_at=1.0)
    base, oltre = validators.desk_cash_basis(desk, 80.0, tm)
    assert (base, oltre) == (100.0, 20.0)                       # tavoli 10 + oltre 20 = 30
    mm = RoserpinaMoneyManagement(_cfg(max_total_exposure_pct=35.0))
    kw = dict(bankroll_current=80.0, equity_peak=80.0, current_total_exposure=tm.total_exposure(),
              event_current_exposure=0.0, table=None, cap_base_cash=base, external_exposure=oltre)
    assert mm.calculate(signal={"price": 3.0, "side": "BACK", "stake": 5.0}, **kw).approved is True
    d = mm.calculate(signal={"price": 3.0, "side": "BACK", "stake": 15.0}, **kw)
    assert d.approved is False and d.reason == "supera_max_total_exposure"
    # Piazzata prima della lettura: ora e' dentro l'impegnato letto (che la conterebbe), niente doppio conteggio.
    tm.mark_placed(1, "E1", at=0.5)
    desk.sync_cash_equity(100.0, 80.0, committed_exposure=30.0, read_at=2.0)
    assert validators.desk_cash_basis(desk, 80.0, tm) == (100.0, 20.0)


def test_dutching_esposizione_esterna_si_sottrae(monkeypatch):
    def _dutch(legs, external):
        def fake_calculate_dutching(selections, total_stake):
            _ = selections, total_stake
            return ([{"selectionId": i + 1, "price": 3.0, "stake": s, "side": "BACK"} for i, s in enumerate(legs)],
                    1.0, 90.0)
        monkeypatch.setattr("controllers.dutching_controller.calculate_dutching", fake_calculate_dutching)
        runtime = FakeRuntime()
        runtime.risk_desk = SimpleNamespace(bankroll_current=80.0, cash_equity_current=100.0,
                                            cash_committed_exposure=external)
        runtime.config.max_total_exposure_pct = 35.0
        runtime.config.target_profit_cycle_pct = 3.0
        ctrl = DutchingController(bus=SimpleNamespace(publish=lambda *a, **k: None), runtime_controller=runtime)
        return ctrl.precheck({"market_id": "1.300", "total_stake": sum(legs), "simulation_mode": True,
                              "event_name": "Casa v Ospite",  # #554: identita' evento per il cap evento
                              "selections": [{"selectionId": i + 1, "price": 3.0, "side": "BACK"}
                                             for i in range(len(legs))]})
    assert _dutch([7.5, 7.5], 20.0)["ok"] is True
    oltre = _dutch([7.5, 8.0], 20.0)
    assert oltre["ok"] is False and oltre["error"] == "Esposizione globale oltre limite"


def test_microstake_headroom_sottrae_l_esposizione_esterna():
    m = _motore(35.0, 100.0)
    m.runtime_controller.table_manager = SimpleNamespace(total_exposure=lambda: 0.0)
    m.runtime_controller.risk_desk.sync_cash_equity(100.0, 80.0, committed_exposure=20.0)
    assert TradingEngine._native_transient_cap(m, {}) == pytest.approx(15.0)


def test_dutching_richiede_anche_il_target(monkeypatch):
    out = _dutching(monkeypatch, legs=[1.0, 1.0], cash=100.0, target_profit_cycle_pct=None)
    assert out["ok"] is False and out["reason"] == "target_profit_cycle_pct_non_configurato"


def test_riskdesk_lettura_piu_vecchia_non_resuscita_la_cassa():
    # get_status legge F1 (80/-20) prima del settlement; il settlement applica F2 (80/0); F1 arriva tardi.
    desk = RiskDesk()
    desk.sync_bankroll(80.0)
    desk.sync_cash_equity(80.0, 80.0, read_at=2.0)           # F2, iniziata dopo
    desk.sync_cash_equity(100.0, 80.0, read_at=1.0)          # F1, iniziata prima: ignorata
    assert desk.cash_equity_current == 80.0
    desk.sync_cash_equity(90.0, 80.0, read_at=3.0)           # lettura nuova: applicata
    assert desk.cash_equity_current == 90.0


def test_runtime_get_status_eccezione_invalida_la_cassa():
    rc, _bus = _make_controller(responses=[RuntimeError("rete")])
    rc._applica_fondi_avvio({"available": 80.0, "exposure": -20.0})
    assert rc.risk_desk.cash_equity_current == 100.0
    with pytest.raises(RuntimeError):
        rc.get_status()
    assert rc.risk_desk.cash_equity_current is None
    assert rc.risk_desk.cash_equity_reason == "BALANCE_READ_EXCEPTION"


def test_mm_lockdown_resta_prima_dei_nuovi_blocchi():
    # Escalation invariata: bankroll <= 0 e perdita giornaliera oltre il limite portano in LOCKDOWN anche con
    # percentuali mancanti o cassa non verificata.
    from core.system_state import DeskMode
    vuota = RoserpinaConfig()
    d = RoserpinaMoneyManagement(vuota).calculate(
        signal={"price": 3.0, "side": "BACK"}, bankroll_current=0.0, equity_peak=100.0, current_total_exposure=0.0,
        event_current_exposure=0.0, table=None)
    assert d.desk_mode is DeskMode.LOCKDOWN and d.reason == "bankroll_non_valido"
    vuota.max_daily_loss = 10.0
    d = RoserpinaMoneyManagement(vuota).calculate(
        signal={"price": 3.0, "side": "BACK", "daily_loss_snapshot": 10.0}, bankroll_current=100.0,
        equity_peak=100.0, current_total_exposure=0.0, event_current_exposure=0.0, table=None)
    assert d.desk_mode is DeskMode.LOCKDOWN and d.approved is False


# --- review Grok su 9c3e172 --------------------------------------------------------------------------------------

def test_ingresso_con_protective_spoofato_non_aggira_il_cap_temporaneo():
    # Solo l'OrderRouter (hedge del cashout) puo' marcare `protective`: un ingresso che lo porta non salta il cap.
    from tests.test_microstake_engine_native import _engine, _orders, _payload
    engine, broker, _ = _engine(bankroll=100.0)
    engine.runtime_controller.risk_desk.cash_equity_current = None           # cassa non verificata
    res = engine.submit_quick_bet(dict(_payload("spoof", size=0.10, price=2.0), protective=True))
    assert res["ok"] is False and "transient_cap_unavailable" in str(res["error"]) and _orders(broker) == []


@pytest.mark.parametrize("desk", [
    SimpleNamespace(cash_equity_current=100.0),                               # campo esterno mancante
    SimpleNamespace(cash_equity_current=100.0, cash_committed_exposure=None),
    SimpleNamespace(cash_equity_current=100.0, cash_committed_exposure=float("nan")),
    SimpleNamespace(cash_equity_current=None, cash_committed_exposure=0.0),
])
def test_base_e_esterna_assenti_non_valgono_zero(desk):
    assert validators.desk_cash_basis(desk, 80.0) == (None, None)


def test_base_e_esterna_dalla_stessa_lettura():
    desk = RiskDesk()
    desk.sync_bankroll(80.0)
    desk.sync_cash_equity(100.0, 80.0, committed_exposure=20.0, read_at=3.0)
    assert desk.cash_basis() == (100.0, 20.0, 3.0)
    assert validators.desk_cash_basis(desk, 80.0) == (100.0, 20.0)


def test_dutching_risk_desk_senza_esposizione_esterna_blocca(monkeypatch):
    def fake_calculate_dutching(selections, total_stake):
        _ = selections, total_stake
        return ([{"selectionId": 1, "price": 3.0, "stake": 0.5, "side": "BACK"}], 1.0, 90.0)
    monkeypatch.setattr("controllers.dutching_controller.calculate_dutching", fake_calculate_dutching)
    runtime = FakeRuntime()
    runtime.risk_desk = SimpleNamespace(bankroll_current=80.0, cash_equity_current=100.0)
    ctrl = DutchingController(bus=SimpleNamespace(publish=lambda *a, **k: None), runtime_controller=runtime)
    out = ctrl.precheck({"market_id": "1.300", "total_stake": 0.5, "simulation_mode": True,
                         "event_name": "Casa v Ospite",  # #554: identita' evento per il cap evento
                         "selections": [{"selectionId": 1, "price": 3.0, "side": "BACK"}]})
    assert out["ok"] is False and out["reason"] == "cassa_non_verificata"
