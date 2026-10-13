"""close02 v20 — CL20-PAR-MANUAL-001: le corsie GUI (bet manuale e dutching) applicano gli stessi blocchi di
nuovo rischio della corsia segnale.

Fonte: L6 PARITY (#461/6085510337) «Applicare la stessa regola pertinente a BACK/LAY e GUI/headless/MCP»;
owner #426/6141902809 D1/D2 (drawdown 10 EUR, cap assente => blocco di ogni nuovo rischio); #426/6143637861 t80u;
#506/6144980907 (blocco anche «nel dutching manuale»); OWNER-EXPOSURE-01 (esposizione mai sotto-stimata).
RED su v19: manual_bet e precheck ammettevano con drawdown raggiunto, config corrotta, risk stop, picco non
persistito, recovery di avvio bloccata ed esposizione remota oltre il tetto."""
from pathlib import Path

import pytest

from controllers.dutching_controller import DutchingController
from tests.acceptance.test_issue437_pr27 import _catena, _rifiuti
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal

CAPS = {"max_open_exposure": 10.0, "max_order_exposure": 1.0, "max_market_exposure": 2.0,
        "max_event_exposure_abs": 2.0, "max_drawdown_abs": 10.0}
H1 = {"betId": "100000000002", "marketId": "1.230000777", "selectionId": 1222347, "side": "LAY", "handicap": 0.0,
      "priceSize": {"price": 7.0, "size": 2.86}, "sizeMatched": 0.0, "sizeRemaining": 2.86, "status": "EXECUTABLE"}
MANUAL = {"market_id": "1.234", "selection_id": 11, "price": 2.0, "stake": 0.5, "bet_type": "BACK",
          "event_key": "man-1"}
DUTCH = {"market_id": "1.234", "event_name": "Alpha v Beta", "market_name": "Match Odds", "total_stake": 1.0,
         "commission": 4.5, "selections": [{"selectionId": 1, "price": 3.0, "side": "BACK"},
                                            {"selectionId": 2, "price": 4.0, "side": "BACK"}]}


def _chain(tmp_path, setup=None):
    c = _catena(Path(tmp_path))
    for k, v in CAPS.items():
        setattr(c.rc.config, k, v)
    if setup:
        setup(c)
    return c


def _dd(c):
    c.rc.risk_desk.equity_peak, c.rc.risk_desk.bankroll_current = 1000.0, 990.0


def _corrupt(c):
    c.rc.config.corrupt_keys = ("roserpina.book_block",)


def _riskstop(c):
    c.rc._risk_stop_reason = "risk_stop_manuale"


def _peak(c):
    c.rc._equity_peak_blocked = "equity_peak_non_persistito"


def _startup(c):
    c.rc._startup_recovery_blocked = "STARTUP_RECONCILIATION_REQUIRED"


def _remote(c):
    c.rc.remote_exposure.seed([dict(H1)])
    c.rc.betfair_service.list_current_orders = lambda market_ids=None: [dict(H1)]


BLOCCHI = [("drawdown_10_eur", _dd), ("config_corrotta", _corrupt), ("risk_stop", _riskstop),
           ("picco_non_persistito", _peak), ("recovery_avvio_bloccata", _startup), ("esposizione_remota", _remote)]


@pytest.mark.parametrize("nome,setup", BLOCCHI)
def test_manual_bet_bloccato_come_il_segnale(tmp_path, nome, setup):
    c = _chain(tmp_path / "s", setup)
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert len(c.broker.state.orders) == 0 and _rifiuti(c), nome  # la corsia segnale blocca gia'
    m = _chain(tmp_path / "m", setup)
    r = DutchingController(m.bus, m.rc).manual_bet(dict(MANUAL))
    assert len(m.broker.state.orders) == 0, (nome, r)
    assert not r.get("ok", r.get("success", False)), (nome, r)


@pytest.mark.parametrize("nome,setup", BLOCCHI)
def test_precheck_dutching_bloccato(tmp_path, nome, setup):
    c = _chain(tmp_path, setup)
    pre = DutchingController(c.bus, c.rc).precheck(dict(DUTCH))
    assert pre["ok"] is False, (nome, pre)


def test_controllo_corsie_gui_ammesse_senza_blocchi(tmp_path):
    m = _chain(tmp_path / "m")
    r = DutchingController(m.bus, m.rc).manual_bet(dict(MANUAL))
    assert len(m.broker.state.orders) == 1, r
    d = _chain(tmp_path / "d")
    assert DutchingController(d.bus, d.rc).precheck(dict(DUTCH))["ok"] is True


def test_manual_bet_esposizione_remota_entra_nel_cap(tmp_path):
    # 17.16 di liability remota + 0.50 > 10: il bet manuale vede la stessa esposizione del runtime
    m = _chain(tmp_path, _remote)
    r = DutchingController(m.bus, m.rc).manual_bet(dict(MANUAL))
    assert len(m.broker.state.orders) == 0, r
    assert "esposizione" in str(r).lower() or "exposure" in str(r).lower(), r


def test_precheck_dutching_cap_assoluto_max_open_exposure(tmp_path):
    # esposizione aperta 9.50 + batch 1.00 > 10: bloccato; il % bankroll (50% di 1000) non basta a fermarlo
    def occupy(c):
        t = c.rc.table_manager._tables[2]
        t.status, t.current_event_key, t.market_id, t.current_exposure = "ACTIVE", "altro", "1.999", 9.5
    c = _chain(tmp_path, occupy)
    pre = DutchingController(c.bus, c.rc).precheck(dict(DUTCH))
    assert pre["ok"] is False and "max_open_exposure" in str(pre.get("error")), pre


def test_precheck_dutching_cap_assente_blocca_d2(tmp_path):
    c = _chain(tmp_path, lambda c: setattr(c.rc.config, "max_open_exposure", None))
    pre = DutchingController(c.bus, c.rc).precheck(dict(DUTCH))
    assert pre["ok"] is False and "max_open_exposure_assente" in str(pre.get("error")), pre


def _pending(c):
    c.rc._daily_loss_pending_stop = True


def _nan_equity(c):
    c.rc.risk_desk.bankroll_current = float("nan")


@pytest.mark.parametrize("nome,setup", [("daily_loss_pending_stop", _pending), ("equity_nan", _nan_equity)])
def test_manual_bet_rifiutato_dal_gate_non_solo_al_broker(tmp_path, nome, setup):
    # l'esito del bet manuale e' un rifiuto esplicito (motivo leggibile), non un ok seguito da uno stop al broker
    m = _chain(tmp_path, setup)
    r = DutchingController(m.bus, m.rc).manual_bet(dict(MANUAL))
    assert not r.get("ok", r.get("success", False)) and "Nuovo rischio bloccato" in str(r.get("error")), (nome, r)
    assert len(m.broker.state.orders) == 0


def test_gate_runtime_che_solleva_blocca_fail_closed(tmp_path):
    m = _chain(tmp_path)

    def boom():
        raise RuntimeError("stato rischio illeggibile")
    m.rc.entry_risk_block_reason = boom
    r = DutchingController(m.bus, m.rc).manual_bet(dict(MANUAL))
    assert not r.get("ok", r.get("success", False)) and "gate_rischio_runtime_illeggibile" in str(r.get("error")), r
    assert len(m.broker.state.orders) == 0
