"""close02 v18 — CL17-RST-GHOST-001: ordini remoti ritrovati al riavvio contati nell'esposizione.

Fonte: #461/6085516609 (PF-CASH-23, V3); owner #426/6124156631 OWNER-EXPOSURE-01 (caso peggiore
netto, matched+unmatched, esito non modellato => blocco) e OWNER-SETTLEMENT-02 (nessun blocco
permanente, nessun rilascio di capitale impegnato). RED su v17 (esposizione 0, ingresso
ammesso oltre il tetto di 10 EUR), GREEN col patch v18.

Scenario del caso Under 1.5 HT: BACK 10.00@2.00 abbinato (B1) e hedge LAY 20/7 ~ 2.86@7.00 a
riposo (H1) sul mercato 1.230000777; al riavvio entrambi sono fra gli ordini correnti."""
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from core.remote_exposure import RemoteExposureRegistry, market_worst_case, normalize_order
from tests.close02.test_close02_restart_t81u import OWNER_VALID, _ctl

M = "1.230000777"
B1 = {"betId": "100000000001", "marketId": M, "selectionId": 1222347, "side": "BACK", "handicap": 0.0,
      "priceSize": {"price": 2.0, "size": 10.0}, "sizeMatched": 10.0, "sizeRemaining": 0.0,
      "averagePriceMatched": 2.0, "status": "EXECUTION_COMPLETE"}
H1 = {"betId": "100000000002", "marketId": M, "selectionId": 1222347, "side": "LAY", "handicap": 0.0,
      "priceSize": {"price": 7.0, "size": 2.86}, "sizeMatched": 0.0, "sizeRemaining": 2.86,
      "averagePriceMatched": 0.0, "status": "EXECUTABLE"}
OWNER = dict(OWNER_VALID, **{"roserpina.max_open_exposure": "10"})
SIGNAL = {"market_id": "1.555", "selection_id": "77", "price": 2.0, "signal_type": "BACK"}


class _Recon:
    def __init__(self, rows):
        self.rows = rows

    def fetch_startup_active_orders(self):
        return [dict(r) for r in self.rows]

    def merge_startup_active_orders(self, rows):
        return {"orders": list(rows)}

    def reconcile_all_open_batches(self):
        return None

    def __getattr__(self, name):
        return MagicMock()


def _avvia(tmp_path, monkeypatch, rows, db_name="r.sqlite"):
    rc = _ctl(str(tmp_path / db_name), monkeypatch, owner=OWNER)
    monkeypatch.setattr(rc, "_build_reconciliation_engine", lambda: _Recon(rows))
    out = rc.start(execution_mode="SIMULATION")
    assert out.get("started") is not False, out
    # decisione nuova a ogni chiamata: il runtime muta approved/reason sul rifiuto
    monkeypatch.setattr(rc.mm, "calculate", MagicMock(
        side_effect=lambda **kw: MagicMock(approved=True, recommended_stake=1.0, table_id=1, reason="")))
    rej = []
    monkeypatch.setattr(rc, "_reject_signal", lambda sg, r: rej.append(r))
    rc.bus.reset_mock()
    return rc, rej


def _inviati(rc):
    return [c for c in rc.bus.publish.call_args_list if c.args and c.args[0] == "CMD_QUICK_BET"]


# ----------------------------------------------------------------- calcolo (OWNER-EXPOSURE-01)
def _n(*rows):
    return [normalize_order(r) for r in rows]


def test_caso_peggiore_netto_cashout_residuo_a_riposo_10_euro():
    # selezione vince: +10 - 6*2.86 = -7.16 ; nessuna vince: -10 + 0 (hedge non abbinato) = -10
    assert market_worst_case(_n(B1, H1)) == Decimal("10.00")


def test_caso_peggiore_netto_hedge_abbinato_per_intero_arrotondato_per_eccesso():
    h = dict(H1, sizeMatched=20 / 7, sizeRemaining=0.0, averagePriceMatched=7.0)
    # vince: +10 - 6*20/7 = -50/7 ; perde: -10 + 20/7 = -50/7 => 7.142857 -> 7.15 (per eccesso)
    assert market_worst_case(_n(B1, h)) == Decimal("7.15")


def test_liability_lay_a_riposo_e_back_a_riposo():
    assert market_worst_case(_n(H1)) == Decimal("17.16")  # 6 * 2.86
    back = dict(B1, sizeMatched=0.0, sizeRemaining=3.0, status="EXECUTABLE")
    assert market_worst_case(_n(back)) == Decimal("3.00")


@pytest.mark.parametrize("campo,valore", [("side", "X"), ("handicap", 1.5), ("sizeRemaining", float("nan")),
                                          ("priceSize", {"price": 1.0, "size": 2.86}), ("sizeMatched", -1.0),
                                          ("selectionId", None), ("sizeRemaining", True)])
def test_riga_non_modellabile_esposizione_infinita(campo, valore):
    reg = RemoteExposureRegistry()
    reg.seed([B1, dict(H1, **{campo: valore})])
    assert reg.total() == float("inf")
    assert reg.market_exposure(M) == float("inf")


# --------------------------------------------------------------------------- RED/GREEN runtime
def test_controllo_senza_ordini_remoti_ingresso_ammesso(tmp_path, monkeypatch):
    rc, rej = _avvia(tmp_path, monkeypatch, [])
    rc._on_signal_received(dict(SIGNAL))
    assert rej == [] and len(_inviati(rc)) == 1


def test_riavvio_con_hedge_a_riposo_nuovo_ingresso_oltre_tetto_bloccato(tmp_path, monkeypatch):
    rc, rej = _avvia(tmp_path, monkeypatch, [B1, H1])
    rc.betfair_service.list_current_orders.return_value = [dict(B1), dict(H1)]
    rc._on_signal_received(dict(SIGNAL))
    assert _inviati(rc) == []
    assert rej and rej[0].startswith("max_open_exposure_exceeded"), rej
    assert rc._open_exposure_total() == pytest.approx(10.0)


def test_secondo_riavvio_ordini_gia_registrati_restano_contati(tmp_path, monkeypatch):
    _avvia(tmp_path, monkeypatch, [B1, H1], db_name="s.sqlite")  # primo avvio: li registra
    rc, rej = _avvia(tmp_path, monkeypatch, [B1, H1], db_name="s.sqlite")
    rc.betfair_service.list_current_orders.return_value = [dict(B1), dict(H1)]
    rc._on_signal_received(dict(SIGNAL))
    assert _inviati(rc) == [] and rej and rej[0].startswith("max_open_exposure_exceeded"), rej


def test_lettura_correnti_fallita_nessun_rilascio(tmp_path, monkeypatch):
    rc, rej = _avvia(tmp_path, monkeypatch, [B1, H1])
    rc.betfair_service.list_current_orders.side_effect = ConnectionError("exchange non raggiungibile")
    rc._on_signal_received(dict(SIGNAL))
    assert _inviati(rc) == [] and rej, rej
    assert rc._open_exposure_total() == pytest.approx(10.0)


def test_ordini_spariti_dai_correnti_rilascio_e_ingresso_ammesso(tmp_path, monkeypatch):
    rc, rej = _avvia(tmp_path, monkeypatch, [B1, H1])
    rc.betfair_service.list_current_orders.return_value = []  # mercato regolato: nulla di impegnato
    rc._on_signal_received(dict(SIGNAL))
    assert rej == [] and len(_inviati(rc)) == 1
    assert rc.remote_exposure.empty() and rc.remote_exposure.total() == 0.0
    rilasci = [c for c in rc.bus.publish.call_args_list if c.args and c.args[0] == "REMOTE_EXPOSURE_RELEASED"]
    assert rilasci and sorted(rilasci[0].args[1]["bet_ids"]) == [B1["betId"], H1["betId"]]


def test_hedge_ancora_a_riposo_solo_b1_regolato_resta_la_liability(tmp_path, monkeypatch):
    rc, rej = _avvia(tmp_path, monkeypatch, [B1, H1])
    rc.betfair_service.list_current_orders.return_value = [dict(H1)]
    rc._on_signal_received(dict(SIGNAL))
    assert _inviati(rc) == [] and rej and rej[0].startswith("max_open_exposure_exceeded"), rej
    assert rc._open_exposure_total() == pytest.approx(17.16)


def test_riga_non_modellabile_blocca_finche_la_lettura_non_e_pulita(tmp_path, monkeypatch):
    rc, rej = _avvia(tmp_path, monkeypatch, [dict(H1, side="?")])
    rc.betfair_service.list_current_orders.return_value = [dict(H1, side="?")]
    rc._on_signal_received(dict(SIGNAL))
    assert _inviati(rc) == [] and rej, rej
    rc.betfair_service.list_current_orders.return_value = []
    rej.clear()
    rc._on_signal_received(dict(SIGNAL))
    assert rej == [] and len(_inviati(rc)) == 1


def test_ordini_di_questo_processo_non_entrano_nel_registro(tmp_path, monkeypatch):
    rc, rej = _avvia(tmp_path, monkeypatch, [H1])
    nuovo = dict(B1, betId="100000000099", marketId="1.777")
    rc.betfair_service.list_current_orders.return_value = [dict(H1), nuovo]
    rc._refresh_remote_exposure()
    assert rc.remote_exposure.bet_ids() == [H1["betId"]]


def test_prezzi_conservativi_lay_al_massimo_back_al_minimo_fra_medio_e_limite():
    # LAY abbinato 2.00 a medio 7.20 (limite 7.00): liability 6.2*2 = 12.40, non 12.00
    lay = dict(H1, sizeMatched=2.0, sizeRemaining=0.0, averagePriceMatched=7.2)
    assert market_worst_case(_n(lay)) == Decimal("12.40")
    # BACK 10 abbinato a medio 2.10 (limite 2.00) + LAY 2.86@7 abbinato: la vincita si conta a 2.00
    # vince: +10*(2.0-1) - 6*2.86 = -7.16 ; perde: -10 + 2.86 = -7.14 => 7.16
    back = dict(B1, averagePriceMatched=2.1)
    h = dict(H1, sizeMatched=2.86, sizeRemaining=0.0, averagePriceMatched=7.0)
    assert market_worst_case(_n(back, h)) == Decimal("7.16")


def test_rilettura_parziale_non_rilascia_mercati_non_letti():
    reg = RemoteExposureRegistry()
    altro = dict(B1, betId="100000000003", marketId="1.888")
    reg.seed([B1, altro])
    assert reg.refresh([], ["1.888"]) == ["100000000003"]
    assert reg.bet_ids() == [B1["betId"]] and reg.total() == 10.0


def test_residuo_back_a_riposo_non_compensa_la_liability():
    # LAY 2.00 abbinato a 5.00 (selezione vince: -8.00) + BACK 3.00 a riposo a 4.00 sulla stessa selezione.
    # Il residuo puo' non abbinarsi: il caso peggiore resta -8.00, non -8 + 3*3 = +1.
    lay = dict(H1, betId="100000000010", priceSize={"price": 5.0, "size": 2.0}, sizeMatched=2.0,
               sizeRemaining=0.0, averagePriceMatched=5.0)
    back = dict(B1, betId="100000000011", priceSize={"price": 4.0, "size": 3.0}, sizeMatched=0.0,
                sizeRemaining=3.0, averagePriceMatched=0.0)
    assert market_worst_case(_n(lay, back)) == Decimal("8.00")
