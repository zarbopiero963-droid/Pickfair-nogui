"""#546 D2 (P2) e #549 (P2): gate fondi su lettura fresca e cap evento per gli ingressi del DutchingController.

Catena reale (`_catena`): DutchingController -> RuntimeController (lock d'ammissione, tavoli, anti-duplicato,
gate fondi PR28-e) -> TradingEngine -> SimulationBroker, money management vero, cassa verificata 1000.

- D2: prima Dutching e bet manuale non rileggevano il conto. Con una cassa ancora vecchia (headless senza
  `get_status`, o dopo un settlement) l'unico freno era l'exchange: l'ordine partiva e veniva rifiutato a valle.
  Ora la lettura fresca si fa fuori dal lock e si confronta sotto il lock con tutte le riserve non ancora a
  mercato, compresa quella dell'ingresso: fondi insufficienti o non verificabili => nessun ordine, risorse
  rilasciate.
- #549: `manual_bet` non valutava `max_event_exposure_pct` (il precheck del Dutching e il MM dei segnali si').
- D1 (P3): uno zero senza `error` dopo un settlement e' un saldo letto, non un ripiego: motivo distinto,
  snapshot registrato, bankroll invariato.
"""
from __future__ import annotations

import threading

import pytest

from tests.acceptance.test_issue437_pr27 import _catena
from tests.acceptance.test_issue523_dutching_manual_bet import (
    _batch, _busy_tables, _dutching, _manual, _no_side_effects, _queueing)
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal

pytestmark = [pytest.mark.integration, pytest.mark.safety]


def _funds(c, **values):
    """Il conto letto all'ingresso (la cassa del RiskDesk resta quella vecchia, 1000)."""
    base = {"available": 1000.0, "exposure": 0.0, "total": 1000.0, "simulated": True}
    base.update(values)
    c.rc.betfair_service.get_account_funds = lambda: dict(base)


# ----------------------------------------------------------------------------- #549 cap evento sul manuale
def test_manual_bet_over_the_event_cap_is_rejected_without_side_effects(tmp_path):
    c = _catena(tmp_path, max_event_exposure_pct=5.0)       # cap evento 5% di 1000 = 50
    res = _dutching(c).manual_bet(_manual(stake=60.0))      # singolo 18% = 180 e totale 35% = 350 reggono
    assert res["ok"] is False and res["error"] == "Esposizione evento oltre limite", res   # prima: ok
    assert res["max_event_exposure"] == 50.0
    _no_side_effects(c)


def test_manual_bet_event_cap_counts_the_other_tables_of_the_same_event(tmp_path):
    c = _catena(tmp_path)                                    # cap evento 18% di 1000 = 180
    dc = _dutching(c)
    assert dc.manual_bet(_manual(selection_id=11, stake=150.0))["ok"] is True
    res = dc.manual_bet(_manual(selection_id=12, stake=50.0))  # stesso evento, altra selezione: 200 > 180
    assert res["ok"] is False and res["error"] == "Esposizione evento oltre limite", res
    assert res["event_current_exposure"] == 150.0
    assert len(c.bus.payloads("CMD_QUICK_BET")) == 1


def test_manual_bet_event_cap_counts_a_signal_table_on_the_same_event(tmp_path):
    c = _catena(tmp_path)
    c.rc._on_signal_received(_signal())                      # un ingresso da segnale sull'evento
    assert len(c.broker.state.orders) == 1
    signal_exposure = c.rc._event_current_exposure(_signal())
    assert signal_exposure > 0
    # Cap evento appena sopra l'esposizione del segnale: da solo il manuale (2) starebbe sotto.
    c.rc.config.max_event_exposure_pct = (signal_exposure + 1.0) / 10.0
    res = _dutching(c).manual_bet(_manual(selection_id=99, stake=2.0))
    assert res["ok"] is False and res["error"] == "Esposizione evento oltre limite", res
    assert res["event_current_exposure"] == pytest.approx(signal_exposure)


def test_manual_bet_under_the_event_cap_still_passes(tmp_path):
    c = _catena(tmp_path)
    assert _dutching(c).manual_bet(_manual(stake=150.0))["ok"] is True


def test_manual_lay_event_cap_uses_the_liability(tmp_path):
    c = _catena(tmp_path, max_event_exposure_pct=5.0)       # 50
    res = _dutching(c).manual_bet(_manual(bet_type="LAY", price=3.0, stake=30.0))   # liability 60
    assert res["ok"] is False and res["error"] == "Esposizione evento oltre limite"


# ----------------------------------------------------------------------------- D2 gate fondi freschi
def test_manual_bet_with_stale_cash_but_insufficient_fresh_funds_is_rejected(tmp_path):
    c = _catena(tmp_path)
    _funds(c, available=3.0, total=3.0)                      # il conto ora ha 3; la cassa vecchia dice 1000
    res = _dutching(c).manual_bet(_manual(stake=4.0))
    assert res["ok"] is False and str(res.get("reason", "")).startswith("fondi_insufficienti"), res  # prima: ok
    _no_side_effects(c)


def test_dutching_batch_with_insufficient_fresh_funds_is_rejected_whole(tmp_path):
    c = _catena(tmp_path)
    _funds(c, available=5.0, total=5.0)
    res = _dutching(c).submit_dutching(_batch(total_stake=10.0))
    assert res["ok"] is False and str(res.get("reason", "")).startswith("fondi_insufficienti"), res
    _no_side_effects(c)


@pytest.mark.parametrize("payload", [
    {"available": 1000.0, "exposure": 0.0, "total": 1000.0, "error": "BALANCE_FIELD_MISSING:exposure"},
    {"available": 0.0, "exposure": 0.0, "total": 0.0, "error": "no_client"},
])
def test_unverifiable_fresh_funds_reject_manual_and_batch(tmp_path, payload):
    c = _catena(tmp_path)
    c.rc.betfair_service.get_account_funds = lambda: dict(payload)
    dc = _dutching(c)
    assert dc.manual_bet(_manual())["reason"] == "fondi_non_verificabili"
    assert dc.submit_dutching(_batch())["reason"] == "fondi_non_verificabili"
    _no_side_effects(c)


def test_funds_read_exception_rejects_the_entry(tmp_path):
    c = _catena(tmp_path)

    def boom():
        raise RuntimeError("network down")
    c.rc.betfair_service.get_account_funds = boom
    assert _dutching(c).manual_bet(_manual())["reason"] == "fondi_non_verificabili"
    _no_side_effects(c)


def test_reservations_not_yet_at_market_count_against_fresh_funds(tmp_path):
    c = _catena(tmp_path)
    _funds(c, available=7.0, total=7.0)
    held, _run = _queueing(c)                               # il primo ordine resta accodato, non a mercato
    dc = _dutching(c)
    assert dc.manual_bet(_manual(selection_id=11, stake=4.0))["ok"] is True
    res = dc.manual_bet(_manual(selection_id=12, stake=4.0, event_name="Altro evento"))   # 4 + 4 > 7
    assert res["ok"] is False and str(res.get("reason", "")).startswith("fondi_insufficienti"), res
    assert len(held) == 1 and len(_busy_tables(c)) == 1


def test_sufficient_fresh_funds_still_pass(tmp_path):
    c = _catena(tmp_path)
    _funds(c, available=500.0, total=500.0)
    dc = _dutching(c)
    assert dc.manual_bet(_manual(stake=4.0))["ok"] is True
    assert dc.submit_dutching(_batch(total_stake=10.0))["ok"] is True


def test_the_funds_read_happens_outside_the_admission_lock(tmp_path):
    c = _catena(tmp_path)
    lock = c.rc._risk_admission_lock
    seen = []
    original = c.rc.betfair_service.get_account_funds

    def read():
        free = []
        t = threading.Thread(target=lambda: free.append(lock.acquire(timeout=1.0) and (lock.release() or True)))
        t.start()
        t.join()
        seen.append(free == [True])                         # un altro thread prende il lock: non e' trattenuto
        return original()
    c.rc.betfair_service.get_account_funds = read
    dc = _dutching(c)
    assert dc.manual_bet(_manual())["ok"] is True
    assert dc.submit_dutching(_batch())["ok"] is True
    assert seen and all(seen)


def test_runtime_without_the_funds_gate_rejects_controller_entries(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    monkeypatch.delattr(type(c.rc), "read_funds_for_controller_entry")   # runtime precedente: fail-closed
    assert _dutching(c).manual_bet(_manual())["reason"] == "fondi_non_verificabili"
    _no_side_effects(c)


# ----------------------------------------------------------------------------- D1 zero verificato dopo il settlement
def test_verified_zero_after_settlement_has_a_distinct_reason_and_fresh_snapshot(tmp_path):
    c = _catena(tmp_path)
    _funds(c, available=0.0, exposure=40.0, total=40.0)
    result = c.rc._sync_bankroll_post_settlement({"event_key": "evt-z", "batch_id": "b-z",
                                                  "correlation_id": "corr-z", "gross_pnl": -1.0})
    assert result["reason"] == "BALANCE_ZERO_VERIFIED"          # prima: BALANCE_ZERO_AMBIGUOUS_OR_FALLBACK
    assert result["bankroll_sync_status"] == "SYNC_FAILED_BALANCE_UNAVAILABLE"   # auto-next ancora saltato
    assert c.rc._account_snapshot["available"] == 0.0 and c.rc._account_snapshot["source"] == "settlement"
    assert float(c.rc.risk_desk.bankroll_current) == 1000.0      # nessun allineamento a 0 qui


def test_zero_with_an_error_after_settlement_stays_unverifiable(tmp_path):
    c = _catena(tmp_path)
    _funds(c, available=0.0, exposure=0.0, total=0.0, error="no_client")
    result = c.rc._sync_bankroll_post_settlement({"event_key": "evt-e", "batch_id": "b-e",
                                                  "correlation_id": "corr-e", "gross_pnl": -1.0})
    assert result["reason"] == "BALANCE_ERROR:no_client"


# ----------------------------------------------------------------------------- D2 casi di confine
@pytest.mark.parametrize("funds_read", [None, "x", ["funds"]])
def test_runtime_gate_rejects_a_missing_or_malformed_reading(tmp_path, funds_read):
    c = _catena(tmp_path)
    assert c.rc.controller_entry_funds_reason(funds_read) == "fondi_non_verificabili"


def test_without_a_table_the_entry_exposure_is_added_to_the_pending_ones(tmp_path, monkeypatch):
    """Senza TableManager nel controller l'ingresso non e' su un tavolo: la sua esposizione si somma a parte."""
    c = _catena(tmp_path)
    _funds(c, available=7.0, total=7.0)
    dc = _dutching(c)
    monkeypatch.setattr(dc, "_table_manager", lambda: None)
    res = dc.manual_bet(_manual(stake=8.0))                  # nessuna riserva pendente: 8 > 7 solo contando l'ingresso
    assert res["ok"] is False and str(res.get("reason", "")).startswith("fondi_insufficienti"), res
    res = dc.submit_dutching(_batch(total_stake=8.0))
    assert res["ok"] is False and str(res.get("reason", "")).startswith("fondi_insufficienti"), res
    assert c.bus.payloads("CMD_QUICK_BET") == []


@pytest.mark.parametrize("identity", [{}, {"event_name": ""}, {"event_name": "   "}])
def test_manual_bet_without_an_event_identity_is_rejected(tmp_path, identity):
    """Rilievo della review indipendente su 35abf40: senza nome/id evento la misura dei segnali vale 0 e un
    tavolo da segnale sullo stesso mercato non entrava nel cap evento (fail-open). Ora il cap non e'
    verificabile => rifiuto, prima di tavolo e chiave."""
    c = _catena(tmp_path)
    c.rc._on_signal_received(_signal())                      # esposizione evento 30 sul mercato 1.234
    c.rc.config.max_event_exposure_pct = 3.1                 # cap 31
    payload = _manual(selection_id=99, stake=2.0)
    payload.pop("event_name")
    payload.update(identity)
    res = _dutching(c).manual_bet(payload)
    assert res["ok"] is False and res["error"] == "Esposizione evento non verificabile", res
    assert len(c.bus.payloads("CMD_QUICK_BET")) == 1           # solo il segnale


def test_manual_bet_event_cap_counts_the_tables_of_the_same_market(tmp_path):
    """Un tavolo manuale sullo stesso mercato con un nome evento diverso resta nel cap evento."""
    c = _catena(tmp_path)
    dc = _dutching(c)
    assert dc.manual_bet(_manual(selection_id=11, stake=150.0))["ok"] is True
    res = dc.manual_bet(_manual(selection_id=12, stake=50.0, event_name="Alpha - Beta"))   # 200 > 180
    assert res["ok"] is False and res["error"] == "Esposizione evento oltre limite", res


def test_manual_bet_event_cap_sums_the_union_of_event_and_market_tables(tmp_path):
    """Rilievo Sol su 743bf8f: con `max(evento, mercato)` due tavoli diversi (stesso evento su un altro
    mercato; stesso mercato con nome scritto diversamente) contavano 40 invece di 80. Ora si somma l'unione."""
    c = _catena(tmp_path, max_event_exposure_pct=10.0)       # cap evento 100
    dc = _dutching(c)
    assert dc.manual_bet(_manual(market_id="1.500", selection_id=1, stake=40.0))["ok"] is True
    assert dc.manual_bet(_manual(selection_id=2, stake=40.0, event_name="Alpha - Beta"))["ok"] is True
    res = dc.manual_bet(_manual(selection_id=3, stake=30.0))  # 40 + 40 + 30 = 110 > 100
    assert res["ok"] is False and res["error"] == "Esposizione evento oltre limite", res
    assert res["event_current_exposure"] == 80.0


def test_verified_zero_after_settlement_recovers_when_funds_return(tmp_path):
    """Rilievo Codex su 743bf8f: la cassa invalidata dallo zero bloccava le entrate fino a un'altra sincronizzazione,
    anche con fondi tornati. Phase 2: lo zero rende la cassa non verificata con un motivo esplicito
    (BALANCE_ZERO_COMMITTED, mai CASH_READING_MISMATCH) e la prima lettura d'ingresso valida la riallinea."""
    c = _catena(tmp_path)
    _funds(c, available=0.0, exposure=1000.0, total=1000.0)
    c.rc._sync_bankroll_post_settlement({"event_key": "evt-m", "batch_id": "b-m",
                                         "correlation_id": "corr-m", "gross_pnl": -1.0})
    assert c.rc.risk_desk.cash_equity_reason == "BALANCE_ZERO_COMMITTED"
    assert float(c.rc.risk_desk.bankroll_current) == 1000.0
    vuoto = _dutching(c).manual_bet(_manual())                                 # conto ancora vuoto
    assert vuoto["ok"] is False and vuoto["reason"] == "cassa_non_verificata", vuoto
    _funds(c)                                                  # i fondi tornano: 1000 disponibili
    assert _dutching(c).manual_bet(_manual())["ok"] is True
