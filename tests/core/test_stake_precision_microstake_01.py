"""PF-STAKE-PRECISION-MICROSTAKE-01 (decisione owner 24/10/2026) — stake per difetto al centesimo, Microstake preservato.

Difetto riprodotto dalla sessione integrata della Fase 4 e poi sulla catena reale (RED sul codice di main 7bd9560):
- il money management chiedeva stake come 1.9647599999999998: il Microstake li bloccava (invalid_size_precision),
  quindi un ingresso Roserpina sotto 2 EUR falliva sempre dopo un settlement;
- sopra 2 EUR l'exchange SIM riceveva 3.08595, un importo che Betfair non esegue cosi' (massimo due decimali);
- 0.1 + 0.2 = 0.30000000000000004 era bloccato per precisione pur valendo 0,30.

Regola: stake inviato = stake calcolato per difetto al centesimo (core.stake_precision), mai piu' alto, nessun minimo
nuovo; il percorso (diretto o Microstake) resta deciso dall'importo. Componenti reali: money management, catena
RuntimeController -> TradingEngine -> SimulationBroker con Microstake nativo SIM, coordinatore Microstake con il
simulatore di stato, DutchingController per la bet manuale, OrderRouter per l'hedge.
"""
from __future__ import annotations

import threading
from decimal import Decimal as D

import pytest

from core import stake_precision
from core.money_management import RoserpinaMoneyManagement
from tests.acceptance.test_issue437_pr27 import _catena
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal
from tests.risk_config_fixtures import legacy_test_config
from tests.test_microstake_native_integration import _DirectClient, _engine, _MicroExecutor, _native


# ------------------------------------------------------------------------------------------- contratto comune
@pytest.mark.parametrize("calcolato,atteso", [
    (0.10599, "0.10"), (0.50999, "0.50"), (1.96476, "1.96"), (2.00000, "2.00"), (3.08595, "3.08"),
    (10.99999, "10.99"),                                     # tabella del mandato owner
    (1.9647599999999998, "1.96"), (3.0859499999999995, "3.08"),   # valori reali osservati nella Fase 4
])
def test_owner_table_is_floored_to_the_cent(calcolato, atteso):
    assert stake_precision.normalize_stake(calcolato) == D(atteso)


@pytest.mark.parametrize("valore,atteso", [
    (0.1, "0.10"),                       # la forma decimale del float, non 0.1000000000000000055...
    (0.30000000000000004, "0.30"), (0.1 + 0.2, "0.30"),
    (D("0.1") * 3, "0.30"),              # 0,30 da operandi decimali esatti
    ("0.30", "0.30"),                    # testo del produttore, senza passare dal float
])
def test_float_values_are_read_from_their_decimal_form(valore, atteso):
    assert stake_precision.normalize_stake(valore) == D(atteso)


@pytest.mark.parametrize("reale,atteso", [
    (1.9999999999, "1.99"),              # FINAL-01: autentico => 1,99, nessuna tolleranza verso 2,00
    (2.0 - 1e-12, "1.99"), (0.09999999999999999, "0.09"),     # sotto il centesimo resta sotto: mai alzato
    (0.099999, "0.09"), (0.10999, "0.10"), (1.999, "1.99"), (0.00999, "0.00"),
])
def test_no_tolerance_ever_raises_an_amount(reale, atteso):
    assert stake_precision.normalize_stake(reale) == D(atteso)


def test_normalization_is_idempotent_and_never_increases_the_amount():
    for x in (0.10599, 0.3, 1.96476, 3.08595, 7.777777, 123.456789, 0.1 + 0.2):
        once = stake_precision.normalize_stake(x)
        assert stake_precision.normalize_stake(once) == once
        assert stake_precision.normalize_stake(float(once)) == once
        assert once <= stake_precision.to_decimal(x)


@pytest.mark.parametrize("cattivo", [float("nan"), float("inf"), -0.5, True, None, "abc", 1e308])
def test_invalid_amounts_are_rejected_not_invented(cattivo):
    with pytest.raises(ValueError):
        stake_precision.normalize_stake(cattivo)


# ------------------------------------------------------------------------------------------- money management
def _mm(**over):
    cfg = legacy_test_config()
    cfg.max_single_bet_pct = cfg.max_total_exposure_pct = cfg.max_event_exposure_pct = 100.0
    cfg.target_profit_cycle_pct = 3.0
    cfg.min_stake = 0.10
    for k, v in over.items():
        setattr(cfg, k, v)
    return RoserpinaMoneyManagement(cfg)


def _calc(mm, *, bankroll=100.0, price=2.0, side="BACK", stake=None):
    signal = {"price": price, "side": side, "bet_type": side}
    if stake is not None:
        signal["stake"] = stake
    return mm.calculate(signal=signal, bankroll_current=bankroll, cap_base_cash=bankroll, equity_peak=bankroll,
                        current_total_exposure=0.0, event_current_exposure=0.0, table=None)


def test_mm_computed_stake_is_requested_floored_and_the_formula_is_unchanged():
    # bankroll 65.492, target 3%, quota 2.0 => calcolato 1.96476 (formula invariata), richiesto 1.96
    d = _calc(_mm(), bankroll=65.492)
    assert d.approved is True
    assert d.recommended_stake == pytest.approx(1.96, abs=1e-12)
    assert d.metadata["base_stake"] == pytest.approx(1.96476, abs=1e-9)
    assert d.metadata["stake_normalizzato"] == "1.96"
    assert d.metadata["motivo_normalizzazione"] == stake_precision.REASON_FLOORED
    assert float(d.metadata["stake_calcolato"]) == pytest.approx(1.96476, abs=1e-9)


@pytest.mark.parametrize("fisso,atteso", [(3.08595, 3.08), (0.10599, 0.10), (0.10, 0.10), (2.0, 2.0)])
def test_mm_fixed_signal_stake_is_floored_never_raised(fisso, atteso):
    d = _calc(_mm(), stake=fisso)
    assert d.approved is True and d.recommended_stake == pytest.approx(atteso, abs=1e-12)
    assert d.recommended_stake <= fisso + 1e-12


def test_mm_binary_artifact_is_fixed_at_the_source_not_with_a_tolerance():
    # bankroll 3.3333333333333335 * 3%: in float 0.09999999999999999 (troncato sarebbe 0,09); sugli operandi
    # in Decimal 0.100000000000000005 => 0,10. Il valore e' quello esatto degli operandi, poi si tronca.
    d = _calc(_mm(), bankroll=10.0 / 3.0)
    assert d.approved is True and d.recommended_stake == pytest.approx(0.10, abs=1e-12)
    assert d.metadata["stake_calcolato"] == "0.100000000000000005"


def test_mm_float_formula_no_longer_rounds_up_silently():
    # bankroll 23.333333333333332 * 3%: il float dava 0.7 (arrotondamento verso l'alto nascosto); il valore
    # esatto degli operandi e' 0.69999999999999996 => 0,69.
    d = _calc(_mm(), bankroll=70.0 / 3.0)
    assert d.recommended_stake == pytest.approx(0.69, abs=1e-12)


def test_mm_exact_operands_give_exact_cents():
    assert _calc(_mm(), bankroll=10.0).recommended_stake == 0.3           # 10 * 3% = 0,30 esatto
    assert _calc(_mm(), bankroll=100.0).recommended_stake == 3.0


def test_mm_floored_below_the_configured_minimum_is_an_explicit_rejection():
    d = _calc(_mm(), stake=0.099999)                       # 0.09 normalizzato < minimo 0.10: mai alzato a 0.10
    assert d.approved is False and d.reason == "stake_sotto_min_stake"
    assert float(d.metadata["requested_stake"]) == pytest.approx(0.099999)
    assert d.metadata["stake_normalizzato"] == "0.09"


def test_mm_lay_liability_and_caps_use_the_floored_stake():
    d = _calc(_mm(), stake=0.10599, side="LAY", price=3.0)
    assert d.approved is True and d.recommended_stake == pytest.approx(0.10)
    assert d.metadata["exposure"] == pytest.approx(0.20, abs=1e-12)      # 0.10 * (3 - 1), non 0.21198
    capped = _calc(_mm(max_single_bet_pct=3.0), stake=3.00999)          # cap 3.00: 3.00 normalizzato non lo supera
    assert capped.approved is True and capped.recommended_stake == pytest.approx(3.0)


# ------------------------------------------------------------------------------------------- catena reale (SIM)
def _orders(c):
    return [(round(o.size, 10), o.side) for o in c.broker.state.orders.values()]


@pytest.mark.parametrize("stake,atteso", [(3.08595, 3.08), (1.96476, 1.96), (0.10599, 0.10), (0.1 + 0.2, 0.30),
                                          (0.10, 0.10)])
def test_signal_stake_reaches_the_sim_exchange_floored_on_the_right_path(tmp_path, stake, atteso):
    c = _catena(tmp_path)
    c.rc._on_signal_received(_signal(stake=stake))
    (cmd,) = c.bus.payloads("CMD_QUICK_BET")
    assert cmd["stake"] == pytest.approx(atteso, abs=1e-12)
    assert _orders(c) == [(atteso, "BACK")], "prima: 3.08595 all'exchange, sotto 2 EUR nessun ordine (precisione)"
    assert not c.bus.payloads("QUICK_BET_FAILED")


def test_reservation_is_the_stake_actually_sent(tmp_path):
    c = _catena(tmp_path)
    c.rc._on_signal_received(_signal(stake=3.08595))
    (tavolo,) = [t for t in c.rc.table_manager._tables.values() if t.status != "FREE"]
    assert float(tavolo.current_exposure) == pytest.approx(3.08, abs=1e-12)   # non 3.08595: nessuna riserva fantasma


def test_lay_microstake_ten_cents_has_the_right_liability_on_the_exchange(tmp_path):
    c = _catena(tmp_path)
    c.rc._on_signal_received(_signal(stake=0.10, bet_type="LAY", price=3.0))
    assert _orders(c) == [(0.10, "LAY")]
    funds = c.broker.get_account_funds()
    assert funds["exposure"] == pytest.approx(0.20, abs=1e-9)                  # liability 0.10 * (3 - 1)


def test_two_simultaneous_microstakes_commit_funds_once_each(tmp_path):
    c = _catena(tmp_path)
    before = c.broker.get_account_funds()
    barrier = threading.Barrier(2)

    def go(market):
        barrier.wait()
        c.rc._on_signal_received(_signal(stake=0.10599, market_id=market, event=f"Ev{market} v X"))
    threads = [threading.Thread(target=go, args=(m,)) for m in ("1.801", "1.802")]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert sorted(_orders(c)) == [(0.10, "BACK"), (0.10, "BACK")]
    after = c.broker.get_account_funds()
    assert after["exposure"] == pytest.approx(0.20, abs=1e-9), "stub temporanei da 2 EUR non rilasciati o doppio impegno"
    assert before["available"] - after["available"] == pytest.approx(0.20, abs=1e-9)


def test_manual_bet_is_floored_before_its_reservation(tmp_path):
    from controllers.dutching_controller import DutchingController

    c = _catena(tmp_path)
    res = DutchingController(bus=c.bus, runtime_controller=c.rc).manual_bet({
        "market_id": "1.234", "selection_id": 11, "price": 2.0, "stake": 3.08595, "bet_type": "BACK",
        "event_name": "Alpha v Beta", "simulation_mode": True})
    assert res["ok"] is True, res
    (cmd,) = c.bus.payloads("CMD_QUICK_BET")
    assert cmd["stake"] == pytest.approx(3.08)
    (tavolo,) = [t for t in c.rc.table_manager._tables.values() if t.status != "FREE"]
    assert float(tavolo.current_exposure) == pytest.approx(3.08, abs=1e-12)
    assert _orders(c) == [(3.08, "BACK")]


# ------------------------------------------------------------------------------------------- engine (choke point)
def test_engine_floors_a_direct_stake_and_records_both_amounts():
    engine, direct = _engine(_MicroExecutor())
    res = engine.submit_quick_bet({"market_id": "1.100", "selection_id": 7, "side": "BACK", "price": 2.0,
                                   "size": 3.08595, "customer_ref": "prec-direct"})
    assert res["ok"] is True
    assert direct.calls[0]["size"] == pytest.approx(3.08, abs=1e-12)    # payload verso il client (SIM e LIVE)
    (row,) = engine.db.rows.values()
    assert row["payload"]["stake_precision"] == {"stake_calcolato": "3.08595", "stake_normalizzato": "3.08",
                                      "motivo_normalizzazione": "troncato_al_centesimo", "modalita_ordine": "DIRETTO"}


def test_engine_authentic_amount_just_below_two_euro_is_1_99_on_the_microstake_path():
    # FINAL-01: 1,9999999999 autentico => 1,99, poi la scelta del percorso sull'importo finale (Microstake).
    micro = _MicroExecutor()
    engine, direct = _engine(micro)
    res = engine.submit_quick_bet({"market_id": "1.100", "selection_id": 7, "side": "BACK", "price": 2.0,
                                   "size": 1.9999999999, "customer_ref": "prec-1999"})
    assert res["ok"] is True and direct.calls == [] and micro.calls[0]["size"] == 1.99
    engine2, direct2 = _engine(_MicroExecutor())
    engine2.submit_quick_bet({"market_id": "1.100", "selection_id": 7, "side": "BACK", "price": 2.0,
                              "size": 2.0, "customer_ref": "prec-200"})
    assert direct2.calls[0]["size"] == 2.0                                  # 2,00 esatto resta diretto


def test_engine_routes_a_floored_sub_minimum_stake_to_microstake():
    micro = _MicroExecutor()
    engine, direct = _engine(micro)
    engine.submit_quick_bet({"market_id": "1.100", "selection_id": 7, "side": "BACK", "price": 2.0,
                             "size": 1.96476, "customer_ref": "prec-micro"})
    assert direct.calls == [] and micro.calls[0]["size"] == pytest.approx(1.96, abs=1e-12)


def test_engine_below_one_cent_is_an_explicit_invalid_request():
    engine, direct = _engine(_MicroExecutor())
    res = engine.submit_quick_bet({"market_id": "1.100", "selection_id": 7, "side": "BACK", "price": 2.0,
                                   "size": 0.004, "customer_ref": "prec-zero"})
    assert res["ok"] is False and res["status"] == "FAILED" and direct.calls == []


def test_native_microstake_saga_executes_the_floored_amount_exactly(tmp_path):
    native, exchange = _native(tmp_path)
    engine, direct = _engine(native)
    res = engine.submit_quick_bet({"market_id": "1.100", "selection_id": 7, "side": "BACK", "price": 2.0,
                                   "size": 0.10599, "customer_ref": "prec-saga"})
    assert res["ok"] is True, res                                         # prima: invalid_size_precision
    assert [op[0] for op in exchange.operation_log] == ["PLACE", "CANCEL", "REPLACE"]
    assert direct.calls == []


def test_native_microstake_with_insufficient_funds_is_still_refused_for_funds(tmp_path):
    native, exchange = _native(tmp_path, cap="1.00")                     # stub 2 EUR oltre il disponibile
    engine, _direct = _engine(native)
    res = engine.submit_quick_bet({"market_id": "1.100", "selection_id": 7, "side": "BACK", "price": 2.0,
                                   "size": 0.10599, "customer_ref": "prec-funds"})
    assert res["ok"] is False and "transient_exposure_cap" in str(res.get("error"))
    assert exchange.operation_log == []


# ------------------------------------------------------------------------------------------- hedge del cashout
class _Service:
    def __init__(self):
        self.client = _DirectClient()

    def get_client(self):
        return self.client


def test_protective_hedge_sizing_is_unchanged_by_this_fix():
    """FINAL-01: l'hedge non e' un nuovo stake d'ingresso: niente ROUND_DOWN, il router lo passa com'e'."""
    from core.order_router import OrderRouter

    micro = _MicroExecutor()
    svc = _Service()
    router = OrderRouter(svc, microstake_executor_provider=lambda: micro)
    router.place({"market_id": "1.1", "selection_id": 7, "bet_type": "LAY", "price": 3.0, "stake": 2.345,
                  "customer_ref": "h"})
    assert svc.client.calls[0]["size"] == 2.345
    router.place({"market_id": "1.1", "selection_id": 7, "bet_type": "LAY", "price": 3.0, "stake": 0.49,
                  "customer_ref": "h2"})
    assert micro.calls[0]["stake"] == 0.49


# ------------------------------------------------------------------------------------------- cashout di un Microstake
def test_microstake_position_keeps_its_event_for_the_cashout_identity():
    """Cashout Microstake senza blocco improprio. L'ordine economico del Microstake (nuovo betId del REPLACE) e'
    nel ledger SIM senza event_name (la saga piazza via porta SIM, senza metadati) e nella tabella `orders` con
    l'evento. Prima la deduplica per bet_id scartava la riga con l'evento: un CASHOUT della partita non trovava
    la posizione (published 0, positions 0) e la lasciava aperta."""
    from database import Database

    db = Database(":memory:")
    db.save_simulation_bet({"bet_id": "MS-NEW", "market_id": "1.561", "selection_id": "11", "side": "BACK",
                            "status": "EXECUTION_COMPLETE", "event_name": ""})
    oid = db.insert_order({"customer_ref": "sig-1", "correlation_id": "c-1", "status": "INFLIGHT",
                           "payload": {"market_id": "1.561", "event_name": "Ev1.561 v X", "selection_id": 11}})
    db.update_order(oid, {"status": "SUBMITTED", "response": {"instructionReports": [{"betId": "MS-NEW"}]}})
    assert db.get_bot_active_orders() == [{"bet_id": "MS-NEW", "market_id": "1.561", "event_name": "Ev1.561 v X"}]


# ------------------------------------------------------------------------------------------- contratto LIVE (no denaro)
def test_live_wire_payload_carries_the_floored_size_with_two_decimals():
    """Stessa precisione in SIM e LIVE: payload placeOrders del BetfairClient reale (trasporto intercettato, nessuna
    chiamata a Betfair). Prima sul wire andava 3.08595. Prova di payload e routing, NON di accettazione Betfair."""
    import json

    from betfair_client import BetfairClient
    from core.trading_engine import TradingEngine

    class _NoSession:
        def post(self, *a, **k):  # pragma: no cover - mai raggiunta
            raise AssertionError("nessuna chiamata reale")

    client = BetfairClient(username="u", app_key="a", cert_pem="c", key_pem="k", session=_NoSession())
    sent = {}
    client._post_jsonrpc = lambda url, method, params, single_shot=False: (
        sent.update(params=params) or {"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS"}]})
    engine, _direct = _engine(_MicroExecutor())
    request = engine._normalize_request({"market_id": "1.123", "selection_id": 47, "side": "BACK", "price": 2.0,
                                         "stake": 3.08595, "customer_ref": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"})
    client.place_bet(**TradingEngine._kwargs_per_place_bet(client.place_bet, request))
    (instruction,) = sent["params"]["instructions"]
    assert instruction["limitOrder"]["size"] == 3.08
    assert '"size": 3.08' in json.dumps(sent["params"])


# ------------------------------------------------------------------------------------------- Dutching e hedge invariati
@pytest.mark.parametrize("total,odds", [(1.0, [1.5, 30.0]), (1.0, [30.0, 1.5]), (6.0, [2.5, 3.5]),
                                        (2.0, [3.0, 3.0, 40.0]), (10.0, [2.0, 3.0, 5.0])])
def test_dutching_legs_are_already_cents_so_this_fix_changes_nothing(total, odds):
    """FINAL-01 punto 4: la formula del Dutching non si tocca. Le gambe escono gia' al centesimo (HALF_UP), la
    somma e' il totale (l'ultima gamba assorbe la differenza): la normalizzazione e' l'identita' su ogni gamba.
    L'elevazione a 0,10 di `_initial_dutching_stakes` viene riassorbita dall'ultima gamba e dall'equalizzazione:
    una gamba puo' uscire sotto 0,10 (es. 0,05) e allora fallisce al minimo del Microstake (lotto parziale,
    PF-EXEC-02) — comportamento preesistente, registrato in docs/microstake_native.md come decisione separata."""
    import dutching

    legs = [D(str(x)) for x in dutching.calculate_dutching_stakes(odds, total)["stakes"]]
    assert sum(legs) == D(str(total))
    assert all(stake_precision.normalize_stake(leg) == leg for leg in legs)


@pytest.mark.parametrize("stake,entry,cur", [(0.10, 2.0, 2.02), (1.5, 2.0, 2.02), (3.0, 2.0, 2.02), (10.0, 2.0, 1.5),
                                             (0.37, 3.4, 2.9)])
def test_hedge_sizing_is_unchanged_cent_exact_and_its_residual_is_bounded(stake, entry, cur):
    """FINAL-01 punto 4: l'hedge resta al centesimo piu' vicino (non ROUND_DOWN). Verifica: importo al centesimo
    (la normalizzazione e' l'identita'), esposizione residua dopo la copertura entro mezzo centesimo per la quota
    di chiusura (l'arrotondamento al piu' vicino e' la copertura piu' stretta possibile al centesimo)."""
    from cashout_resolver import build_cashout_request

    req = build_cashout_request({"market_id": "1.1", "selection_id": 7, "side": "BACK", "stake": stake,
                                 "price": entry}, current_price=cur, commission=4.5)
    hedge = D(str(req["stake"]))
    assert stake_precision.normalize_stake(hedge) == hedge
    win = D(str(stake)) * (D(str(entry)) - 1) - hedge * (D(str(cur)) - 1)     # selezione vince
    lose = -D(str(stake)) + hedge                                             # selezione perde
    assert abs(win - lose) <= D("0.005") * D(str(cur)) + D("1e-12"), (win, lose)
