"""PF-MICROSTAKE-OWNER-FINAL-RECTIFICATION-01 (decisione owner 24/10/2026) — adapter Microstake LIVE, certificazione
offline.

Componenti REALI: `TradingEngine` (ramo LIVE, gate `is_live_allowed`), esecutore e saga Microstake, porta LIVE,
`BetfairClient` (validazione, payload, classificazione degli esiti). Solo il trasporto JSON-RPC e' sostituito da un
exchange Betfair finto e con stato (`_FakeBetfair`): placeOrders, cancelOrders con sizeReduction, replaceOrders con
nuovo betId, listCurrentOrders, listMarketBook, rifiuto sotto il minimo (1 EUR, riferimento owner), fondi, guasti
(risposta persa, crash del processo). Nessuna rete, nessun denaro reale: prova di logica e di contratto, NON di
accettazione Betfair (da verificare nel collaudo reale autorizzato dall'owner).
"""
from __future__ import annotations

import itertools
import os
import tempfile
import threading
from decimal import Decimal as D
from types import SimpleNamespace

import pytest

from betfair_client import BetfairClient
from core.microstake_live_port import (LIVE_SAGA_SETTINGS_KEY, LiveExchangePort, LiveQuoteProvider,
                                       build_live_microstake_executor)
from core.microstake_safe import MicrostakeCoordinator, MicrostakePolicy, MicrostakeRequest, Phase, Side
from core.microstake_sim_port import SAGA_SETTINGS_KEY, DbSagaStore
from core.money_management import RoserpinaMoneyManagement
from core.trading_engine import TradingEngine
from tests.risk_config_fixtures import legacy_test_config
from database import Database
from tests.test_microstake_engine_native import _DB, _Bus, _Inline

MIN = D("1.00")                       # minimo exchange modellato: riferimento owner (core.stake_precision)


class _Crash(BaseException):
    """Arresto del processo durante una chiamata (non e' un Exception: nessun handler applicativo lo intercetta)."""


# ------------------------------------------------------------------------------------------- exchange Betfair finto
class _FakeBetfair:
    def __init__(self, *, balance="100", carry_ref=True, keep_dead=False):
        self.balance = D(balance)
        self.carry_ref = carry_ref            # il replace conserva customerOrderRef? (da verificare su Betfair reale)
        self.keep_dead = keep_dead            # un ordine cancellato senza abbinato resta fra i correnti? (idem)
        self.reject_replace = False
        self.orders = {}
        self.ids = itertools.count(1)
        self.books = {}
        self.market_status = "OPEN"
        self.calls = []
        self.lose = set()                     # metodo => esito applicato, risposta persa (timeout)
        self.crash = set()                    # metodo => esito applicato, processo arrestato
        self.hold = None                      # (metodo, evento_entrato, evento_rilascio): blocca dentro la chiamata
        self.lock = threading.RLock()

    # -- book e stato
    def book(self, market, selection, *, back=None, lay=None):
        self.books[(market, int(selection))] = {"availableToBack": [dict(price=p, size=s) for p, s in back or []],
                                                "availableToLay": [dict(price=p, size=s) for p, s in lay or []]}

    def live(self):
        return [o for o in self.orders.values() if not self._dead(o)]

    @staticmethod
    def _dead(o):
        return o["status"] == "EXECUTION_COMPLETE" and o["matched"] == 0

    def exposure(self):
        total = D("0")
        for o in self.orders.values():
            size = o["matched"] + o["remaining"]
            total += size if o["side"] == "BACK" else size * (o["price"] - 1)
        return total

    def _match(self, o):
        levels = self.books.get((o["marketId"], o["selectionId"]), {})
        ladder = levels.get("availableToBack" if o["side"] == "BACK" else "availableToLay", [])
        for level in sorted(ladder, key=lambda lv: -lv["price"] if o["side"] == "BACK" else lv["price"]):
            price = D(str(level["price"]))
            crosses = price >= o["price"] if o["side"] == "BACK" else price <= o["price"]
            if not crosses or o["remaining"] == 0:
                continue
            take = min(o["remaining"], D(str(level["size"])))
            o["matched"] += take
            o["remaining"] -= take
        if o["remaining"] == 0:
            o["status"] = "EXECUTION_COMPLETE"

    def _row(self, o):
        row = {"betId": o["betId"], "marketId": o["marketId"], "selectionId": o["selectionId"], "side": o["side"],
               "priceSize": {"price": float(o["price"]), "size": float(o["size"])}, "status": o["status"],
               "sizeMatched": float(o["matched"]), "sizeRemaining": float(o["remaining"]),
               "sizeCancelled": float(o["cancelled"])}
        if o["ref"]:
            row["customerOrderRef"] = o["ref"]
        return row

    # -- trasporto JSON-RPC (firma di BetfairClient._post_jsonrpc)
    def __call__(self, url, method, params, single_shot=False):
        name = method.split("/")[-1]
        with self.lock:
            self.calls.append((name, params, single_shot))
            result = getattr(self, "_" + name)(params)
        if self.hold and self.hold[0] == name:
            entered, release = self.hold[1], self.hold[2]
            self.hold = None
            entered.set()
            release.wait(5)
        if name in self.crash:
            self.crash.discard(name)
            raise _Crash(name)
        if name in self.lose:
            self.lose.discard(name)
            raise RuntimeError("REQUEST_FAILED: TIMEOUT")
        return result

    def _placeOrders(self, params):
        (ins,) = params["instructions"]
        size, price = D(str(ins["limitOrder"]["size"])), D(str(ins["limitOrder"]["price"]))
        fail = None
        if self.market_status != "OPEN":
            fail = "MARKET_NOT_OPEN_FOR_BETTING"
        elif size < MIN:
            fail = "INVALID_BET_SIZE"
        else:
            need = size if ins["side"] == "BACK" else size * (price - 1)
            if self.exposure() + need > self.balance:
                fail = "INSUFFICIENT_FUNDS"
        if fail:
            return {"status": "FAILURE", "errorCode": "BET_ACTION_ERROR",
                    "instructionReports": [{"status": "FAILURE", "errorCode": fail}]}
        bet_id = str(next(self.ids))
        o = {"betId": bet_id, "marketId": params["marketId"], "selectionId": int(ins["selectionId"]),
             "side": ins["side"], "price": price, "size": size, "matched": D("0"), "remaining": size,
             "cancelled": D("0"), "status": "EXECUTABLE", "ref": ins.get("customerOrderRef", "")}
        self._match(o)
        self.orders[bet_id] = o
        return {"status": "SUCCESS", "instructionReports": [
            {"status": "SUCCESS", "betId": bet_id, "sizeMatched": float(o["matched"]), "orderStatus": o["status"]}]}

    def _cancelOrders(self, params):
        reports = []
        for ins in params["instructions"]:
            o = self.orders.get(ins["betId"])
            if o is None or o["remaining"] == 0:
                reports.append({"status": "FAILURE", "errorCode": "BET_TAKEN_OR_LAPSED"})
                continue
            reduction = D(str(ins["sizeReduction"])) if "sizeReduction" in ins else o["remaining"]
            if reduction > o["remaining"]:
                reports.append({"status": "FAILURE", "errorCode": "INVALID_BET_SIZE"})
                continue
            o["remaining"] -= reduction
            o["cancelled"] += reduction
            if o["remaining"] == 0:
                o["status"] = "EXECUTION_COMPLETE"
            reports.append({"status": "SUCCESS", "sizeCancelled": float(reduction)})
        return {"status": "SUCCESS", "instructionReports": reports}

    def _replaceOrders(self, params):
        (ins,) = params["instructions"]
        old = self.orders.get(ins["betId"])
        price = D(str(ins["newPrice"]))
        if self.reject_replace or old is None or old["remaining"] == 0 or old["price"] == price:
            return {"status": "FAILURE", "instructionReports": [{"status": "FAILURE"}]}
        size = old["remaining"]
        old["cancelled"] += size
        old["remaining"] = D("0")
        old["status"] = "EXECUTION_COMPLETE"
        bet_id = str(next(self.ids))
        new = {"betId": bet_id, "marketId": old["marketId"], "selectionId": old["selectionId"], "side": old["side"],
               "price": price, "size": size, "matched": D("0"), "remaining": size, "cancelled": D("0"),
               "status": "EXECUTABLE", "ref": old["ref"] if self.carry_ref else ""}
        self._match(new)
        self.orders[bet_id] = new
        return {"status": "SUCCESS", "instructionReports": [{
            "status": "SUCCESS", "cancelInstructionReport": {"status": "SUCCESS", "betId": old["betId"]},
            "placeInstructionReport": {"status": "SUCCESS", "betId": bet_id}}]}

    def _listCurrentOrders(self, params):
        wanted = set(params.get("marketIds") or [])
        rows = [self._row(o) for o in self.orders.values()
                if (self.keep_dead or not self._dead(o)) and (not wanted or o["marketId"] in wanted)]
        return {"currentOrders": rows, "moreAvailable": False}

    def _listClearedOrders(self, params):
        """Ordini spariti dai correnti senza abbinato: CANCELLED (o LAPSED se segnato), come listClearedOrders."""
        wanted = set(params.get("betIds") or [])
        rows = []
        for o in self.orders.values():
            if not self._dead(o) or (wanted and o["betId"] not in wanted):
                continue
            status = "LAPSED" if o.get("lapsed") else "CANCELLED"
            if status != params["betStatus"]:
                continue
            row = {"betId": o["betId"], "marketId": o["marketId"], "selectionId": o["selectionId"],
                   "side": o["side"], "priceRequested": float(o["price"]), "sizeCancelled": float(o["cancelled"])}
            if o["ref"]:
                row["customerOrderRef"] = o["ref"]
            rows.append(row)
        return {"clearedOrders": rows, "moreAvailable": False}

    def _listMarketBook(self, params):
        (market,) = params["marketIds"]
        empty = {"availableToBack": [], "availableToLay": []}
        runners = [{"selectionId": sel, "status": "ACTIVE", "ex": dict(self.books.get((market, sel), empty))}
                   for sel in range(1, 21)]
        return [{"marketId": market, "status": self.market_status, "runners": runners}]

    def available(self):
        return {"ok": True, "available": float(self.balance - self.exposure()), "exposure": float(self.exposure())}


class _NoSession:
    def post(self, *a, **k):  # pragma: no cover - mai raggiunta
        raise AssertionError("nessuna chiamata di rete reale")


def _client(fake):
    client = BetfairClient(username="u", app_key="a", cert_pem="c", key_pem="k", session=_NoSession())
    client._post_jsonrpc = fake
    return client


def _runtime(fake, *, live_allowed=True, funds=None):
    cfg = legacy_test_config()
    cfg.max_win = 10000.0
    return SimpleNamespace(
        get_effective_execution_mode=lambda: "LIVE",
        is_live_allowed=lambda: live_allowed,
        betfair_service=SimpleNamespace(_session_invalid=False,
                                        get_account_funds=funds or fake.available),
        config=cfg, mm=RoserpinaMoneyManagement(cfg),
        risk_desk=SimpleNamespace(bankroll_current=100.0, cash_equity_current=100.0, cash_committed_exposure=0.0),
        table_manager=SimpleNamespace(total_exposure=lambda: 0.0),
    )


def _db():
    # Database reale (SQLite su disco): il ramo LIVE esige la deduplica durevole degli intenti.
    return Database(os.path.join(tempfile.mkdtemp(prefix="ms_live_"), "pickfair.db"))


def _settings(db):
    return db.get_settings() or {}


def _live_engine(fake=None, *, db=None, client=None, **rt):
    fake = fake or _FakeBetfair()
    db = db or _db()
    client = client or _client(fake)
    engine = TradingEngine(bus=_Bus(), db=db, client_getter=lambda: client, executor=_Inline())
    engine.betfair_client = client
    engine.runtime_controller = _runtime(fake, **rt)
    return engine, fake, db


def _bet(ref, *, size, price=3.0, side="BACK", market="1.200", selection=7):
    return {"market_id": market, "selection_id": selection, "side": side, "bet_type": side, "price": price,
            "size": size, "stake": size, "customer_ref": ref}


def _names(fake):
    return [c[0] for c in fake.calls if c[0] in ("placeOrders", "cancelOrders", "replaceOrders")]


def _the_position(fake):
    (o,) = fake.live()
    return o


# ------------------------------------------------------------------------------------------- importi richiesti dall'owner
@pytest.mark.parametrize("side", ["BACK", "LAY"])
@pytest.mark.parametrize("amount", ["0.10", "0.20", "0.30", "0.50", "0.90"])
def test_sub_minimum_amount_is_executed_exactly_in_live_through_the_saga(amount, side):
    engine, fake, db = _live_engine()
    res = engine.submit_quick_bet(_bet(f"ms-{side}-{amount}", size=float(amount), side=side))
    assert res["ok"] is True, res
    assert _names(fake) == ["placeOrders", "cancelOrders", "replaceOrders"]
    place = fake.calls[[c[0] for c in fake.calls].index("placeOrders")][1]
    (ins,) = place["instructions"]
    assert ins["limitOrder"]["size"] == 1.0, "stub = minimo exchange (1 EUR), mai di piu'"
    assert ins["limitOrder"]["price"] == (1000.0 if side == "BACK" else 1.01), "stub a prezzo non abbinabile"
    assert ins["customerOrderRef"] == f"ms-{side}-{amount}", "identita' della saga sull'ordine"
    reduce_call = [c for c in fake.calls if c[0] == "cancelOrders"][0]
    assert reduce_call[2] is True, "la riduzione non si ripete mai (single_shot)"
    assert D(str(reduce_call[1]["instructions"][0]["sizeReduction"])) == MIN - D(amount)
    pos = _the_position(fake)
    assert pos["matched"] + pos["remaining"] == D(amount) and pos["price"] == D("3.0"), "importo esatto, prezzo target"
    expected_exposure = D(amount) if side == "BACK" else D(amount) * 2
    assert fake.exposure() == expected_exposure, "nessuna esposizione residua dello stub"
    saga = DbSagaStore(db, LIVE_SAGA_SETTINGS_KEY).load(f"ms-{side}-{amount}")
    assert saga["phase"] == "ACTIVE" and SAGA_SETTINGS_KEY not in _settings(db), "store LIVE separato dallo store SIM"
    assert engine._live_microstake_executor.port._reservations == {}, "riserva temporanea rilasciata"


@pytest.mark.parametrize("side", ["BACK", "LAY"])
@pytest.mark.parametrize("amount", ["1.00", "1.50", "2.00", "5.00"])
def test_amount_at_or_above_the_one_euro_reference_goes_direct_without_saga(amount, side):
    engine, fake, db = _live_engine()
    res = engine.submit_quick_bet(_bet(f"dir-{side}-{amount}", size=float(amount), side=side))
    assert res["ok"] is True, res
    assert _names(fake) == ["placeOrders"]
    (ins,) = fake.calls[0][1]["instructions"]
    assert D(str(ins["limitOrder"]["size"])) == D(amount) and ins["limitOrder"]["price"] == 3.0
    assert "customerOrderRef" not in ins, "percorso diretto invariato"
    assert engine._live_microstake_executor is None and LIVE_SAGA_SETTINGS_KEY not in _settings(db)


def test_mixed_dutching_batch_routes_each_leg_on_its_own_amount_in_live():
    """Le gambe del Dutching (come Roserpina, Telegram e Quick Bet) arrivano all'engine come CMD_QUICK_BET: lo
    stesso choke point sceglie per ogni gamba, sul suo importo, diretto o Microstake."""
    engine, fake, _ = _live_engine()
    legs = [("leg-1", 7, 0.40), ("leg-2", 8, 1.20), ("leg-3", 9, 0.10)]
    for ref, sel, size in legs:
        bet = dict(_bet(ref, size=size, selection=sel), batch_id="B-1")
        assert engine.submit_quick_bet(bet)["ok"] is True, ref
    by_sel = {o["selectionId"]: o for o in fake.live()}
    assert {sel: by_sel[sel]["matched"] + by_sel[sel]["remaining"] for sel in (7, 8, 9)} == \
        {7: D("0.40"), 8: D("1.20"), 9: D("0.10")}
    assert _names(fake) == ["placeOrders", "cancelOrders", "replaceOrders", "placeOrders",
                            "placeOrders", "cancelOrders", "replaceOrders"]


def test_direct_placement_below_the_minimum_is_rejected_by_the_exchange_model():
    """Premessa della saga: l'esecuzione diretta di 0,10 non e' supportata (rifiuto INVALID_BET_SIZE)."""
    fake = _FakeBetfair()
    out = _client(fake).place_bet(market_id="1.200", selection_id=7, side="BACK", price=3.0, size=0.10)
    assert out["ok"] is False and out["order_unknown"] is False and fake.orders == {}


def test_target_price_that_crosses_the_book_is_matched_after_the_replace():
    engine, fake, _ = _live_engine()
    fake.book("1.200", 7, back=[(3.0, 50.0)])
    res = engine.submit_quick_bet(_bet("cross", size=0.10, price=3.0))
    assert res["ok"] is True, res
    pos = _the_position(fake)
    assert pos["matched"] == D("0.10") and pos["remaining"] == 0, "abbinato esattamente lo stake richiesto"
    assert all(o["matched"] == 0 for o in fake.orders.values() if o is not pos), "lo stub non si abbina mai"


# ------------------------------------------------------------------------------------------- fondi e liability
def test_insufficient_available_funds_block_the_stub_before_any_exchange_operation():
    engine, fake, db = _live_engine(funds=lambda: {"ok": True, "available": 0.99})
    res = engine.submit_quick_bet(_bet("nofunds", size=0.10))
    assert res["ok"] is False and "transient_exposure_cap" in str(res["error"])
    assert _names(fake) == [] and fake.orders == {}
    assert LIVE_SAGA_SETTINGS_KEY not in _settings(db)


def test_lay_stub_liability_is_the_reserved_temporary_exposure():
    # LAY: stub 1 EUR a 1.01 = liability 0.01; l'ordine finale 0.10 a 3.0 = liability 0.20 (fondi 0.005 => blocco)
    engine, fake, _ = _live_engine(funds=lambda: {"ok": True, "available": 0.005})
    res = engine.submit_quick_bet(_bet("lay-cap", size=0.10, side="LAY"))
    assert res["ok"] is False and "transient_exposure_cap" in str(res["error"]) and fake.orders == {}
    engine2, fake2, _ = _live_engine()
    assert engine2.submit_quick_bet(_bet("lay-ok", size=0.10, side="LAY"))["ok"] is True
    assert fake2.exposure() == D("0.20")


def test_unreadable_funds_block_fail_closed():
    engine, fake, _ = _live_engine(funds=lambda: {"ok": False, "error": "API"})
    res = engine.submit_quick_bet(_bet("funds-err", size=0.10))
    assert res["ok"] is False and "transient_cap_unavailable" in str(res["error"]) and _names(fake) == []


def test_exchange_rejection_of_the_stub_leaves_no_order_no_reservation_no_pending_saga():
    fake = _FakeBetfair(balance="0.50")                         # l'exchange rifiuta lo stub per fondi
    engine, fake, db = _live_engine(fake, funds=lambda: {"ok": True, "available": 100.0})
    res = engine.submit_quick_bet(_bet("bf-reject", size=0.10))
    assert res["ok"] is False and "BET_FAILED" in str(res["error"]) and "'phase': 'FAILED'" in str(res["error"]), res
    assert fake.orders == {} and engine._live_microstake_executor.port._reservations == {}
    assert DbSagaStore(db, LIVE_SAGA_SETTINGS_KEY).pending_refs() == []
    assert engine.submit_quick_bet(_bet("bf-reject", size=0.10))["status"] == "DUPLICATE_BLOCKED", \
        "rifiuto dopo l'invio: nessun nuovo invio automatico con lo stesso ref"


# ------------------------------------------------------------------------------------------- prova del book
@pytest.mark.parametrize("setup", ["suspended", "unreadable"])
def test_unproven_book_blocks_before_any_order(setup):
    engine, fake, _ = _live_engine()
    if setup == "suspended":
        fake.market_status = "SUSPENDED"
    else:
        engine.betfair_client.get_market_book = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("REQUEST_FAILED"))
    res = engine.submit_quick_bet(_bet(f"book-{setup}", size=0.10))
    assert res["ok"] is False and "passive_stub_price_unverified" in str(res["error"]), res
    assert _names(fake) == []


def test_stub_that_would_cross_the_book_is_blocked_before_place():
    engine, fake, _ = _live_engine()
    fake.book("1.200", 7, back=[(1000.0, 5.0)])                 # qualcuno offre a 1000: lo stub BACK si abbinerebbe
    res = engine.submit_quick_bet(_bet("stub-cross", size=0.10))
    assert res["ok"] is False and "stub_price_would_cross_book" in str(res["error"]) and _names(fake) == []


# ------------------------------------------------------------------------------------------- risposte perse
def test_lost_place_response_is_recovered_by_customer_order_ref_never_placed_twice():
    engine, fake, _ = _live_engine()
    fake.lose.add("placeOrders")
    res = engine.submit_quick_bet(_bet("lost-place", size=0.10))
    assert res["ok"] is True, res                                # stub ritrovato per ref, saga completata
    assert _names(fake).count("placeOrders") == 1
    assert _the_position(fake)["remaining"] == D("0.10")


@pytest.mark.parametrize("method", ["cancelOrders", "replaceOrders"])
def test_lost_cancel_or_replace_response_is_ambiguous_with_the_reservation_held(method):
    engine, fake, db = _live_engine()
    fake.lose.add(method)
    res = engine.submit_quick_bet(_bet(f"lost-{method}", size=0.10))
    assert res["ok"] is False and res["status"] == "AMBIGUOUS", res
    assert engine._live_microstake_executor.port.has_reservation(f"lost-{method}")
    assert DbSagaStore(db, LIVE_SAGA_SETTINGS_KEY).pending_refs() == [f"lost-{method}"]
    again = engine.submit_quick_bet(_bet(f"lost-{method}", size=0.10))
    assert again["status"] == "DUPLICATE_BLOCKED" and _names(fake).count("placeOrders") == 1


def test_lost_replace_response_is_reconciled_on_restart_when_the_ref_survives_the_replace():
    fake = _FakeBetfair()
    engine, fake, db = _live_engine(fake)
    fake.lose.add("replaceOrders")
    assert engine.submit_quick_bet(_bet("rp", size=0.10))["status"] == "AMBIGUOUS"
    restarted, _, _ = _live_engine(fake, db=db)
    out = restarted.recover_native_microstake_sagas()
    assert out["ok"] is True and out["results"] == {"rp": "ACTIVE"}, out
    assert _the_position(fake)["remaining"] == D("0.10") and _names(fake).count("replaceOrders") == 1


def test_lost_replace_response_stays_pending_when_the_ref_does_not_survive_the_replace():
    """Comportamento Betfair da verificare nel collaudo: se il replace non porta il customerOrderRef, la saga non
    ritrova il nuovo ordine e resta pendente (fail-closed, riserva tenuta): mai un secondo ordine."""
    fake = _FakeBetfair(carry_ref=False)
    engine, fake, db = _live_engine(fake)
    fake.lose.add("replaceOrders")
    assert engine.submit_quick_bet(_bet("rp2", size=0.10))["status"] == "AMBIGUOUS"
    restarted, _, _ = _live_engine(fake, db=db)
    out = restarted.recover_native_microstake_sagas()
    assert out["ok"] is False and out["pending"] == ["rp2"], out
    assert len(fake.live()) == 1 and _names(fake).count("placeOrders") == 1


def test_cancelled_stub_still_listed_is_never_taken_for_the_position():
    """Se Betfair tenesse fra gli ordini correnti lo stub sostituito (cancellato, zero abbinato), la riconciliazione
    per ref non lo confonde con la posizione: un solo ordine vivo, saga completata."""
    fake = _FakeBetfair(keep_dead=True)
    engine, fake, db = _live_engine(fake)
    fake.lose.add("replaceOrders")
    assert engine.submit_quick_bet(_bet("dead", size=0.10))["status"] == "AMBIGUOUS"
    restarted, _, _ = _live_engine(fake, db=db)
    out = restarted.recover_native_microstake_sagas()
    assert out["ok"] is True and out["results"] == {"dead": "ACTIVE"}, out
    assert _the_position(fake)["remaining"] == D("0.10")


def test_rejected_replace_rolls_the_stub_back_and_leaves_no_exposure():
    engine, fake, db = _live_engine()
    fake.reject_replace = True
    res = engine.submit_quick_bet(_bet("rb", size=0.10))
    assert res["ok"] is False, res
    assert fake.live() == [] and fake.exposure() == 0, "stub cancellato, nessuna esposizione"
    assert DbSagaStore(db, LIVE_SAGA_SETTINGS_KEY).load("rb")["phase"] == "ROLLED_BACK"
    assert engine._live_microstake_executor.port._reservations == {}


# ------------------------------------------------------------------------------------------- review indipendente (P2)
class _ReplaceTimeoutButApplied(_FakeBetfair):
    """replaceOrders avvenuto davvero, ma con ExecutionReportStatus TIMEOUT (esito ignoto per Betfair)."""

    def _replaceOrders(self, params):
        out = super()._replaceOrders(params)
        new_id = out["instructionReports"][0]["placeInstructionReport"]["betId"]
        return {"status": "TIMEOUT", "instructionReports": [
            {"status": "TIMEOUT", "placeInstructionReport": {"status": "TIMEOUT", "betId": new_id}}]}


def test_replace_with_timeout_status_is_an_unknown_outcome_never_a_rollback():
    fake = _ReplaceTimeoutButApplied(carry_ref=False, keep_dead=True)
    engine, fake, db = _live_engine(fake)
    res = engine.submit_quick_bet(_bet("rto", size=0.10))
    assert res["status"] == "AMBIGUOUS", res                     # prima: ROLLED_BACK => FAILED con l'ordine vivo
    assert DbSagaStore(db, LIVE_SAGA_SETTINGS_KEY).pending_refs() == ["rto"]
    assert engine._live_microstake_executor.port.has_reservation("rto")
    assert _the_position(fake)["remaining"] == D("0.10")        # l'ordine vivo resta seguito dalla saga


class _ReplaceCancelsThenPlaceFails(_FakeBetfair):
    """Semantica Betfair: il replace cancella e poi piazza; se il piazzamento fallisce la cancellazione resta."""

    def _replaceOrders(self, params):
        (ins,) = params["instructions"]
        old = self.orders[ins["betId"]]
        size, old["remaining"] = old["remaining"], D("0")
        old["cancelled"] += size
        old["status"] = "EXECUTION_COMPLETE"
        return {"status": "FAILURE", "errorCode": "BET_ACTION_ERROR", "instructionReports": [{
            "status": "FAILURE", "errorCode": "MARKET_SUSPENDED",
            "cancelInstructionReport": {"status": "SUCCESS", "betId": old["betId"], "sizeCancelled": float(size)},
            "placeInstructionReport": {"status": "FAILURE", "errorCode": "MARKET_SUSPENDED"}}]}


def test_replace_that_cancelled_the_stub_and_failed_to_place_closes_without_exposure():
    fake = _ReplaceCancelsThenPlaceFails()                      # lo stub cancellato sparisce dai correnti
    engine, fake, db = _live_engine(fake)
    res = engine.submit_quick_bet(_bet("rpf", size=0.10))
    assert res["ok"] is False and fake.live() == [] and fake.exposure() == 0
    assert DbSagaStore(db, LIVE_SAGA_SETTINGS_KEY).load("rpf")["phase"] == "ROLLED_BACK"
    assert DbSagaStore(db, LIVE_SAGA_SETTINGS_KEY).pending_refs() == []   # prima: ROLLBACK_PENDING per sempre
    assert _names(fake).count("cancelOrders") == 1, "nessuna cancellazione di uno stub gia' cancellato"


def test_stub_lapsed_by_the_exchange_closes_the_saga_without_exposure():
    fake = _FakeBetfair()
    engine, fake, db = _live_engine(fake)
    real_cancel = fake._cancelOrders

    def lapse_then_cancel(params):                              # mercato sospeso: Betfair lapsa gli unmatched
        for o in fake.orders.values():
            o["cancelled"] += o["remaining"]
            o["remaining"], o["status"], o["lapsed"] = D("0"), "EXECUTION_COMPLETE", True
        return real_cancel(params)
    fake._cancelOrders = lapse_then_cancel
    assert engine.submit_quick_bet(_bet("lap", size=0.10))["ok"] is False
    assert fake.exposure() == 0
    restarted, _, _ = _live_engine(fake, db=db)
    out = restarted.recover_native_microstake_sagas()
    assert out["ok"] is True and out["results"] == {"lap": "ROLLED_BACK"}, out   # prima: AMBIGUOUS per sempre
    assert _names(fake).count("placeOrders") == 1


def test_stub_partly_matched_then_cancelled_is_never_read_as_unmatched_after_settlement():
    """Rilievo GPT-6.1 Sol: lo stesso betId puo' comparire in listClearedOrders sia CANCELLED (quota cancellata)
    sia SETTLED (quota abbinata). L'esito deve sommare l'abbinato regolato, mai fermarsi a CANCELLED con zero."""
    fake = _FakeBetfair()
    engine, fake, db = _live_engine(fake)
    fake.crash.add("placeOrders")
    with pytest.raises(_Crash):
        engine.submit_quick_bet(_bet("part", size=0.10))
    (stub,) = fake.orders.values()
    # lo stub si abbina per 0.30 e il resto viene cancellato; dopo il settlement non e' piu' fra i correnti
    stub.update(matched=D("0.30"), remaining=D("0"), cancelled=D("0.70"), status="EXECUTION_COMPLETE")
    fake.orders.clear()
    base = {"betId": stub["betId"], "marketId": stub["marketId"], "selectionId": stub["selectionId"],
            "side": stub["side"], "priceRequested": 1000.0, "customerOrderRef": stub["ref"]}
    fake._listClearedOrders = lambda params: {"moreAvailable": False, "clearedOrders": {
        "CANCELLED": [dict(base, sizeCancelled=0.70)],
        "SETTLED": [dict(base, sizeSettled=0.30, betOutcome="LOSE")]}.get(params["betStatus"], [])}
    DbSagaStore(db, LIVE_SAGA_SETTINGS_KEY).save("part", phase="STUB_ACCEPTED", bet_id=stub["betId"])
    restarted, _, _ = _live_engine(fake, db=db)
    out = restarted.recover_native_microstake_sagas()
    assert out["ok"] is False and out["pending"] == ["part"], out    # abbinato reale: riconciliazione, mai ROLLED_BACK
    assert DbSagaStore(db, LIVE_SAGA_SETTINGS_KEY).load("part")["phase"] == "AMBIGUOUS"


class _CancelSilentlyFails(_FakeBetfair):
    """cancelOrders dell'intero ordine (senza sizeReduction) che non cancella nulla: risposta configurabile."""

    def __init__(self, response, **kw):
        super().__init__(**kw)
        self.cancel_response = response

    def _cancelOrders(self, params):
        if any("sizeReduction" in ins for ins in params["instructions"]):
            return super()._cancelOrders(params)
        return self.cancel_response


@pytest.mark.parametrize("response", [
    {"status": "SUCCESS", "instructionReports": [{"status": "FAILURE", "errorCode": "ERROR_IN_ORDER"}]},
    {"status": "PROCESSED_WITH_ERRORS", "instructionReports": [{"status": "FAILURE", "errorCode": "ERROR_IN_ORDER"}]},
    {"status": "TIMEOUT", "instructionReports": [{"status": "TIMEOUT"}]},
    {"status": "SUCCESS", "instructionReports": []},
    {"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS", "sizeCancelled": 0.1,
                                                  "instruction": {"betId": "999"}}]},          # report contraddittorio
], ids=["success-instruction-failure", "processed-with-errors", "timeout", "no-report", "other-bet"])
def test_rollback_is_never_declared_while_a_residual_stays_on_the_exchange(response):
    """P1 del mandato PF-561-LIVE-MICROSTAKE-P1-HARDENING-01: `cancel_orders` puo' tornare ok=True con l'istruzione
    fallita. ROLLED_BACK (riserva rilasciata) solo con la prova che non resta residuo eseguibile."""
    fake = _CancelSilentlyFails(response)
    fake.reject_replace = True                                   # replace rifiutato => rollback dello stub
    engine, fake, db = _live_engine(fake)
    res = engine.submit_quick_bet(_bet("p1", size=0.10))
    assert res["ok"] is False and res["status"] == "AMBIGUOUS", res
    saga = DbSagaStore(db, LIVE_SAGA_SETTINGS_KEY).load("p1")
    assert saga["phase"] in ("ROLLBACK_PENDING", "AMBIGUOUS") and saga["phase"] != "ROLLED_BACK", saga
    assert engine._live_microstake_executor.port.has_reservation("p1"), "riserva rilasciata con lo stub vivo"
    assert [o["remaining"] for o in fake.live()] == [D("0.10")], "lo stub e' davvero ancora sull'exchange"
    assert DbSagaStore(db, LIVE_SAGA_SETTINGS_KEY).pending_refs() == ["p1"]


def test_rollback_after_a_lost_cancel_response_is_verified_never_resent_blindly():
    fake = _FakeBetfair()
    fake.reject_replace = True
    engine, fake, db = _live_engine(fake)
    real = fake._cancelOrders

    def lost(params):                                           # cancellazione APPLICATA, risposta persa
        if not any("sizeReduction" in ins for ins in params["instructions"]):
            real(params)
            raise RuntimeError("REQUEST_FAILED: TIMEOUT")
        return real(params)
    fake._cancelOrders = lost
    res = engine.submit_quick_bet(_bet("lostc", size=0.10))
    assert fake.live() == [] and fake.exposure() == 0
    assert res["ok"] is False
    full_cancels = [c for c in fake.calls if c[0] == "cancelOrders"
                    and not any("sizeReduction" in i for i in c[1]["instructions"])]
    saga = DbSagaStore(db, LIVE_SAGA_SETTINGS_KEY).load("lostc")
    assert saga["phase"] == "ROLLED_BACK", saga                  # prova: lo stub riletto non ha residuo
    restarted, _, _ = _live_engine(fake, db=db)
    assert restarted.recover_native_microstake_sagas()["status"] == "NOTHING_PENDING"
    assert len([c for c in fake.calls if c[0] == "cancelOrders"
                and not any("sizeReduction" in i for i in c[1]["instructions"])]) == len(full_cancels)


def test_recovery_with_live_blocked_only_cancels_the_stub_never_reprices_it():
    """Rilievo Codex P1 su #561: a LIVE bloccato (o STOP) il recovery non porta lo stub al prezzo d'ingresso."""
    fake = _FakeBetfair()
    engine, fake, db = _live_engine(fake)
    fake.crash.add("placeOrders")
    with pytest.raises(_Crash):
        engine.submit_quick_bet(_bet("blocked", size=0.10))
    before = len(_names(fake))
    restarted, _, _ = _live_engine(fake, db=db, live_allowed=False)
    out = restarted.recover_native_microstake_sagas()
    assert _names(fake)[before:] == ["cancelOrders"], "solo la cancellazione protettiva: nessuna riduzione ne' replace"
    cancel = [c for c in fake.calls if c[0] == "cancelOrders"][-1]
    assert "sizeReduction" not in cancel[1]["instructions"][0], "cancellazione dell'intero stub, mai la riduzione"
    assert out["ok"] is True and out["results"] == {"blocked": "ROLLED_BACK"}, out
    assert fake.live() == [] and fake.exposure() == 0
    reopened, _, _ = _live_engine(fake, db=db)                  # LIVE di nuovo consentito: nulla da riprendere
    assert reopened.recover_native_microstake_sagas()["status"] == "NOTHING_PENDING"


@pytest.mark.parametrize("live_allowed,daily_blocked,emergency,expected", [
    (True, False, False, True), (False, False, False, False), (True, True, False, False),
    (True, False, True, False), (True, None, True, False),             # None: runtime senza blocco giornaliero
])
def test_recovery_entry_gate_reads_every_stop_source(live_allowed, daily_blocked, emergency, expected):
    """Rilievo Grok su f1594fb: il gate legge sia il blocco giornaliero/pending-stop sia l'emergenza."""
    engine, _, _ = _live_engine(live_allowed=live_allowed)
    if daily_blocked is not None:
        engine.runtime_controller._daily_loss_entry_blocked = lambda: daily_blocked
    engine.runtime_controller.is_emergency_stopped = emergency
    assert engine._microstake_recovery_entry_allowed(True) is expected
    assert engine._microstake_recovery_entry_allowed(False) is True                # SIM invariata


def test_recovery_with_an_active_stop_is_protective_only():
    fake = _FakeBetfair()
    engine, fake, db = _live_engine(fake)
    fake.crash.add("placeOrders")
    with pytest.raises(_Crash):
        engine.submit_quick_bet(_bet("stopped", size=0.10))
    restarted, _, _ = _live_engine(fake, db=db)
    restarted.runtime_controller._daily_loss_entry_blocked = lambda: True
    out = restarted.recover_native_microstake_sagas()
    assert out["results"] == {"stopped": "ROLLED_BACK"} and "replaceOrders" not in _names(fake)


class _StubMatchesDuringReplace(_FakeBetfair):
    """Lo stub si abbina per 0.04 fra la lettura e il replace: Betfair rimette solo il residuo (0.06)."""

    def _replaceOrders(self, params):
        old = self.orders[params["instructions"][0]["betId"]]
        old["matched"] += D("0.04")
        old["remaining"] -= D("0.04")
        return super()._replaceOrders(params)


def test_stub_partly_matched_during_the_replace_is_never_declared_success():
    """Rilievo Codex P1 su #561: prima SUCCESS con 0.06, mentre 0.04 restavano abbinati sullo stub a 1000."""
    engine, fake, db = _live_engine(_StubMatchesDuringReplace())
    res = engine.submit_quick_bet(_bet("race", size=0.10))
    assert res["ok"] is False and res["status"] == "AMBIGUOUS", res
    saga = DbSagaStore(db, LIVE_SAGA_SETTINGS_KEY).load("race")
    assert saga["phase"] == "AMBIGUOUS" and saga["reason"] == "replacement_size_mismatch"
    assert engine._live_microstake_executor.port.has_reservation("race")


def test_accepted_stub_identity_is_persisted_when_the_post_place_read_fails():
    """Rilievo Codex P2 su f1594fb: PLACE accettato, lettura successiva fallita, stub poi cancellato/lapsato. Senza il
    betId persistito il recovery non poteva leggerne l'esito e restava pendente per sempre."""
    fake = _FakeBetfair()
    engine, fake, db = _live_engine(fake)
    real = fake._listCurrentOrders
    state = {"placed": False, "failed": False}
    real_place = fake._placeOrders

    def place(params):
        state["placed"] = True
        return real_place(params)

    def flaky(params):
        if state["placed"] and not state["failed"]:
            state["failed"] = True                                  # la rilettura dopo l'accettazione fallisce...
            raise RuntimeError("REQUEST_FAILED: TIMEOUT")
        if state["failed"] and state.get("hide"):
            return {"currentOrders": [], "moreAvailable": False}   # ...e la riconciliazione per ref non lo vede
        return real(params)
    fake._placeOrders, fake._listCurrentOrders = place, flaky
    state["hide"] = True
    assert engine.submit_quick_bet(_bet("sid", size=0.10))["status"] == "AMBIGUOUS"
    saga = DbSagaStore(db, LIVE_SAGA_SETTINGS_KEY).load("sid")
    assert saga.get("bet_id"), saga                                 # identita' dello stub accettato persistita
    for o in fake.orders.values():                                  # lo stub viene poi cancellato dall'exchange
        o["cancelled"], o["remaining"], o["status"] = o["remaining"], D("0"), "EXECUTION_COMPLETE"
    state["hide"] = False
    restarted, _, _ = _live_engine(fake, db=db)
    out = restarted.recover_native_microstake_sagas()
    assert out["ok"] is True and out["results"] == {"sid": "ROLLED_BACK"}, out
    assert _names(fake).count("placeOrders") == 1


def test_replacement_identity_is_persisted_before_the_read_and_used_by_recovery():
    """Rilievo Codex P2 su #561: replace accettato, lettura successiva fallita, ref non conservato dal replace."""
    fake = _FakeBetfair(carry_ref=False)
    engine, fake, db = _live_engine(fake)
    real = fake._listCurrentOrders
    state = {"replaced": False}
    real_replace = fake._replaceOrders

    def replace(params):
        state["replaced"] = True
        return real_replace(params)

    def flaky(params):
        if state["replaced"] and not state.get("failed"):
            state["failed"] = True
            raise RuntimeError("REQUEST_FAILED: TIMEOUT")
        return real(params)
    fake._replaceOrders, fake._listCurrentOrders = replace, flaky
    assert engine.submit_quick_bet(_bet("rid", size=0.10))["status"] == "AMBIGUOUS"
    saga = DbSagaStore(db, LIVE_SAGA_SETTINGS_KEY).load("rid")
    assert saga.get("replacement_bet_id"), saga
    restarted, _, _ = _live_engine(fake, db=db)
    out = restarted.recover_native_microstake_sagas()
    assert out["ok"] is True and out["results"] == {"rid": "ACTIVE"}, out     # prima: pendente per sempre
    assert _names(fake).count("replaceOrders") == 1 and _the_position(fake)["remaining"] == D("0.10")


def test_lost_replace_response_with_the_economic_order_already_matched_concludes_active():
    fake = _FakeBetfair()
    engine, fake, db = _live_engine(fake)
    fake.book("1.200", 7, back=[(3.0, 50.0)])                   # il target e' il best: l'ordine si abbina subito
    fake.lose.add("replaceOrders")
    assert engine.submit_quick_bet(_bet("rpm", size=0.10))["status"] == "AMBIGUOUS"
    restarted, _, _ = _live_engine(fake, db=db)
    out = restarted.recover_native_microstake_sagas()
    assert out["ok"] is True and out["results"] == {"rpm": "ACTIVE"}, out        # prima: pendente per sempre
    assert _the_position(fake)["matched"] == D("0.10") and _names(fake).count("placeOrders") == 1


def test_pre_send_read_failure_then_retry_of_the_same_ref_executes_once():
    fake = _FakeBetfair()
    engine, fake, db = _live_engine(fake)
    real = fake._listCurrentOrders
    calls = {"n": 0}

    def flaky(params):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("REQUEST_FAILED: NETWORK_ERROR: reset")
        return real(params)
    fake._listCurrentOrders = flaky
    first = engine.submit_quick_bet(_bet("pre", size=0.10))
    assert first["ok"] is False and _names(fake) == []
    again = engine.submit_quick_bet(_bet("pre", size=0.10))     # intento rilasciato: nuovo tentativo
    assert again["ok"] is True, again                           # prima: AMBIGUOUS con riserva tenuta per sempre
    assert _names(fake).count("placeOrders") == 1 and DbSagaStore(db, LIVE_SAGA_SETTINGS_KEY).pending_refs() == []


def test_pending_saga_with_a_legacy_two_euro_stub_is_completed_after_the_upgrade(tmp_path):
    """Migrazione RECTIFICATION-01: una saga SIM pendente creata con lo stub da 2 EUR (nessun `stub_size`
    persistito) viene ridotta dallo stub che ha DAVVERO piazzato, non dal nuovo minimo da 1 EUR."""
    from core.microstake_safe import JsonSagaStore, StatefulExchangeSimulator
    ex = StatefulExchangeSimulator()
    store = JsonSagaStore(tmp_path / "s.json")
    req = MicrostakeRequest("1.1", 7, Side.BACK, D("0.50"), D("3.0"), "legacy")
    store.save("legacy", phase=Phase.PREPARED, reservation_held=True, transient_exposure=D("2.00"),
               request=MicrostakeCoordinator._request_json(req), stub_price=D("1000"))
    stub = ex.place(market_id="1.1", selection_id=7, side=Side.BACK, size=D("2.00"), price=D("1000"),
                    customer_ref="legacy")
    store.save("legacy", phase=Phase.STUB_ACCEPTED, bet_id=stub.bet_id)
    res = MicrostakeCoordinator(ex.restart(), store, MicrostakePolicy()).recover("legacy")
    assert res.phase is Phase.ACTIVE, res                       # prima: AMBIGUOUS unexpected_residual_size
    live = [o for o in ex._state["orders"].values() if o.remaining > 0]
    assert [(o.remaining, o.price) for o in live] == [(D("0.50"), D("3.0"))]


def test_live_microstake_outcomes_feed_the_submission_circuit_breaker():
    engine, fake, _ = _live_engine(funds=lambda: {"ok": True, "available": 100.0})
    seen = []
    engine._order_submission_breaker.record_success = lambda: seen.append("ok")
    engine._order_submission_breaker.record_failure = lambda exc: seen.append(f"ko:{exc}")
    assert engine.submit_quick_bet(_bet("cb-ok", size=0.10))["ok"] is True
    fake.balance = D("0")                                        # l'exchange rifiuta lo stub
    assert engine.submit_quick_bet(_bet("cb-ko", size=0.10))["ok"] is False
    assert seen[0] == "ok" and seen[1].startswith("ko:")


# ------------------------------------------------------------------------------------------- crash e riavvio
@pytest.mark.parametrize("side", ["BACK", "LAY"])
@pytest.mark.parametrize("crash_at,outcome,calls_after,exposure", [
    # Caso A (decisione owner PF-561 §4): stub intero o ridotto, ingresso NON completato => cancellazione verificata,
    # mai un replace al prezzo d'ingresso dopo il riavvio
    ("placeOrders", "ROLLED_BACK", ["cancelOrders"], "0"),
    ("cancelOrders", "ROLLED_BACK", ["cancelOrders"], "0"),
    # Caso B: l'ordine richiesto e' gia' sull'exchange (nuovo betId) => riconciliato, nessun secondo ordine
    ("replaceOrders", "ACTIVE", [], None),
])
def test_process_crash_mid_saga_never_completes_an_old_entry_on_restart(tmp_path, side, crash_at, outcome,
                                                                       calls_after, exposure):
    db = Database(str(tmp_path / "live.db"))
    fake = _FakeBetfair()
    engine, fake, db = _live_engine(fake, db=db)
    fake.crash.add(crash_at)
    with pytest.raises(_Crash):
        engine.submit_quick_bet(_bet("crash-1", size=0.10, side=side))
    before = len(_names(fake))

    restarted, _, _ = _live_engine(fake, db=Database(str(tmp_path / "live.db")))   # nuovo processo, stesso DB
    out = restarted.recover_native_microstake_sagas()
    assert out["ok"] is True and out["results"] == {"crash-1": outcome}, out
    assert _names(fake)[before:] == calls_after, "mai un nuovo PLACE ne' un replace dopo il riavvio"
    if outcome == "ROLLED_BACK":
        cancel = [c for c in fake.calls if c[0] == "cancelOrders"][-1]
        assert "sizeReduction" not in cancel[1]["instructions"][0]
        assert fake.live() == [] and fake.exposure() == D(exposure), "nessuna esposizione residua"
    else:
        pos = _the_position(fake)
        assert pos["remaining"] == D("0.10") and pos["price"] == D("3.0")
        assert fake.exposure() == (D("0.10") if side == "BACK" else D("0.20")), "esposizione/liability esatta"
    assert restarted._live_microstake_executor.port._reservations == {}
    again = restarted.recover_native_microstake_sagas()
    assert again["ok"] is True and again["status"] == "NOTHING_PENDING", "idempotente su riavvii ripetuti"
    assert _names(fake).count("placeOrders") == 1


def test_restart_with_a_partly_matched_stub_keeps_the_matched_part_and_cancels_the_rest():
    """Caso D: quota dello stub gia' abbinata al riavvio => residuo cancellato, abbinato conservato e contabilizzato
    (AMBIGUOUS `rollback_matched` con riserva tenuta), mai un rollback totale ne' il completamento dell'ingresso."""
    fake = _FakeBetfair()
    engine, fake, db = _live_engine(fake)
    fake.crash.add("placeOrders")
    with pytest.raises(_Crash):
        engine.submit_quick_bet(_bet("partial", size=0.10))
    (stub,) = fake.orders.values()
    stub["matched"], stub["remaining"] = D("0.25"), D("0.75")
    restarted, _, _ = _live_engine(fake, db=db)
    out = restarted.recover_native_microstake_sagas()
    assert out["ok"] is False and out["pending"] == ["partial"], out
    assert stub["remaining"] == 0 and stub["matched"] == D("0.25"), "residuo cancellato, abbinato intatto"
    assert "replaceOrders" not in _names(fake)
    saga = DbSagaStore(db, LIVE_SAGA_SETTINGS_KEY).load("partial")
    assert saga["phase"] == "AMBIGUOUS" and saga["reason"] == "rollback_matched" and D(saga["matched"]) == D("0.25")
    assert restarted._live_microstake_executor.port.has_reservation("partial")


def test_simulation_recovery_still_completes_the_saga_as_approved_in_512():
    """La differenza SIM/LIVE e' intenzionale: in SIM il recovery completa la saga (comportamento #512)."""
    from core.microstake_safe import JsonSagaStore, StatefulExchangeSimulator
    import tempfile as _tf
    ex = StatefulExchangeSimulator()
    store = JsonSagaStore(os.path.join(_tf.mkdtemp(), "s.json"))
    req = MicrostakeRequest("1.1", 7, Side.BACK, D("0.10"), D("3.0"), "sim")
    store.save("sim", phase=Phase.PREPARED, reservation_held=True, transient_exposure=D("1.00"),
               request=MicrostakeCoordinator._request_json(req), stub_price=D("1000"), stub_size=D("1.00"))
    stub = ex.place(market_id="1.1", selection_id=7, side=Side.BACK, size=D("1.00"), price=D("1000"),
                    customer_ref="sim")
    store.save("sim", phase=Phase.STUB_ACCEPTED, bet_id=stub.bet_id)
    assert MicrostakeCoordinator(ex.restart(), store, MicrostakePolicy()).recover("sim").phase is Phase.ACTIVE


def test_pending_live_saga_blocks_a_simulation_session_until_reconciled():
    engine, fake, db = _live_engine()
    fake.lose.add("cancelOrders")
    assert engine.submit_quick_bet(_bet("blk", size=0.10))["status"] == "AMBIGUOUS"
    sim = TradingEngine(bus=_Bus(), db=db, client_getter=lambda: None, executor=_Inline())
    sim.runtime_controller = SimpleNamespace(get_effective_execution_mode=lambda: "SIMULATION")
    out = sim.recover_native_microstake_sagas()
    assert out["ok"] is False and out["status"] == "LIVE_SAGAS_PENDING" and out["pending"] == ["blk"]


def test_current_order_row_without_remaining_is_never_read_as_cancelled():
    """Audit PF-561 §6: una riga senza `sizeRemaining` non e' un residuo zero."""
    fake = _FakeBetfair()
    fake.reject_replace = True
    engine, fake, db = _live_engine(fake)
    real = fake._row

    def incomplete(o):
        row = real(o)
        if o["remaining"] > 0 and o["price"] == D("1000") and o["remaining"] < MIN:
            row.pop("sizeRemaining")                          # stub ridotto: riga incompleta al rollback
        return row
    fake._row = incomplete
    res = engine.submit_quick_bet(_bet("inc", size=0.10))
    assert res["status"] == "AMBIGUOUS", res
    assert DbSagaStore(db, LIVE_SAGA_SETTINGS_KEY).load("inc")["phase"] != "ROLLED_BACK"
    assert engine._live_microstake_executor.port.has_reservation("inc")


def test_corrupt_live_saga_store_is_fail_closed_for_entries_and_recovery():
    engine, fake, db = _live_engine()
    db.save_settings({LIVE_SAGA_SETTINGS_KEY: "{rotto"})
    res = engine.submit_quick_bet(_bet("corrupt", size=0.10))
    assert res["ok"] is False and _names(fake) == [], res          # nessun ordine su store illeggibile
    out = engine.recover_native_microstake_sagas()
    assert out["ok"] is False and out["status"] == "STORE_UNREADABLE"
    sim = TradingEngine(bus=_Bus(), db=db, client_getter=lambda: None, executor=_Inline())
    sim.runtime_controller = SimpleNamespace(get_effective_execution_mode=lambda: "SIMULATION")
    assert sim.recover_native_microstake_sagas()["ok"] is False      # anche una sessione SIM resta bloccata


def test_two_processes_on_the_same_database_never_execute_the_same_intent_twice(tmp_path):
    """Due engine (due processi) sullo stesso DB e sullo stesso exchange: la deduplica durevole degli intenti
    lascia passare un solo invio per ref; nessuna seconda saga, nessuna riserva impropria."""
    path = str(tmp_path / "shared.db")
    fake = _FakeBetfair()
    a, fake, _ = _live_engine(fake, db=Database(path))
    b, _, _ = _live_engine(fake, db=Database(path))
    first = a.submit_quick_bet(_bet("same", size=0.10))
    second = b.submit_quick_bet(_bet("same", size=0.10))
    assert first["ok"] is True and second["status"] == "DUPLICATE_BLOCKED", (first, second)
    assert _names(fake).count("placeOrders") == 1 and len(fake.live()) == 1
    assert b._live_microstake_executor is None or b._live_microstake_executor.port._reservations == {}


@pytest.mark.parametrize("side", ["BACK", "LAY"])
def test_multi_table_microstakes_and_direct_orders_keep_exposure_and_reservations_coherent(side):
    """Piu' tavoli (Roserpina) su mercati diversi, microstake e diretti misti: ogni ordine all'importo esatto,
    esposizione/liability totale corretta, nessuna riserva temporanea residua."""
    engine, fake, _ = _live_engine()
    tables = [("t1", "1.301", 0.10), ("t2", "1.302", 0.30), ("t3", "1.303", 1.50), ("t4", "1.304", 0.90),
              ("t5", "1.305", 2.00)]
    for i, (ref, market, size) in enumerate(tables, start=1):
        bet = dict(_bet(ref, size=size, side=side, market=market), table_id=i, event_key=f"EV-{i}")
        assert engine.submit_quick_bet(bet)["ok"] is True, ref
    sizes = sorted(o["matched"] + o["remaining"] for o in fake.live())
    assert sizes == sorted(D(str(t[2])) for t in tables)
    total = sum(D(str(t[2])) for t in tables)
    assert fake.exposure() == (total if side == "BACK" else total * 2)
    assert engine._live_microstake_executor.port._reservations == {}
    assert _names(fake).count("replaceOrders") == 3 and _names(fake).count("placeOrders") == 5


# ------------------------------------------------------------------------------------------- review del diff completo
def test_replacement_gone_never_rolls_back_while_the_stub_holds_a_settled_matched_part():
    """F1: stub abbinato 0.04 durante il replace, nuovo ordine (0.06) poi lapsato, stub regolato e uscito dai
    correnti (SETTLED 0.04). Il recovery non chiude ROLLED_BACK guardando solo il replacement."""
    fake = _StubMatchesDuringReplace()
    engine, fake, db = _live_engine(fake)
    assert engine.submit_quick_bet(_bet("f1", size=0.10))["status"] == "AMBIGUOUS"
    saga = DbSagaStore(db, LIVE_SAGA_SETTINGS_KEY).load("f1")
    stub_id = [o for o in fake.orders.values() if o["price"] == D("1000")][0]["betId"]
    assert saga["stub_bet_id"] == stub_id, "identita' dello stub conservata accanto a quella del replacement"
    new = fake.orders[saga["replacement_bet_id"]]
    new["cancelled"] += new["remaining"]
    new["remaining"], new["status"], new["lapsed"] = D("0"), "EXECUTION_COMPLETE", True
    del fake.orders[stub_id]
    real = fake._listClearedOrders

    def cleared(params):
        out = real(params)
        if params["betStatus"] == "SETTLED" and stub_id in (params.get("betIds") or [stub_id]):
            out["clearedOrders"].append({"betId": stub_id, "marketId": "1.200", "selectionId": 7, "side": "BACK",
                                         "priceRequested": 1000.0, "sizeSettled": 0.04, "customerOrderRef": "f1"})
        return out
    fake._listClearedOrders = cleared
    restarted, _, _ = _live_engine(fake, db=db)
    out = restarted.recover_native_microstake_sagas()
    assert out["ok"] is False and out["pending"] == ["f1"], out        # prima: ROLLED_BACK, 0.04 non contabilizzati
    assert DbSagaStore(db, LIVE_SAGA_SETTINGS_KEY).load("f1")["phase"] == "AMBIGUOUS"


def test_legacy_saga_without_replace_flag_is_never_rolled_back_on_a_vanished_stub():
    """Rilievo GPT-6.1 Sol su 844ae41: una saga persistita prima di questi campi (niente `replace_sent` ne'
    `stub_bet_id`) non dice se il replace e' partito. Stub cancellato + nuovo ordine vivo senza ref: mai ROLLED_BACK
    (riserva rilasciata con l'ordine vivo); resta AMBIGUOUS finche' non e' riconciliata."""
    fake = _FakeBetfair(carry_ref=False)
    client = _client(fake)
    client.place_bet(market_id="1.200", selection_id=7, side="BACK", price=1000.0, size=1.0, customer_order_ref="old")
    (stub_id,) = fake.orders
    client.reduce_order(market_id="1.200", bet_id=stub_id, size_reduction=0.9)
    client.replace_orders(market_id="1.200", bet_id=stub_id, new_price=3.0)          # replace avvenuto, risposta persa
    engine, fake, db = _live_engine(fake, client=client)
    req = MicrostakeRequest("1.200", 7, Side.BACK, D("0.10"), D("3.0"), "old")
    DbSagaStore(db, LIVE_SAGA_SETTINGS_KEY).save("old", phase="AMBIGUOUS", reservation_held=True,
                                                 transient_exposure="1.00", stub_price="1000", bet_id=stub_id,
                                                 request=MicrostakeCoordinator._request_json(req))
    out = engine.recover_native_microstake_sagas()
    assert out["ok"] is False and out["pending"] == ["old"], out          # prima: ROLLED_BACK con 0.10 vivo
    assert [o["remaining"] for o in fake.live()] == [D("0.10")]


def test_saga_with_a_replacement_but_no_stub_identity_never_rolls_back_without_proof():
    """Saga di un head intermedio: `replacement_bet_id` persistito ma niente `stub_bet_id`. Replacement lapsato senza
    abbinato, stub originale abbinato in parte: senza l'identita' dello stub non c'e' prova => mai ROLLED_BACK."""
    fake = _FakeBetfair()
    client = _client(fake)
    client.place_bet(market_id="1.200", selection_id=7, side="BACK", price=1000.0, size=1.0, customer_order_ref="mid")
    (stub_id,) = fake.orders
    fake.orders[stub_id].update(matched=D("0.04"), remaining=D("0.06"))
    client.replace_orders(market_id="1.200", bet_id=stub_id, new_price=3.0)
    new_id = [b for b in fake.orders if b != stub_id][0]
    new = fake.orders[new_id]
    new["cancelled"], new["remaining"], new["status"], new["lapsed"] = new["remaining"], D("0"), "EXECUTION_COMPLETE", True
    for o in fake.orders.values():
        o["ref"] = ""                                              # nessun ref utile: identita' solo dalla saga
    engine, fake, db = _live_engine(fake, client=client)
    req = MicrostakeRequest("1.200", 7, Side.BACK, D("0.10"), D("3.0"), "mid")
    DbSagaStore(db, LIVE_SAGA_SETTINGS_KEY).save("mid", phase="AMBIGUOUS", reservation_held=True,
                                                 transient_exposure="1.00", stub_price="1000", bet_id=new_id,
                                                 replacement_bet_id=new_id, replace_sent=True,
                                                 request=MicrostakeCoordinator._request_json(req))
    out = engine.recover_native_microstake_sagas()
    assert out["ok"] is False and out["pending"] == ["mid"], out


def test_new_session_client_restores_the_reservations_of_pending_sagas():
    """F2: re-auth => nuovo BetfairClient => nuovo esecutore LIVE. Le riserve delle saghe pendenti si ripristinano
    alla costruzione: lo stub vivo resta contato nel cap temporaneo."""
    engine, fake, db = _live_engine()
    fake.lose.add("cancelOrders")
    assert engine.submit_quick_bet(_bet("f2", size=0.10))["status"] == "AMBIGUOUS"
    old = engine._live_microstake_executor
    engine.betfair_client = _client(fake)                           # BetfairService._connect_live(force=True)
    new = engine.native_microstake_executor()
    assert new is not old and new.port.has_reservation("f2"), "riserva della saga pendente persa"


def test_live_blocked_restart_still_cancels_the_live_stub_protectively():
    """F3: con LIVE non consentito il runtime reale dichiara SIMULATION. Le saghe LIVE pendenti si riprendono
    comunque in sola protezione sul client reale (cancellazione verificata), mai lasciate eseguibili."""
    fake = _FakeBetfair()
    engine, fake, db = _live_engine(fake)
    fake.crash.add("placeOrders")
    with pytest.raises(_Crash):
        engine.submit_quick_bet(_bet("f3", size=0.10))
    before = len(_names(fake))
    restarted, _, _ = _live_engine(fake, db=db, live_allowed=False)
    restarted.runtime_controller.get_effective_execution_mode = (
        lambda: "LIVE" if restarted.runtime_controller.is_live_allowed() else "SIMULATION")
    out = restarted.recover_native_microstake_sagas()
    assert _names(fake)[before:] == ["cancelOrders"], "cancellazione protettiva, nessuna riduzione ne' replace"
    assert fake.live() == [] and out["ok"] is True, out


def test_replace_timeout_with_the_new_bet_id_in_the_report_is_persisted():
    """P3: replace in TIMEOUT con il betId nel placeInstructionReport => identita' persistita e usata."""
    fake = _ReplaceTimeoutButApplied(carry_ref=False)
    engine, fake, db = _live_engine(fake)
    assert engine.submit_quick_bet(_bet("rtid", size=0.10))["status"] == "AMBIGUOUS"
    saga = DbSagaStore(db, LIVE_SAGA_SETTINGS_KEY).load("rtid")
    assert saga.get("replacement_bet_id"), saga
    restarted, _, _ = _live_engine(fake, db=db)
    assert restarted.recover_native_microstake_sagas()["results"] == {"rtid": "ACTIVE"}


def test_two_live_orders_with_the_same_ref_before_place_are_ambiguous_never_failed():
    """P3: `find_by_ref` che trova due ordini vivi con lo stesso ref (store perso) => mai FAILED senza ordine (che
    rilascerebbe riserva e intento): AMBIGUOUS, nessun invio."""
    fake = _FakeBetfair()
    client = _client(fake)
    for _ in range(2):
        client.place_bet(market_id="1.200", selection_id=7, side="BACK", price=1000.0, size=1.0,
                         customer_order_ref="dup")
    engine, fake, db = _live_engine(fake, client=client)
    res = engine.submit_quick_bet(_bet("dup", size=0.10))
    assert res["status"] == "AMBIGUOUS", res
    assert _names(fake).count("placeOrders") == 2                    # solo i due preesistenti


# ------------------------------------------------------------------------------------------- difese indipendenti
# Ogni difesa del P1 e' provata DA SOLA: la porta senza il coordinatore e il coordinatore senza la porta.
def test_port_never_confirms_a_cancel_that_left_a_residual():
    from core.microstake_live_port import LiveExchangePort
    fake = _CancelSilentlyFails({"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS"}]})
    client = _client(fake)
    client.place_bet(market_id="1.200", selection_id=7, side="BACK", price=1000.0, size=1.0, customer_order_ref="u")
    (bet,) = fake.orders
    with pytest.raises(RuntimeError, match="CANCEL_NOT_CONFIRMED"):
        LiveExchangePort(client).cancel_all_remaining(bet)        # report SUCCESS, ma il residuo e' ancora li'


@pytest.mark.parametrize("report", [{"status": "TIMEOUT"}, None, {"status": "SUCCESS", "instruction": {"betId": "x"}}])
def test_port_never_confirms_a_reduction_without_its_own_success_report(report):
    from core.microstake_live_port import LiveExchangePort
    from core.microstake_safe import LostResponse
    fake = _FakeBetfair()
    client = _client(fake)
    client.place_bet(market_id="1.200", selection_id=7, side="BACK", price=1000.0, size=1.0, customer_order_ref="r")
    (bet,) = fake.orders
    real = fake._cancelOrders
    fake._cancelOrders = lambda params: (real(params), {"status": "SUCCESS",
                                                        "instructionReports": [report] if report else []})[1]
    with pytest.raises(LostResponse):
        LiveExchangePort(client).cancel_reduction(bet, D("0.90"))


def test_port_rejects_a_current_order_row_without_remaining():
    from core.microstake_live_port import LiveExchangePort
    row = {"betId": "1", "marketId": "1.2", "selectionId": 7, "side": "BACK", "priceSize": {"price": 3.0, "size": 1},
           "sizeMatched": 0.0}
    with pytest.raises(ValueError, match="CURRENT_ORDER_INCOMPLETE"):
        LiveExchangePort._to_order(row)


def test_simulation_stub_lapsed_after_a_crash_closes_the_saga_without_exposure():
    """Stesso caso dello stub sparito senza abbinato, sul SimulationBroker reale (SIM completa le saghe vive, ma uno
    stub lapsato non ha nulla da completare: ROLLED_BACK, nessuna saga pendente per sempre)."""
    from core.microstake_sim_port import build_native_microstake_executor
    from tests.test_microstake_engine_native import _engine, _orders, _payload
    engine, broker, db = _engine()
    engine._default_native_microstake_executor("SIMULATION")
    port = engine.microstake_executor.port
    port.inject_fault("LOST_CANCEL_RESPONSE")                  # betId dello stub persistito, esito ignoto
    assert engine.submit_quick_bet(_payload("simlap", size=0.10, price=3.0))["status"] == "AMBIGUOUS"
    (stub,) = [o for o in _orders(broker) if o["status"] == "EXECUTABLE"]
    broker.cancel_orders(market_id=stub["marketId"], instructions=[{"betId": stub["betId"]}])   # lapsato
    restarted = build_native_microstake_executor(broker, db, lambda _p: D("10"))
    assert restarted.recover_pending() == {"simlap": "ROLLED_BACK"}


def test_coordinator_never_closes_rolled_back_with_a_residual_whatever_the_port_says(tmp_path):
    from core.microstake_safe import Fault, JsonSagaStore, StatefulExchangeSimulator

    class _PortThatLies(StatefulExchangeSimulator):
        def cancel_all_remaining(self, bet_id):
            return self.order(bet_id)                             # "cancellato" ma il residuo resta

    ex = _PortThatLies()
    ex.queue_fault(Fault.FAIL_REPLACE)
    store = JsonSagaStore(tmp_path / "s.json")
    req = MicrostakeRequest("1.1", 7, Side.BACK, D("0.10"), D("3.0"), "liar")
    res = MicrostakeCoordinator(ex, store, MicrostakePolicy()).execute(req, stub_price=D("1000"),
                                                                     transient_cap=D("10"))
    assert res.phase is Phase.AMBIGUOUS and res.reason == "rollback_residual_open", res
    assert store.load("liar")["phase"] == "ROLLBACK_PENDING" and ex.has_reservation("liar")


def test_coordinator_never_declares_active_on_an_amount_different_from_the_request(tmp_path):
    from core.microstake_safe import JsonSagaStore, SimOrder, StatefulExchangeSimulator
    ex = StatefulExchangeSimulator()
    store = JsonSagaStore(tmp_path / "s.json")
    req = MicrostakeRequest("1.1", 7, Side.BACK, D("0.10"), D("3.0"), "size")
    store.save("size", phase=Phase.REDUCED, request=MicrostakeCoordinator._request_json(req))
    ex.reserve("size", D("1.00"), D("10"))
    wrong = SimOrder("B1", "1.1", 7, Side.BACK, D("3.0"), D("0.06"), D("0"), D("0.06"), "size")
    res = MicrostakeCoordinator(ex, store, MicrostakePolicy())._activate(req, wrong, D("1.00"))
    assert res.phase is Phase.AMBIGUOUS and res.reason == "active_size_mismatch" and ex.has_reservation("size")


# ------------------------------------------------------------------------------------------- concorrenza
def test_concurrent_microstakes_respect_the_temporary_cap_and_never_duplicate():
    """Ingresso e hedge del cashout usano lo STESSO esecutore (l'engine serializza i suoi invii, l'OrderRouter no):
    la riserva temporanea e' cio' che impedisce a due stub di superare i fondi insieme."""
    fake = _FakeBetfair()
    engine, fake, _ = _live_engine(fake, funds=lambda: {"ok": True, "available": 1.50})   # un solo stub alla volta
    executor = engine.native_microstake_executor()
    entered, release = threading.Event(), threading.Event()
    fake.hold = ("placeOrders", entered, release)
    results = {}
    first = threading.Thread(target=lambda: results.update(a=executor.execute(_bet("cc-a", size=0.10))))
    first.start()
    assert entered.wait(5)
    results["b"] = executor.execute(_bet("cc-b", size=0.20))          # stub BACK di A ancora sull'exchange: 1+1 > 1.50
    release.set()
    first.join(5)
    assert results["a"]["status"] == "SUCCESS", results
    assert results["b"]["status"] == "FAILURE" and results["b"]["error"] == "transient_exposure_cap"
    assert results["b"]["pre_send"] is True and _names(fake).count("placeOrders") == 1
    retry = executor.execute(_bet("cc-b", size=0.20))                 # A concluso: riserva libera
    assert retry["status"] == "SUCCESS" and len(fake.live()) == 2
    assert executor.port._reservations == {}


# ------------------------------------------------------------------------------------------- gate e identita'
def test_live_not_allowed_sends_nothing():
    engine, fake, _ = _live_engine(live_allowed=False)
    res = engine.submit_quick_bet(_bet("nogo", size=0.10))
    assert res["ok"] is False and "LIVE_EXECUTION_BLOCKED" in str(res["error"]) and fake.calls == []


def test_open_submission_circuit_breaker_blocks_the_microstake_like_a_direct_order():
    engine, fake, _ = _live_engine()
    engine._order_submission_breaker.is_open = lambda: True
    res = engine.submit_quick_bet(_bet("breaker", size=0.10))
    assert res["ok"] is False and "ORDER_SUBMISSION_CIRCUIT_BREAKER_OPEN" in str(res["error"]) and fake.calls == []


def test_a_client_that_is_not_the_real_betfair_client_never_gets_the_live_adapter():
    class _Double:                    # stesse operazioni del client reale, ma non e' il BetfairClient
        def _never(self, *a, **k):  # pragma: no cover - non deve essere chiamato
            raise AssertionError("chiamata a un doppio in LIVE")
        place_bet = reduce_order = cancel_orders = replace_orders = get_current_orders = _never
        get_market_book = list_cleared_orders = _never
    engine, fake, _ = _live_engine(client=_Double())
    res = engine.submit_quick_bet(_bet("double", size=0.10))
    assert res["ok"] is False and "MICROSTAKE_EXECUTOR_UNAVAILABLE" in str(res["error"])


def test_live_executor_is_never_reused_in_simulation():
    engine, fake, _ = _live_engine()
    assert engine.submit_quick_bet(_bet("once", size=0.10))["ok"] is True
    assert engine.microstake_executor is None, "l'esecutore LIVE non occupa lo slot dell'esecutore SIM"
    assert engine._bound_native_microstake_executor("SIMULATION") is None


@pytest.mark.parametrize("policy,live_port", [(MicrostakePolicy(), True),
                                              (MicrostakePolicy(simulation_only=False, live_adapter=True), False),
                                              (MicrostakePolicy(simulation_only=False), True)])
def test_saga_refuses_a_policy_that_does_not_match_the_port(tmp_path, policy, live_port):
    fake = _FakeBetfair()
    port = LiveExchangePort(_client(fake))
    if not live_port:
        port.is_live_port = False
    store = DbSagaStore(_DB(), LIVE_SAGA_SETTINGS_KEY)
    req = MicrostakeRequest("1.200", 7, Side.BACK, D("0.10"), D("3.0"), "pol")
    res = MicrostakeCoordinator(port, store, policy).execute(req, stub_price=D("1000"), transient_cap=D("10"))
    assert res.phase is Phase.BLOCKED and res.reason == "adapter_mode_not_allowed" and fake.calls == []


def test_cashout_hedge_below_the_minimum_uses_the_same_live_adapter():
    from core.order_router import OrderRouter
    engine, fake, _ = _live_engine()
    service = SimpleNamespace(get_client=lambda: engine.betfair_client)
    router = OrderRouter(service, microstake_executor_provider=engine.native_microstake_executor)
    res = router.place({"market_id": "1.200", "selection_id": 7, "bet_type": "LAY", "price": 3.0, "stake": 0.30,
                        "customer_ref": "hedge"})
    assert res["placed"] is True and res["order_unknown"] is False, res
    assert _names(fake) == ["placeOrders", "cancelOrders", "replaceOrders"]
    assert _the_position(fake)["remaining"] == D("0.30")


# ------------------------------------------------------------------------------------------- BetfairClient (wire)
def test_place_bet_carries_the_order_ref_only_when_asked_and_rejects_a_bad_one_before_sending():
    fake = _FakeBetfair()
    client = _client(fake)
    client.place_bet(market_id="1.200", selection_id=7, side="BACK", price=3.0, size=2.0, customer_ref="r1")
    assert "customerOrderRef" not in fake.calls[-1][1]["instructions"][0]
    client.place_bet(market_id="1.200", selection_id=7, side="BACK", price=3.0, size=2.0, customer_order_ref="r2")
    assert fake.calls[-1][1]["instructions"][0]["customerOrderRef"] == "r2"
    n = len(fake.calls)
    with pytest.raises(RuntimeError, match="INVALID_CUSTOMER_ORDER_REF"):
        client.place_bet(market_id="1.200", selection_id=7, side="BACK", price=3.0, size=2.0,
                         customer_order_ref="x" * 33)
    assert len(fake.calls) == n


@pytest.mark.parametrize("bad", [0, -0.1, float("nan"), float("inf"), "abc", None])
def test_reduce_order_rejects_invalid_reductions_before_sending(bad):
    fake = _FakeBetfair()
    with pytest.raises(RuntimeError, match="INVALID_SIZE_REDUCTION"):
        _client(fake).reduce_order(market_id="1.200", bet_id="1", size_reduction=bad)
    assert fake.calls == []


def test_quote_provider_reads_the_best_counterparty_price():
    fake = _FakeBetfair()
    fake.book("1.200", 7, back=[(3.0, 5.0), (2.9, 9.0)], lay=[(3.1, 4.0), (3.2, 8.0)])
    quote = LiveQuoteProvider(_client(fake))
    back = quote({"market_id": "1.200", "selection_id": 7, "side": "BACK"})
    lay = quote({"market_id": "1.200", "selection_id": 7, "side": "LAY"})
    assert back == {"passive_stub_price": D("1000"), "best_counterparty_price": D("3.0")}
    assert lay == {"passive_stub_price": D("1.01"), "best_counterparty_price": D("3.1")}


def test_build_requires_a_client_with_the_needed_operations():
    with pytest.raises(TypeError):
        build_live_microstake_executor(SimpleNamespace(place_bet=lambda **k: None), _DB(), lambda p: D("10"))
