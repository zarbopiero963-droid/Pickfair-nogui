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


# ------------------------------------------------------------------------------------------- crash e riavvio
@pytest.mark.parametrize("crash_at,calls_after", [
    ("placeOrders", ["cancelOrders", "replaceOrders"]),          # stub intero sull'exchange
    ("cancelOrders", ["replaceOrders"]),                         # stub gia' ridotto
    ("replaceOrders", []),                                       # ordine finale gia' piazzato
])
def test_process_crash_mid_saga_is_completed_on_restart_without_duplicates(tmp_path, crash_at, calls_after):
    from database import Database
    db = Database(str(tmp_path / "live.db"))
    fake = _FakeBetfair()
    engine, fake, db = _live_engine(fake, db=db)
    fake.crash.add(crash_at)
    with pytest.raises(_Crash):
        engine.submit_quick_bet(_bet("crash-1", size=0.10))
    before = len(_names(fake))

    restarted, _, _ = _live_engine(fake, db=Database(str(tmp_path / "live.db")))   # nuovo processo, stesso DB
    out = restarted.recover_native_microstake_sagas()
    assert out["ok"] is True and out["results"] == {"crash-1": "ACTIVE"}, out
    assert _names(fake)[before:] == calls_after, "il recovery riprende dal passo persistito, mai un nuovo PLACE"
    pos = _the_position(fake)
    assert pos["remaining"] == D("0.10") and pos["price"] == D("3.0")
    assert fake.exposure() == D("0.10"), "nessuna esposizione dimenticata"
    again = restarted.recover_native_microstake_sagas()
    assert again["ok"] is True and again["status"] == "NOTHING_PENDING", "idempotente su riavvii ripetuti"
    assert len(fake.live()) == 1


def test_pending_live_saga_blocks_a_simulation_session_until_reconciled():
    engine, fake, db = _live_engine()
    fake.lose.add("cancelOrders")
    assert engine.submit_quick_bet(_bet("blk", size=0.10))["status"] == "AMBIGUOUS"
    sim = TradingEngine(bus=_Bus(), db=db, client_getter=lambda: None, executor=_Inline())
    sim.runtime_controller = SimpleNamespace(get_effective_execution_mode=lambda: "SIMULATION")
    out = sim.recover_native_microstake_sagas()
    assert out["ok"] is False and out["status"] == "LIVE_SAGAS_PENDING" and out["pending"] == ["blk"]


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
        place_bet = reduce_order = cancel_orders = replace_orders = get_current_orders = get_market_book = _never
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
