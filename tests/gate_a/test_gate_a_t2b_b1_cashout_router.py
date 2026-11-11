"""Gate A — T2b batch b1, componente cashout_router (CashoutRouter reale).

Verdetti individuali C-1 basati sul comportamento ratificato (PF-EXEC-01 punto 4, PF-EXEC-05,
P27/P38). I/O iniettati come stub deterministici: nessuna rete, nessun broker reale.
TRG-628-B-0770 (filtro per event_name) e' NEEDS_OWNER_DECISION: nessuna asserzione qui.
"""
from __future__ import annotations

from typing import Any, Dict, List

from cashout_router import CashoutRouter, _pure_resting_target
from core.trading_constants import CASHOUT_FAILED, REQ_EXECUTE_CASHOUT

_BOOK = {"status": "OPEN", "runners": [
    {"selectionId": 10, "ex": {"availableToLay": [{"price": 2.5, "size": 1000.0}],
                               "availableToBack": [{"price": 2.4, "size": 1000.0}]}},
    {"selectionId": 11, "ex": {"availableToLay": [{"price": 2.5, "size": 1000.0}],
                               "availableToBack": [{"price": 2.4, "size": 1000.0}]}},
]}


def _order(bet_id: str, market: str, sel: int, matched: float, remaining: float, side: str = "BACK") -> Dict[str, Any]:
    return {"betId": bet_id, "marketId": market, "selectionId": sel, "side": side,
            "sizeMatched": matched, "sizeRemaining": remaining, "averagePriceMatched": 3.0,
            "priceSize": {"price": 3.0}}


class _Harness:
    def __init__(self, *, bot_orders: Any = None, current: Any = None, book: Any = None,
                 cancel_result: Any = True) -> None:
        self.events: List[tuple] = []
        self.cancels: List[tuple] = []
        self.calls: List[str] = []
        self._bot_orders = bot_orders
        self._current = current
        self._book = book if book is not None else _BOOK
        self._cancel_result = cancel_result

    def fetch_bot_orders(self):
        self.calls.append("bot")
        if isinstance(self._bot_orders, Exception):
            raise self._bot_orders
        return self._bot_orders

    def fetch_current_orders(self):
        self.calls.append("current")
        if isinstance(self._current, Exception):
            raise self._current
        return self._current

    def fetch_market_book(self, market_id):
        self.calls.append(f"book:{market_id}")
        book = self._book
        if callable(book):
            return book(market_id)
        return book

    def cancel_orders(self, market_id, bet_ids):
        self.calls.append(f"cancel:{market_id}")
        self.cancels.append((market_id, list(bet_ids)))
        res = self._cancel_result
        return res(market_id, bet_ids) if callable(res) else res

    def publish(self, topic, payload=None):
        self.calls.append(f"publish:{topic}")
        self.events.append((topic, payload))

    def router(self) -> CashoutRouter:
        return CashoutRouter(fetch_current_orders=self.fetch_current_orders,
                             fetch_bot_orders=self.fetch_bot_orders,
                             fetch_market_book=self.fetch_market_book,
                             cancel_orders=self.cancel_orders, publish=self.publish)

    def topics(self) -> List[str]:
        return [t for t, _ in self.events]


def test_trg_628_b_0771():
    """ID: TRG-628-B-0771 / PF-B1-CASH-READ-FAIL"""
    h = _Harness(bot_orders=RuntimeError("database is locked"), current=[])
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["reason"] == "bot_orders_error" and out["published"] == 0
    assert h.topics() == [CASHOUT_FAILED]
    payload = h.events[0][1]
    assert payload["status"] == "ERROR"
    assert payload["reason"].startswith("fetch_bot_orders:")
    # abort: nessuna lettura degli ordini correnti, nessun book, nessun cancel, nessun hedge
    assert h.calls == ["bot", f"publish:{CASHOUT_FAILED}"]


def test_trg_628_b_0772():
    """ID: TRG-628-B-0772 / PF-B1-CASH-CURRENT-FAIL"""
    h = _Harness(bot_orders=[{"bet_id": "B1", "market_id": "1.1", "event_name": "A v B"}],
                 current=ConnectionError("session invalid"))
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["reason"] == "current_orders_error" and out["published"] == 0
    assert h.topics() == [CASHOUT_FAILED]
    payload = h.events[0][1]
    assert payload["status"] == "ERROR"
    assert payload["reason"].startswith("list_current_orders:")
    assert h.calls == ["bot", "current", f"publish:{CASHOUT_FAILED}"]
    assert h.cancels == []


def test_trg_628_b_0774():
    """ID: TRG-628-B-0774 / PF-B1-CASH-CANCEL-BEFORE-HEDGE"""
    bot = [{"bet_id": "B1", "market_id": "1.1", "event_name": "A v B"}]
    current = [_order("B1", "1.1", 10, matched=10.0, remaining=5.0)]
    # cancel NON confermato => nessun hedge per la posizione
    h = _Harness(bot_orders=bot, current=current, cancel_result=False)
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert REQ_EXECUTE_CASHOUT not in h.topics()
    assert out["published"] == 0 and out["skipped"] == 1
    assert h.cancels and h.cancels[0] == ("1.1", ["B1"])
    # cancel confermato => hedge pubblicato, e solo DOPO il cancel
    h2 = _Harness(bot_orders=bot, current=current, cancel_result=True)
    out2 = h2.router().route({"signal_type": "CASHOUT_ALL"})
    assert out2["published"] == 1
    assert h2.calls.index("cancel:1.1") < h2.calls.index(f"publish:{REQ_EXECUTE_CASHOUT}")


def test_trg_628_b_0777():
    """ID: TRG-628-B-0777 / PF-B1-CASH-PURE-RESTING"""
    bot = [{"bet_id": b, "market_id": "1.1", "event_name": "A v B"} for b in ("B1", "B2", "B3")]
    current = [
        _order("B1", "1.1", 10, matched=10.0, remaining=5.0),   # posizione con resting
        _order("B3", "1.1", 10, matched=0.0, remaining=2.0),    # puro sulla stessa selezione: gia' cancellato
        _order("B2", "1.1", 11, matched=0.0, remaining=4.0),    # puro del bot: da flattare
        _order("X9", "1.1", 11, matched=0.0, remaining=7.0),    # non del bot: mai toccato
    ]
    h = _Harness(bot_orders=bot, current=current, cancel_result=True)
    h.router().route({"signal_type": "CASHOUT_ALL"})
    assert h.cancels == [("1.1", ["B1", "B3"]), ("1.1", ["B2"])]
    # scope per mercato: fuori scope => nessun target; set vuoto => niente flatten
    is_bot = lambda row: row.get("betId") in {"B2"}  # noqa: E731
    row = _order("B2", "1.1", 11, matched=0.0, remaining=4.0)
    assert _pure_resting_target(row, is_bot, {"1.1"}, set()) == ("1.1", "B2")
    assert _pure_resting_target(row, is_bot, {"1.2"}, set()) is None
    assert _pure_resting_target(row, is_bot, set(), set()) is None
    assert _pure_resting_target(row, is_bot, None, {"B2"}) is None


def test_trg_628_b_0778():
    """ID: TRG-628-B-0778 / PF-B1-CASH-POSITION-ISOLATE"""
    bot = [{"bet_id": "B1", "market_id": "1.1", "event_name": "A v B"},
           {"bet_id": "B2", "market_id": "1.2", "event_name": "C v D"}]
    current = [_order("B1", "1.1", 10, matched=10.0, remaining=0.0),
               _order("B2", "1.2", 10, matched=10.0, remaining=0.0)]

    def book(market_id):
        if market_id == "1.1":
            raise TimeoutError("book timeout")
        return _BOOK

    h = _Harness(bot_orders=bot, current=current, book=book)
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out == {"published": 1, "positions": 2, "skipped": 1}
    reqs = [p for t, p in h.events if t == REQ_EXECUTE_CASHOUT]
    assert len(reqs) == 1 and str(reqs[0]["market_id"]) == "1.2"


def test_trg_628_b_0779():
    """ID: TRG-628-B-0779 / PF-B1-CASH-CANCEL-RESULT"""
    ok = CashoutRouter._cancel_succeeded
    # fallimento annidato sotto `result` (wrapper live): non confermato
    assert ok({"ok": True, "status": "SUCCESS",
               "result": {"status": "SUCCESS", "instructionReports": [{"status": "FAILURE"}]}}) is False
    assert ok({"ok": True, "result": {"status": "FAILURE"}}) is False
    # fallimento top-level (SIM)
    assert ok({"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS"}, {"status": "FAILURE"}]}) is False
    assert ok({"status": "FAILURE"}) is False
    assert ok({"ok": False}) is False
    for falsy in (None, {}, [], False):
        assert ok(falsy) is False
    # conferma: top e annidato tutti SUCCESS
    assert ok({"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS"}]}) is True
    assert ok({"ok": True, "status": "SUCCESS",
               "result": {"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS"}]}}) is True
    assert ok(True) is True
