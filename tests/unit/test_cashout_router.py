"""Unit test per CashoutRouter (Fase 2.1-B, design A3 + identità DB I1)."""

from cashout_router import CASHOUT_FAILED, REQ_EXECUTE_CASHOUT, CashoutRouter


def _curr(bet_id="B1", market="1.1", sel=7, side="BACK", matched=10.0, remaining=0.0, avg=2.0):
    return {
        "betId": bet_id, "marketId": market, "selectionId": sel, "side": side,
        "sizeMatched": matched, "sizeRemaining": remaining, "averagePriceMatched": avg,
    }


def _bot(bet_id="B1", market="1.1", sel=7, event="inter vs milan"):
    return {"bet_id": bet_id, "market_id": market, "selection_id": sel, "event_name": event}


def _book(market="1.1", sel=7, status="OPEN", back=2.6, lay=1.5):
    return {
        "marketId": market, "status": status,
        "runners": [{
            "selectionId": sel,
            "availableToBack": [{"price": back, "size": 100.0}],
            "availableToLay": [{"price": lay, "size": 100.0}],
        }],
    }


class _Harness:
    def __init__(self, current, bot, books, raise_current=False):
        self.published = []
        self.cancelled = []
        self._current = current
        self._bot = bot
        self._books = books
        self._raise = raise_current

    def _fetch_current(self):
        if self._raise:
            raise RuntimeError("LIVE_BLOCKED_SESSION_INVALID")
        return self._current

    def router(self):
        return CashoutRouter(
            fetch_current_orders=self._fetch_current,
            fetch_bot_orders=lambda: self._bot,
            fetch_market_book=lambda m: self._books.get(m),
            cancel_orders=lambda m, ids: self.cancelled.append((m, ids)),
            publish=lambda t, p: self.published.append((t, p)),
            commission_pct=4.5,
        )

    def reqs(self):
        return [p for t, p in self.published if t == REQ_EXECUTE_CASHOUT]


def test_cashout_all_publishes_one_req_per_bot_position():
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", 10.0, avg=2.0),
                 _curr("B2", "1.2", 9, "LAY", 8.0, avg=1.8)],
        bot=[_bot("B1", "1.1", 7, "inter vs milan"),
             _bot("B2", "1.2", 9, "roma vs lazio")],
        books={"1.1": _book("1.1", 7, lay=1.5), "1.2": _book("1.2", 9, back=2.2)},
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 2
    reqs = h.reqs()
    by_market = {r["market_id"]: r for r in reqs}
    assert by_market["1.1"]["side"] == "LAY"   # chiusura di un BACK
    assert by_market["1.2"]["side"] == "BACK"  # chiusura di un LAY
    assert all(r["stake"] > 0 and "green_up" in r for r in reqs)


def test_cashout_single_restricted_to_event_name():
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", 10.0),
                 _curr("B2", "1.2", 9, "BACK", 10.0)],
        bot=[_bot("B1", "1.1", 7, "Inter vs Milan"),
             _bot("B2", "1.2", 9, "Roma vs Lazio")],
        books={"1.1": _book("1.1", 7), "1.2": _book("1.2", 9)},
    )
    out = h.router().route({"signal_type": "CASHOUT", "event_name": "inter vs milan"})
    assert out["published"] == 1
    assert h.reqs()[0]["market_id"] == "1.1"


def test_cashout_single_without_event_name_publishes_nothing():
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", 10.0)],
        bot=[_bot("B1", "1.1", 7, "Inter vs Milan")],
        books={"1.1": _book("1.1", 7)},
    )
    out = h.router().route({"signal_type": "CASHOUT"})
    assert out["published"] == 0
    assert h.reqs() == []


def test_non_bot_orders_are_not_closed():
    # B9 non è negli ordini del bot => non deve diventare un cashout.
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", 10.0),
                 _curr("B9", "1.2", 9, "BACK", 50.0)],  # bet manuale
        bot=[_bot("B1", "1.1", 7)],
        books={"1.1": _book("1.1", 7), "1.2": _book("1.2", 9)},
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 1
    assert h.reqs()[0]["market_id"] == "1.1"


def test_suspended_market_is_skipped_best_effort():
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", 10.0),
                 _curr("B2", "1.2", 9, "BACK", 10.0)],
        bot=[_bot("B1", "1.1", 7), _bot("B2", "1.2", 9)],
        books={"1.1": _book("1.1", 7, status="SUSPENDED"), "1.2": _book("1.2", 9)},
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 1   # solo il mercato OPEN
    assert out["skipped"] == 1
    assert h.reqs()[0]["market_id"] == "1.2"


def test_missing_book_is_skipped():
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", 10.0)],
        bot=[_bot("B1", "1.1", 7)],
        books={},  # nessun book
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 0


def test_resting_is_cancelled_before_publishing():
    # Ordine parzialmente abbinato (matched 6 + resting 4): il resting va
    # cancellato prima del cashout.
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", matched=6.0, remaining=4.0)],
        bot=[_bot("B1", "1.1", 7)],
        books={"1.1": _book("1.1", 7)},
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 1
    assert h.cancelled == [("1.1", ["B1"])]


def test_current_orders_error_aborts_fail_closed():
    h = _Harness(current=[], bot=[_bot("B1", "1.1", 7)], books={}, raise_current=True)
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 0
    assert any(t == CASHOUT_FAILED for t, _ in h.published)


def test_no_bot_orders_does_nothing():
    h = _Harness(current=[_curr("B9", "1.1", 7)], bot=[], books={"1.1": _book("1.1", 7)})
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 0
    assert out["reason"] == "no_bot_orders"


def test_mixed_position_is_skipped_by_resolver():
    # Stessa selezione con matched BACK e LAY (mista) => il resolver la salta.
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", 10.0),
                 _curr("B2", "1.1", 7, "LAY", 4.0)],
        bot=[_bot("B1", "1.1", 7), _bot("B2", "1.1", 7)],
        books={"1.1": _book("1.1", 7)},
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 0


def test_non_cashout_signal_is_ignored():
    h = _Harness(current=[], bot=[], books={})
    out = h.router().route({"signal_type": "QUICK_BET"})
    assert out["reason"] == "not_cashout"
    assert h.published == []
