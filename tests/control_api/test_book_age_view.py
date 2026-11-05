"""MCP-01n: età del book (`book_age_s`) nelle viste mercati, dal `MarketTracker.book_age_s` di Claude D.

Il core usato è la fusione LOCALE (mai spinta) del ponte con `claude/d-market-book-age` (ea81e93e). L'età viene
dall'istante monotono di ricezione del tracker: il ponte non la ricalcola, la legge prima dei book (mai
sottostimata) e la dichiara null quando non è dimostrabile.
"""

from __future__ import annotations

from typing import Any

import pytest

from control_api import views
from control_api.views import market_book_view, markets_view
from core.market_tracker import MarketTracker
from core.runtime_controller import RuntimeController


class Clock:
    def __init__(self, now: float = 100.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


def book(market_id: str = "1.234567") -> dict[str, Any]:
    return {"marketId": market_id, "status": "OPEN", "inplay": False, "totalMatched": 10.0,
            "runners": [{"selectionId": 1, "status": "ACTIVE",
                         "ex": {"availableToBack": [{"price": 2.0, "size": 5.0}]}}]}


@pytest.fixture
def clock(runtime: RuntimeController, monkeypatch: pytest.MonkeyPatch) -> Clock:
    c = Clock()
    monkeypatch.setattr(runtime.market_tracker, "_monotonic", c)
    return c


def ages(rc: RuntimeController) -> dict[str, Any]:
    return {m["market_id"]: m["book_age_s"] for m in markets_view(rc)["markets"]}


def test_age_comes_from_the_tracker_monotonic_instant(runtime: RuntimeController, clock: Clock) -> None:
    runtime.market_tracker.on_market_book(book("1.1"))
    clock.now = 101.0
    runtime.market_tracker.on_market_book(book("1.2"))
    clock.now = 103.5
    assert ages(runtime) == {"1.1": "3.5", "1.2": "2.5"}
    assert market_book_view(runtime, "1.1")["book"]["book_age_s"] == "3.5"
    assert runtime.market_tracker.book_age_s("1.1") == 3.5  # stessa regola del core, nessun ricalcolo


def test_just_received_book_has_age_zero_not_null(runtime: RuntimeController, clock: Clock) -> None:
    runtime.market_tracker.on_market_book(book("1.1"))
    assert ages(runtime) == {"1.1": "0"}


def test_clock_going_backwards_is_unknown_never_fresh(runtime: RuntimeController, clock: Clock) -> None:
    runtime.market_tracker.on_market_book(book("1.1"))
    clock.now = 99.0
    assert ages(runtime) == {"1.1": None}
    assert market_book_view(runtime, "1.1")["book"]["book_age_s"] is None


def test_book_without_receive_instant_has_unknown_age(runtime: RuntimeController, clock: Clock) -> None:
    runtime.market_tracker.on_market_book(book("1.1"))
    runtime.market_tracker._markets["1.9"] = MarketTracker._normalize_market_book(  # noqa: SLF001
        runtime.market_tracker, book("1.9"))
    assert ages(runtime) == {"1.1": "0", "1.9": None}


def test_failing_age_only_blanks_that_market(runtime: RuntimeController, clock: Clock,
                                             monkeypatch: pytest.MonkeyPatch) -> None:
    runtime.market_tracker.on_market_book(book("1.1"))
    runtime.market_tracker.on_market_book(book("1.2"))
    clock.now = 102.0
    real = runtime.market_tracker.book_age_s

    def flaky(market_id: str) -> Any:
        if market_id == "1.1":
            raise RuntimeError("boom")
        return real(market_id)

    monkeypatch.setattr(runtime.market_tracker, "book_age_s", flaky)
    assert ages(runtime) == {"1.1": None, "1.2": "2"}
    monkeypatch.setattr(runtime.market_tracker, "book_age_s", lambda _m: float("nan"))
    assert ages(runtime) == {"1.1": None, "1.2": None}  # non rappresentabile: ignota


def test_tracker_without_book_age_declares_null(runtime: RuntimeController,
                                                monkeypatch: pytest.MonkeyPatch) -> None:
    # forma di main prima di D: nessun book_age_s => età ignota, mai inventata da last_match_time
    monkeypatch.setattr(MarketTracker, "book_age_s", None)
    runtime.market_tracker.on_market_book(book("1.1"))
    assert ages(runtime) == {"1.1": None}
    assert market_book_view(runtime, "1.1")["book"]["book_age_s"] is None
    monkeypatch.setattr(runtime.market_tracker, "_received_at", None)
    monkeypatch.setattr(MarketTracker, "book_age_s", lambda self, m: 1.0)
    assert ages(runtime) == {"1.1": None}


def test_age_is_read_before_the_books(runtime: RuntimeController, clock: Clock,
                                      monkeypatch: pytest.MonkeyPatch) -> None:
    # il tracker scrive book e istante in due passi: leggere prima l'età la rende al più sovrastimata
    order: list[str] = []
    real_books = views._tracked_books  # noqa: SLF001
    real_age = runtime.market_tracker.book_age_s
    monkeypatch.setattr(views, "_tracked_books", lambda rc: (order.append("books"), real_books(rc))[1])
    monkeypatch.setattr(runtime.market_tracker, "book_age_s", lambda m: (order.append("age"), real_age(m))[1])
    runtime.market_tracker.on_market_book(book("1.1"))
    markets_view(runtime)
    assert order == ["age", "books"]
    order.clear()
    market_book_view(runtime, "1.1")
    assert order == ["age", "books"]


def test_unavailable_tracker_still_unavailable(runtime: RuntimeController, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delattr(runtime, "market_tracker")
    v = markets_view(runtime)
    assert (v["markets"], v["unavailable"]) == (None, ["markets"])


def test_views_pass_the_server_contract(runtime: RuntimeController, clock: Clock) -> None:
    from pickfair_control_api.contract import response_schema
    from pickfair_control_api.schema_check import validate

    runtime.market_tracker.on_market_book(book("1.1"))
    clock.now = 100.125
    validate(markets_view(runtime), response_schema("markets"))
    validate(market_book_view(runtime, "1.1"), response_schema("market_book"))
    clock.now = 50.0
    validate(markets_view(runtime), response_schema("markets"))
