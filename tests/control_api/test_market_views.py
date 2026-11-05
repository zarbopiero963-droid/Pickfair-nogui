"""MCP-01l: viste GET /v1/markets e GET /v1/markets/{id}/book sul MarketTracker di un RuntimeController reale.

I book sono quelli che il runtime riceve davvero (`RuntimeController.on_market_book` → `MarketTracker`), nella
forma Betfair `listMarketBook`. Nessuna chiamata di rete, nessuna scrittura: un mercato non seguito è NOT_TRACKED.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from control_api.views import MAX_MARKETS, market_book_view, markets_view
from core.runtime_controller import RuntimeController


def book(market_id: str = "1.234567", **over: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "marketId": market_id, "status": "OPEN", "inplay": False, "totalMatched": 1500.25,
        "lastMatchTime": "2026-11-05T10:00:00.000Z",
        "runners": [
            {"selectionId": 47972, "status": "ACTIVE", "lastPriceTraded": 2.5, "totalMatched": 800.0,
             "ex": {"availableToBack": [{"price": 2.5, "size": 100.0}, {"price": 2.48, "size": 40.0},
                                        {"price": 2.46, "size": 10.0}, {"price": 2.44, "size": 5.0}],
                    "availableToLay": [{"price": 2.52, "size": 70.0}]}},
            {"selectionId": 58805, "status": "REMOVED", "ex": {}},
        ],
    }
    data.update(over)
    return data


def feed(rc: RuntimeController, b: dict[str, Any]) -> None:
    rc.market_tracker.on_market_book(b)  # stesso ingresso dello stream/polling del runtime


def test_no_markets_yet(runtime: RuntimeController) -> None:
    v = markets_view(runtime)
    assert (v["markets"], v["omitted"], v["truncated"], v["unavailable"]) == ([], 0, False, [])
    assert market_book_view(runtime, "1.234567")["state"] == "NOT_TRACKED"


def test_markets_and_book_from_the_real_tracker(runtime: RuntimeController) -> None:
    feed(runtime, book())
    feed(runtime, book("1.111", status="SUSPENDED", inplay=True, totalMatched=None, lastMatchTime=None))
    v = markets_view(runtime)
    assert [m["market_id"] for m in v["markets"]] == ["1.111", "1.234567"]
    m = v["markets"][1]
    assert (m["book_status"], m["inplay"], m["runners"], m["total_matched"]) == ("OPEN", False, 2, "1500.25")
    assert m["last_match_time"] == pytest.approx(1793872800.0)
    assert v["markets"][0]["total_matched"] is None and v["markets"][0]["last_match_time"] is None
    b = market_book_view(runtime, "1.234567")
    assert b["state"] == "OK"
    r1, r2 = b["book"]["runners"]
    assert r1["back"] == [{"price": "2.5", "size": "100"}, {"price": "2.48", "size": "40"},
                          {"price": "2.46", "size": "10"}]  # solo i tre migliori livelli
    assert r1["lay"] == [{"price": "2.52", "size": "70"}] and r1["last_price_traded"] == "2.5"
    assert (r2["status"], r2["back"], r2["lay"], r2["last_price_traded"]) == ("REMOVED", [], [], None)


def test_unreadable_values_never_become_zero(runtime: RuntimeController) -> None:
    feed(runtime, book(status="WEIRD", inplay="yes", totalMatched=float("nan"), runners=[
        {"selectionId": 1, "status": "ACTIVE", "lastPriceTraded": -1.0,
         "ex": {"availableToBack": [{"price": 2.0, "size": 1.0}]}},
        {"selectionId": 2, "ex": {"availableToBack": [{"price": "2.0", "size": 1.0}]}},      # stringa
        {"selectionId": 3, "ex": {"availableToBack": [{"price": 2.0, "size": 1.0},
                                                      {"price": 2.2, "size": 1.0}]}},      # fuori ordine
        {"selectionId": None},
        {"selectionId": 1, "ex": {}},                                                      # duplicato
    ]))
    b = market_book_view(runtime, "1.234567")
    assert (b["book"]["book_status"], b["book"]["inplay"], b["book"]["total_matched"]) == ("UNKNOWN", None, None)
    assert [r["selection_id"] for r in b["book"]["runners"]] == ["1"]
    assert b["book"]["runners"][0]["last_price_traded"] is None
    assert b["book"]["omitted_runners"] == 4


def test_tracker_absent_or_corrupt_is_unavailable(runtime: RuntimeController) -> None:
    feed(runtime, book())
    runtime.market_tracker._markets = None
    assert markets_view(runtime)["unavailable"] == ["markets"]
    assert market_book_view(runtime, "1.234567")["state"] == "UNAVAILABLE"
    del runtime.__dict__["market_tracker"]
    assert markets_view(runtime)["markets"] is None


def test_invalid_ids_and_bad_books_are_counted(runtime: RuntimeController) -> None:
    runtime.market_tracker._markets.update({"../x": book(), "1.5": {"marketId": "1.5"}})
    feed(runtime, book("1.6"))
    v = markets_view(runtime)
    assert [m["market_id"] for m in v["markets"]] == ["1.6"] and v["omitted"] == 2
    with pytest.raises(ValueError):
        market_book_view(runtime, "../x")
    assert market_book_view(runtime, "1.5")["state"] == "UNAVAILABLE"


def test_truncation_is_declared(runtime: RuntimeController) -> None:
    for i in range(MAX_MARKETS + 3):
        runtime.market_tracker._markets[f"1.{i + 1}"] = book(f"1.{i + 1}")
    v = markets_view(runtime)
    assert len(v["markets"]) == MAX_MARKETS and v["truncated"] is True


def test_reads_have_no_effects(runtime: RuntimeController) -> None:
    feed(runtime, book())
    published = list(runtime.bus.published)
    before = json.dumps(runtime.market_tracker._markets, sort_keys=True, default=str)
    for _ in range(20):
        markets_view(runtime)
        market_book_view(runtime, "1.234567")
    assert runtime.bus.published == published
    assert json.dumps(runtime.market_tracker._markets, sort_keys=True, default=str) == before


def test_views_pass_the_server_contract(runtime: RuntimeController) -> None:
    from pickfair_control_api.contract import response_schema
    from pickfair_control_api.schema_check import validate

    validate(markets_view(runtime), response_schema("markets"))
    validate(market_book_view(runtime, "1.234567"), response_schema("market_book"))
    feed(runtime, book())
    validate(markets_view(runtime), response_schema("markets"))
    validate(market_book_view(runtime, "1.234567"), response_schema("market_book"))
