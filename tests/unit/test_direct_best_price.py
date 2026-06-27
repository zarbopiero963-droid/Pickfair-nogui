"""Unit test per direct_best_price.resolve_direct_best_price (Fase 2.2 / B6.1)."""

from direct_best_price import (
    SOURCE_FALLBACK_MASTER,
    SOURCE_LIVE_BOOK,
    resolve_direct_best_price,
)


def _book(*, status="OPEN", selection_id=55, runner_status="ACTIVE",
          backs=None, lays=None):
    ex = {}
    if backs is not None:
        ex["availableToBack"] = backs
    if lays is not None:
        ex["availableToLay"] = lays
    return {
        "status": status,
        "runners": [
            {"selectionId": selection_id, "status": runner_status, "ex": ex},
        ],
    }


def _resolve(book, *, side="BACK", master=2.50, sel=55, tol=2.0):
    return resolve_direct_best_price(
        market_book=book, selection_id=sel, side=side,
        master_price=master, max_deviation_pct=tol,
    )


# =========================================================
# Happy path BACK / LAY (best entro tolleranza)
# =========================================================
def test_back_uses_best_available_back_within_tolerance():
    book = _book(backs=[{"price": 2.52, "size": 100}], lays=[{"price": 2.60}])
    r = _resolve(book, side="BACK", master=2.50)
    assert r["price"] == 2.52
    assert r["source"] == SOURCE_LIVE_BOOK
    assert r["within_tolerance"] is True
    assert r["reason"] == "ok"


def test_lay_uses_best_available_lay_within_tolerance():
    book = _book(backs=[{"price": 2.40}], lays=[{"price": 2.48, "size": 50}])
    r = _resolve(book, side="LAY", master=2.50)
    assert r["price"] == 2.48
    assert r["source"] == SOURCE_LIVE_BOOK


# =========================================================
# Difensivo: mai cross-spread (lato richiesto assente => fallback, non l'altro)
# =========================================================
def test_back_does_not_cross_to_lay_side():
    # Solo liquidità LAY disponibile: un BACK NON deve prendere il prezzo LAY.
    book = _book(backs=[], lays=[{"price": 2.60}])
    r = _resolve(book, side="BACK", master=2.50)
    assert r["price"] == 2.50                       # master, non 2.60
    assert r["source"] == SOURCE_FALLBACK_MASTER
    assert r["reason"] == "no_side_liquidity"


def test_lay_does_not_cross_to_back_side():
    book = _book(backs=[{"price": 2.40}], lays=[])
    r = _resolve(book, side="LAY", master=2.50)
    assert r["price"] == 2.50
    assert r["reason"] == "no_side_liquidity"


# =========================================================
# Tolleranza: drift oltre soglia => fallback master
# =========================================================
def test_price_drift_beyond_tolerance_falls_back_to_master():
    book = _book(backs=[{"price": 3.00}])            # +20% vs 2.50
    r = _resolve(book, side="BACK", master=2.50, tol=2.0)
    assert r["price"] == 2.50
    assert r["source"] == SOURCE_FALLBACK_MASTER
    assert r["reason"] == "out_of_tolerance"
    assert r["within_tolerance"] is False
    assert r["best_price"] == 3.00                   # osservato ma non applicato
    assert r["deviation_pct"] == 20.0


def test_price_just_within_tolerance_is_used():
    book = _book(backs=[{"price": 2.55}])            # +2% esatto vs 2.50, tol 2%
    r = _resolve(book, side="BACK", master=2.50, tol=2.0)
    assert r["price"] == 2.55
    assert r["within_tolerance"] is True


# =========================================================
# Fail-closed: book/mercato/runner anomali => master
# =========================================================
def test_empty_or_missing_book_falls_back():
    assert _resolve(None)["reason"] == "no_book"
    assert _resolve({})["reason"] == "no_book"
    for r in (_resolve(None), _resolve({})):
        assert r["price"] == 2.50
        assert r["source"] == SOURCE_FALLBACK_MASTER


def test_market_suspended_falls_back():
    book = _book(status="SUSPENDED", backs=[{"price": 2.52}])
    r = _resolve(book, side="BACK")
    assert r["reason"] == "market_not_open"
    assert r["price"] == 2.50


def test_runner_missing_falls_back():
    book = _book(selection_id=999, backs=[{"price": 2.52}])
    r = _resolve(book, side="BACK", sel=55)          # cerco 55, c'è 999
    assert r["reason"] == "runner_missing"
    assert r["price"] == 2.50


def test_runner_not_active_falls_back():
    book = _book(runner_status="REMOVED", backs=[{"price": 2.52}])
    r = _resolve(book, side="BACK")
    assert r["reason"] == "runner_not_active"
    assert r["price"] == 2.50


def test_invalid_book_price_falls_back():
    book = _book(backs=[{"price": 1.0}])             # <= 1.0 non valido
    r = _resolve(book, side="BACK")
    assert r["reason"] == "invalid_book_price"
    assert r["price"] == 2.50


# =========================================================
# Input invalidi => master
# =========================================================
def test_invalid_master_price_falls_back():
    book = _book(backs=[{"price": 2.52}])
    r = _resolve(book, side="BACK", master=1.0)
    assert r["reason"] == "invalid_master_price"


def test_invalid_side_falls_back():
    book = _book(backs=[{"price": 2.52}])
    r = _resolve(book, side="SOMETHING", master=2.50)
    assert r["reason"] == "invalid_side"
    assert r["price"] == 2.50


def test_bool_selection_id_rejected():
    book = _book(backs=[{"price": 2.52}])
    r = resolve_direct_best_price(market_book=book, selection_id=True,
                                  side="BACK", master_price=2.50)
    assert r["reason"] == "invalid_selection_id"
