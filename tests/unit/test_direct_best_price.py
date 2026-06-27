"""Unit test per direct_best_price.resolve_direct_best_price (Fase 2.2 / B6.1)."""

import pytest

from direct_best_price import (
    SOURCE_FALLBACK_MASTER,
    SOURCE_LIVE_BOOK,
    resolve_direct_best_price,
)

# Il job CI "unit" gira `-m unit`: senza marker questi test verrebbero deselezionati.
pytestmark = pytest.mark.unit


def _with_size(levels):
    """Inietta una size eseguibile di default sui livelli che non la specificano."""
    out = []
    for lv in levels or []:
        lv = dict(lv)
        lv.setdefault("size", 100.0)
        out.append(lv)
    return out


def _book(*, status="OPEN", selection_id=55, runner_status="ACTIVE",
          backs=None, lays=None):
    ex = {}
    if backs is not None:
        ex["availableToBack"] = _with_size(backs)
    if lays is not None:
        ex["availableToLay"] = _with_size(lays)
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


def test_nested_market_definition_suspended_falls_back():
    # Formato streaming: lo status sospeso vive sotto marketDefinition, niente top-level.
    book = {
        "marketDefinition": {"status": "SUSPENDED"},
        "runners": [{"selectionId": 55, "status": "ACTIVE",
                     "ex": {"availableToBack": [{"price": 2.52}]}}],
    }
    r = _resolve(book, side="BACK")
    assert r["reason"] == "market_not_open"
    assert r["price"] == 2.50


def test_malformed_schema_falls_back_not_raises():
    # Tipi inattesi (runners non-lista, ex non-dict, ladder feed-internal [[p,s]])
    # NON devono sollevare: fail-closed al master.
    for book in (
        {"status": "OPEN", "runners": 5},
        {"status": "OPEN", "runners": [{"selectionId": 55, "status": "ACTIVE", "ex": [1, 2]}]},
        {"status": "OPEN", "runners": [{"selectionId": 55, "status": "ACTIVE",
                                        "ex": {"availableToBack": [[2.5, 100]]}}]},
    ):
        r = _resolve(book, side="BACK")
        assert r["source"] == SOURCE_FALLBACK_MASTER
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


def test_runner_missing_status_is_fail_closed():
    # Status del runner assente = stato attivo non confermabile (book parziale)
    # => fallback, coerente con la severità del market status.
    book = {
        "status": "OPEN",
        "runners": [{"selectionId": 55,   # nessun campo 'status'
                     "ex": {"availableToBack": [{"price": 2.52, "size": 10}]}}],
    }
    r = _resolve(book, side="BACK")
    assert r["reason"] == "runner_not_active"
    assert r["price"] == 2.50


def test_invalid_book_price_falls_back():
    book = _book(backs=[{"price": 1.0}])             # <= 1.0 non valido
    r = _resolve(book, side="BACK")
    assert r["reason"] == "invalid_book_price"
    assert r["price"] == 2.50


def test_zero_size_level_is_no_liquidity():
    # Livello presente ma con size 0 = nessuna liquidità eseguibile => fallback.
    book = {
        "status": "OPEN",
        "runners": [{"selectionId": 55, "status": "ACTIVE",
                     "ex": {"availableToBack": [{"price": 2.52, "size": 0}]}}],
    }
    r = _resolve(book, side="BACK")
    assert r["reason"] == "no_side_liquidity"
    assert r["price"] == 2.50


def test_non_finite_book_price_falls_back():
    book = _book(backs=[{"price": float("nan")}])
    r = _resolve(book, side="BACK")
    assert r["source"] == SOURCE_FALLBACK_MASTER
    assert r["price"] == 2.50
    assert r["reason"] == "invalid_book_price"


def test_missing_status_is_fail_closed():
    # Nessuno status (né top-level né marketDefinition) => non confermabile OPEN.
    book = {"runners": [{"selectionId": 55, "status": "ACTIVE",
                         "ex": {"availableToBack": [{"price": 2.52, "size": 10}]}}]}
    r = _resolve(book, side="BACK")
    assert r["reason"] == "market_not_open"
    assert r["price"] == 2.50


def test_invalid_tolerance_falls_back():
    book = _book(backs=[{"price": 2.52}])
    for bad in (float("nan"), float("inf"), -1.0):
        r = resolve_direct_best_price(market_book=book, selection_id=55,
                                      side="BACK", master_price=2.50,
                                      max_deviation_pct=bad)
        assert r["reason"] == "invalid_tolerance"
        assert r["price"] == 2.50


def test_malformed_tolerance_is_not_silent_zero():
    # None / stringa non numerica NON devono diventare tolleranza 0.0 silenziosa:
    # best == master (deviation 0) smaschererebbe il guard mancante (0>0 = False).
    book = _book(backs=[{"price": 2.50}])
    for bad in (None, "abc", object()):
        r = resolve_direct_best_price(market_book=book, selection_id=55,
                                      side="BACK", master_price=2.50,
                                      max_deviation_pct=bad)
        assert r["reason"] == "invalid_tolerance"
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


def test_fractional_selection_id_rejected():
    # 55.9 NON deve essere troncato a 55: id frazionario = payload malformato.
    book = _book(backs=[{"price": 2.52}])
    r = resolve_direct_best_price(market_book=book, selection_id=55.9,
                                  side="BACK", master_price=2.50)
    assert r["reason"] == "invalid_selection_id"
    assert r["price"] == 2.50


def test_fractional_runner_selection_id_not_matched():
    # Anche un selectionId frazionario nel book non deve combaciare col target.
    book = {
        "status": "OPEN",
        "runners": [{"selectionId": 55.9, "status": "ACTIVE",
                     "ex": {"availableToBack": [{"price": 2.52, "size": 10}]}}],
    }
    r = _resolve(book, side="BACK", sel=55)
    assert r["reason"] == "runner_missing"
    assert r["price"] == 2.50


def test_integral_float_selection_id_still_matches():
    # 55.0 è un intero valido: deve continuare a combaciare (no falsi negativi).
    book = {
        "status": "OPEN",
        "runners": [{"selectionId": 55.0, "status": "ACTIVE",
                     "ex": {"availableToBack": [{"price": 2.52, "size": 10}]}}],
    }
    r = _resolve(book, side="BACK", sel=55)
    assert r["price"] == 2.52
    assert r["source"] == SOURCE_LIVE_BOOK
