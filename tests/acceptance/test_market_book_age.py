"""Eta' del book per mercato (richiesta Control API, Claude C 05/11/2026): istante MONOTONO di ricezione nel MarketTracker,
cosi' la freschezza del book e' dimostrabile senza dipendere da `lastMatchTime` dell'exchange."""
from __future__ import annotations

import pytest

from core.market_tracker import MarketTracker


class _Clock:
    def __init__(self, t: float) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


def test_eta_misurata_dalla_ricezione_con_orologio_monotono() -> None:
    clock = _Clock(100.0)
    mt = MarketTracker(monotonic=clock)
    mt.on_market_book({"marketId": "1.1", "status": "OPEN", "runners": []})
    clock.t = 107.5

    assert mt.received_at("1.1") == 100.0
    assert mt.book_age_s("1.1") == pytest.approx(7.5)


def test_ogni_nuovo_book_aggiorna_l_istante() -> None:
    clock = _Clock(10.0)
    mt = MarketTracker(monotonic=clock)
    mt.on_market_book({"market_id": "1.2"})
    clock.t = 20.0
    mt.on_market_book({"marketId": "1.2"})
    clock.t = 21.0
    assert mt.book_age_s("1.2") == pytest.approx(1.0)


@pytest.mark.parametrize("book", [None, "x", {}, {"marketId": ""}])
def test_block_book_non_valido_nessun_istante(book) -> None:
    mt = MarketTracker(monotonic=_Clock(1.0))
    mt.on_market_book(book)
    assert mt.snapshot()["received_at"] == {}


def test_mercato_mai_ricevuto_eta_ignota() -> None:
    mt = MarketTracker(monotonic=_Clock(1.0))
    assert mt.received_at("9.9") is None and mt.book_age_s("9.9") is None


def test_orologio_che_torna_indietro_non_da_eta_negativa() -> None:
    clock = _Clock(50.0)
    mt = MarketTracker(monotonic=clock)
    mt.on_market_book({"marketId": "1.3"})
    clock.t = 40.0
    assert mt.book_age_s("1.3") is None


def test_default_usa_time_monotonic() -> None:
    import time

    mt = MarketTracker()
    mt.on_market_book({"marketId": "1.4"})
    age = mt.book_age_s("1.4")
    assert age is not None and 0.0 <= age < 5.0
    assert mt.received_at("1.4") <= time.monotonic()
