"""G7-R06 (#586): un ordine AMBIGUOUS gia' regolato dall'exchange non rinasce come posizione aperta al riavvio.

Riprodotto su main `911562a` (HeadlessApp reale, SQLite, poller del settlement reale, exchange SIM): ingresso con
risposta persa dopo l'invio (AMBIGUOUS, nessun betId nella riga), settlement del mercato attribuito al suo intento via
`customerOrderRef` (tavolo rilasciato, P&L contabilizzato), crash, riavvio. Prima della correzione la ricostruzione F018
riprendeva la riga AMBIGUOUS come posizione incerta: tavolo occupato e riserva trattenuta per sempre su un mercato gia'
chiuso. La prova di chiusura e' l'effetto di settlement registrato (`settlement_effects`, chiave citata); senza prova la
riga resta incerta (F028_04). Quote SINTETICHE.
"""
from __future__ import annotations

import pytest

from tests.f026.test_f026_financial import describe, run

pytestmark = pytest.mark.integration

M = "1.997"


def _tables(s):
    return {t["market_id"] for t in s["tables"] if t["market_id"]}


@pytest.fixture(scope="module")
def riavvio():
    r1 = run([{"op": "mode", "m": M, "mode": "lost"}, {"op": "signal", "m": M, "price": 2.0}, {"op": "wait", "s": 3},
              {"op": "settle", "m": M, "win": True}, {"op": "wait", "s": 3}, {"op": "crash_now"}])
    r2 = run([{"op": "wait", "s": 3}], home=r1["_home"])
    return r1, r2


def test_precondizione_ambiguous_regolato_prima_del_crash(riavvio) -> None:
    r1, _r2 = riavvio
    s = r1["snaps"][-1]
    assert [o["status"] for o in s["orders"] if o["market_id"] == M] == ["AMBIGUOUS"], describe(s)
    assert M not in _tables(s), "settlement non attribuito: " + describe(s)
    assert s["realized"] > 0, describe(s)


def test_block_ambiguous_regolato_non_rinasce_al_riavvio(riavvio) -> None:
    _r1, r2 = riavvio
    for s in r2["snaps"]:
        assert M not in _tables(s), "riserva fantasma ricostruita su un mercato gia' regolato: " + describe(s)
        assert s["mode"] == "ACTIVE", describe(s)


def test_ambiguous_mai_regolato_resta_trattenuto_al_riavvio() -> None:
    """Controllo opposto (F028_04): senza prova dell'exchange la riga incerta resta trattenuta anche dopo il riavvio."""
    r1 = run([{"op": "mode", "m": "1.998", "mode": "lost"}, {"op": "signal", "m": "1.998", "price": 2.0},
              {"op": "wait", "s": 3}, {"op": "crash_now"}])
    r2 = run([{"op": "wait", "s": 3}], home=r1["_home"])
    assert "1.998" in _tables(r2["snaps"][-1]), describe(r2["snaps"][-1])
