"""G7-R06 (#586): la riconciliazione chiude un AMBIGUOUS solo con una prova dell'exchange (campagna, percorso esistente).

HeadlessApp reale, exchange SIM. Ingresso con risposta persa dopo l'invio (riga AMBIGUOUS senza betId):
- PASS atteso: prima del settlement la riserva resta trattenuta; il settlement dell'exchange (prova) la chiude per
  identita' (`customerOrderRef`) e contabilizza il P&L confermato;
- DIFETTO NOTO (xfail strict): dopo un riavvio la ricostruzione F018 ricrea la riserva dell'AMBIGUOUS gia' regolato
  (riga mai finalizzata). Correzione proposta sul branch `claude/d-g7-r06-settled-ambiguous`: quando sara' su main
  questo xfail diventa XPASS e va rimosso.
"""
from __future__ import annotations

import pytest

from tests.f026.test_f026_financial import describe, run

pytestmark = pytest.mark.integration

M = "1.996"


@pytest.fixture(scope="module")
def corsa():
    r1 = run([{"op": "mode", "m": M, "mode": "lost"}, {"op": "signal", "m": M, "price": 2.0}, {"op": "wait", "s": 3},
              {"op": "settle", "m": M, "win": True}, {"op": "wait", "s": 3}, {"op": "crash_now"}])
    r2 = run([{"op": "wait", "s": 3}], home=r1["_home"])
    return r1, r2


def _tables(s):
    return {t["market_id"] for t in s["tables"] if t["market_id"]}


def test_r06_chiusura_solo_con_la_prova_dell_exchange(corsa) -> None:
    r1, _r2 = corsa
    prima = [s for s in r1["snaps"] if s["tag"].startswith("2:wait")][0]       # dopo l'ingresso, prima del settlement
    assert M in _tables(prima) and prima["realized"] == 0, describe(prima)
    dopo = [s for s in r1["snaps"] if s["tag"].startswith("3:settle")][0]
    assert M not in _tables(dopo) and dopo["realized"] > 0, describe(dopo)


@pytest.mark.xfail(strict=True, reason="difetto G7-R06: al riavvio F018 ricrea la riserva di un AMBIGUOUS gia' "
                                       "regolato; correzione su claude/d-g7-r06-settled-ambiguous")
def test_r06_dopo_il_riavvio_nessuna_riserva_fantasma(corsa) -> None:
    _r1, r2 = corsa
    assert all(M not in _tables(s) for s in r2["snaps"]), describe(r2["snaps"][-1])
