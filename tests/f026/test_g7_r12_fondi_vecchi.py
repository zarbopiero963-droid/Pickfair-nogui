"""G7-R12 (#586): fondi vecchi o non verificabili => nessun ingresso, su HeadlessApp reale (F-16).

La soglia di freschezza della conferma dei fondi e' forzata a 0 s: ogni lettura e' gia' vecchia quando arriva al gate.
Il segnale deve essere rifiutato con `FUNDS_STALE` e nessun ordine deve raggiungere l'exchange SIM. Controllo opposto:
con la soglia normale lo stesso segnale entra. Senza F-16 (`RuntimeController.funds_max_age_s` assente) SALTATO.
"""
from __future__ import annotations

import pytest

from core.runtime_controller import RuntimeController
from tests.f026.test_f026_financial import describe, run

pytestmark = [pytest.mark.integration, pytest.mark.skipif(
    not hasattr(RuntimeController, "funds_max_age_s"),
    reason="richiede F-16 (claude/d-f16-funds-freshness): freschezza dei fondi non su main")]


def test_block_fondi_vecchi_nessun_ordine_all_exchange() -> None:
    r = run([{"op": "funds_max_age", "s": 0}, {"op": "signal", "m": "1.981", "price": 2.0}])
    s = r["snaps"][-1]
    assert [o for o in s["exch_orders"] if str(o["market_id"]) == "1.981"] == [], describe(s)
    assert any(e["e"] == "SIGNAL_REJECTED" and "FUNDS_STALE" in str(e["reason"]) for e in r["events"]), r["events"]
    assert "1.981" not in {t["market_id"] for t in s["tables"] if t["market_id"]}, "riserva rimasta su un rifiuto"


def test_fondi_freschi_lo_stesso_segnale_entra() -> None:
    r = run([{"op": "signal", "m": "1.982", "price": 2.0}])
    s = r["snaps"][-1]
    assert len([o for o in s["exch_orders"] if str(o["market_id"]) == "1.982"]) == 1, describe(s)
