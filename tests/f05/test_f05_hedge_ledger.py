"""F-05 (#586, CSH-06 P0) — l'hedge del cashout deve essere scritto nel ledger `orders` (RED prima della patch).

Riproduzione con HeadlessApp REALE in SIM (child di tests/f026, SQLite su disco): una posizione BACK abbinata, poi
cashout parziale di quel mercato. Oggi l'hedge LAY e' eseguito sull'exchange (CASHOUT_SUCCESS, gamba LAY abbinata in
`currentOrders`) ma NESSUNA riga `orders` lo rappresenta: daily-loss LIVE, riconciliazione e storico non lo vedono.

Contratto atteso (F05_PHASE0 §7):
- una riga `orders` per l'hedge, `customer_ref` con prefisso ``hedge:``, payload con ``role == "HEDGE"`` e
  ``order_origin == "CASHOUT_HEDGE"``, mercato/selezione/lato/prezzo/stake dell'hedge, senza ``event_key``/``table_id``;
- la riga NON crea un tavolo fantasma al riavvio (F018 rebuild) e non fa partire il runtime in PAUSED;
- le invarianti finanziarie F026_10/F026_12 restano verdi (eseguite dalla loro suite).
"""
from __future__ import annotations

import glob
import json
import os
import sqlite3

import pytest

from tests.f026.test_f026_financial import run

pytestmark = [pytest.mark.integration, pytest.mark.safety]


def _orders(home):
    dbs = [p for p in glob.glob(os.path.join(home, "**", "*.db"), recursive=True) if os.path.getsize(p) > 0]
    rows = []
    for path in dbs:
        con = sqlite3.connect(path)
        try:
            tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if "orders" not in tables:
                continue
            for ref, status, payload in con.execute("SELECT customer_ref, status, payload_json FROM orders"):
                rows.append({"ref": ref, "status": status, "payload": json.loads(payload or "{}")})
        finally:
            con.close()
    return rows


def _position_then_partial_cashout(home=None):
    return run([{"op": "signal", "m": "1.951", "price": 2.0}, {"op": "cashout", "m": "1.951"}], home=home)


def test_cashout_hedge_is_persisted_in_the_orders_ledger():
    r = _position_then_partial_cashout()
    s = r["snaps"][-1]
    assert sum(1 for e in r["events"] if e["e"] == "CASHOUT_SUCCESS") == 1, "precondizione: cashout eseguito"
    lay_legs = [o for o in s["exch_orders"] if o["market_id"] == "1.951" and o["side"] == "LAY"
                and float(o.get("matched") or 0) > 0]
    assert lay_legs, "precondizione: hedge LAY abbinato sull'exchange"
    hedges = [o for o in _orders(r["_home"]) if str(o["ref"]).startswith("hedge:")]
    assert len(hedges) == 1, f"hedge eseguito sull'exchange ma assente dal ledger orders: {_orders(r['_home'])}"
    p = hedges[0]["payload"]
    assert p.get("role") == "HEDGE" and p.get("order_origin") == "CASHOUT_HEDGE", p
    assert str(p.get("market_id")) == "1.951" and str(p.get("bet_type")).upper() == "LAY", p
    assert not p.get("event_key") and not p.get("table_id"), "l'hedge non e' un ingresso: niente tavolo/evento"
    assert float(p.get("stake")) == pytest.approx(float(lay_legs[0]["size"])), (p, lay_legs)


def test_persisted_hedge_creates_no_phantom_table_and_no_paused_start_after_restart():
    import tempfile
    home = tempfile.mkdtemp(prefix="f05_restart_")
    first = _position_then_partial_cashout(home)
    assert any(str(o["ref"]).startswith("hedge:") for o in _orders(home)), "RED: hedge non persistito"
    second = run([{"op": "wait", "s": 2}], home=home)
    s0 = second["snaps"][0]
    assert str(s0["mode"]).upper() != "PAUSED", f"avvio in PAUSED con la riga hedge nel ledger: {s0['mode']}"
    assert not [t for t in s0["tables"] if t["market_id"] == "1.951" and "hedge" in str(t.get("event_key"))], s0
    assert len([t for t in s0["tables"] if t["market_id"]]) <= len([t for t in first["snaps"][-1]["tables"]
                                                                     if t["market_id"]]), "tavolo fantasma"


def test_hedged_position_settles_once_releases_its_table_and_books_the_net_pnl():
    """Con la riga hedge nel ledger il settlement del mercato libera il tavolo e contabilizza UNA volta il netto
    ingresso + hedge (oracolo Decimal indipendente, come F026_12): la riga hedge non e' un secondo ingresso."""
    from decimal import ROUND_HALF_UP
    from decimal import Decimal as D

    from tests.f026.test_f026_financial import COMM, q
    r = run([{"op": "signal", "m": "1.951", "price": 2.0}, {"op": "cashout", "m": "1.951"},
             {"op": "settle", "m": "1.951", "win": True}])
    s = r["snaps"][-1]
    assert any(str(o["ref"]).startswith("hedge:") for o in _orders(r["_home"])), "hedge non nel ledger"
    back, lay, stake = D("2.00"), D("2.02"), D("3")
    hedge = (stake * back / lay).quantize(D("0.01"), rounding=ROUND_HALF_UP)
    net = stake * (back - 1) - hedge * (lay - 1)
    exp = net * (1 - COMM) if net > 0 else net
    assert [t for t in s["tables"] if t["market_id"] == "1.951"] == [], f"tavolo non liberato: {s['tables']}"
    assert q(s["realized"]) == q(exp), f"realizzato {s['realized']} != oracolo {q(exp)} (doppio conteggio?)"
    assert len([x for x in s["exch_settlements"] if x["market_id"] == "1.951"]) == 1
