"""F-33 — oracolo finanziario INDIPENDENTE (Decimal), senza import del codice sotto test.

Invarianti del piano §6 verificati sugli snapshot del child runner:
 1  conservazione: disponibile + impegnato (exchange) = saldo iniziale + realizzato (+ variazioni esterne);
 2  separazione saldo / disponibile / riserve / liability (esposizione tavoli = capitale a rischio delle posizioni vive);
 3  nessun doppio invio per lo stesso intento (un solo ordine SUBMITTED/COMPLETED per mercato di segnale);
 4  nessuna liberazione prematura: tavolo vivo finche' il mercato non e' regolato;
 6  P&L BACK/LAY: BACK vince = matched*(p-1)*(1-c), perde = -matched; LAY vince = -matched*(p-1), perde = matched*(1-c);
 7  commissione applicata una volta sul netto di mercato (SIM: commission_pct);
 8  settlement idempotente: un solo settlement per mercato nel registro exchange;
 9  debito persistente dopo restart: il realizzato negativo non scompare;
 11 nessun ordine reale: netguard nel sottoprocesso.
Il cashout (hedge LAY) e' contabilizzato quando compare nel ledger SQLite con il suo prezzo e size.
"""
from __future__ import annotations

from decimal import ROUND_HALF_UP
from decimal import Decimal as D
from typing import Any, Dict, Iterable, List, Optional, Set

CENT = D("0.01")
MILLI = D("0.001")


def q(x: Any, step: D = MILLI) -> D:
    return D(str(x)).quantize(step, rounding=ROUND_HALF_UP)


def side_pnl(side: str, matched: D, price: D, win: bool, commission: D) -> D:
    side = (side or "BACK").upper()
    if side == "BACK":
        return matched * (price - 1) * (1 - commission) if win else -matched
    # LAY: vince se la selezione PERDE
    return -(matched * (price - 1)) if win else matched * (1 - commission)


class Oracle:
    """Ricostruisce dal ledger SQLite (``snap['orders']``) e dagli esiti dichiarati il valore atteso."""

    OPEN_STATES = {"SUBMITTED", "AMBIGUOUS", "INFLIGHT", "COMPLETED", "PARTIAL"}

    def __init__(self, snap: Dict[str, Any], outcomes: Dict[str, bool], *, start: D, commission: D,
                 external: D = D(0), extra_legs: Optional[List[Dict[str, Any]]] = None) -> None:
        self.snap = snap
        self.outcomes = dict(outcomes)
        self.start = D(str(start))
        self.commission = D(str(commission))
        self.external = D(str(external))
        self.orders: List[Dict[str, Any]] = [o for o in snap.get("orders", []) if o.get("status") not in ("DUPLICATE_BLOCKED", "FAILED", "DENIED")]
        # Gambe presenti sull'EXCHANGE ma assenti dal ledger SQLite (oggi: l'hedge del cashout, CSH-06 -> F-05).
        # L'oracolo le conta perche' il denaro e' stato mosso davvero; la loro assenza dal ledger e' un finding a parte.
        for leg in extra_legs or []:
            if not any(str(o.get("market_id")) == str(leg["market_id"]) and (o.get("side") or "BACK").upper() == leg["side"]
                       and D(str(o.get("price") or 0)) == D(str(leg["price"])) for o in self.orders):
                self.orders.append({"market_id": str(leg["market_id"]), "side": leg["side"], "stake": leg["matched"],
                                    "matched": leg["matched"], "price": leg["price"], "status": "COMPLETED",
                                    "customer_ref": leg.get("ref"), "_from_exchange": True})

    def orders_of(self, market: str) -> List[Dict[str, Any]]:
        return [o for o in self.orders if str(o.get("market_id")) == str(market)]

    def pnl_market(self, market: str) -> D:
        """Netto di mercato = somma dei lordi per gamba (ingresso + eventuale hedge), commissione una volta sul netto > 0."""
        win = self.outcomes[market]
        gross = D(0)
        for o in self.orders_of(market):
            matched = o.get("matched")
            if matched is None:
                matched = o.get("stake")
            if matched is None or o.get("price") is None:
                continue
            m, p = D(str(matched)), D(str(o["price"]))
            if m <= 0:
                continue
            gross += side_pnl(o.get("side") or "BACK", m, p, win, D(0))
        return gross * (1 - self.commission) if gross > 0 else gross

    def realized(self) -> D:
        return sum((self.pnl_market(m) for m in self.outcomes if self.orders_of(m)), D(0))

    def equity(self) -> D:
        return self.start + self.realized() + self.external

    def open_markets(self) -> Set[str]:
        return {str(o["market_id"]) for o in self.orders
                if str(o["market_id"]) not in self.outcomes and o.get("status") in self.OPEN_STATES}

    def capital_at_risk(self) -> D:
        tot = D(0)
        for m in self.open_markets():
            for o in self.orders_of(m):
                stake, price = D(str(o.get("matched") if o.get("matched") is not None else o.get("stake") or 0)), D(str(o.get("price") or 1))
                tot += stake if (o.get("side") or "BACK").upper() == "BACK" else stake * (price - 1)
        return tot


def exchange_legs(snap: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Gambe abbinate viste dall'exchange SIM (currentOrders): market, side, matched, price, ref."""
    legs = []
    for o in snap.get("exch_orders", []):
        matched = o.get("matched")
        if matched is None or float(matched) <= 0:
            continue
        legs.append({"market_id": str(o.get("market_id")), "side": str(o.get("side") or "BACK").upper(),
                     "matched": matched, "price": o.get("price"), "ref": o.get("ref")})
    return legs


def ledger_missing_exchange_legs(snap: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Gambe abbinate sull'exchange senza riga corrispondente (mercato+lato) nel ledger SQLite."""
    missing = []
    for leg in exchange_legs(snap):
        if not any(str(o.get("market_id")) == leg["market_id"] and (o.get("side") or "BACK").upper() == leg["side"]
                   for o in snap.get("orders", [])):
            missing.append(leg)
    return missing


def table_markets(snap: Dict[str, Any]) -> Set[str]:
    return {str(t["market_id"]) for t in snap.get("tables", []) if t.get("market_id")}


def describe(snap: Dict[str, Any], oracle: Optional[Oracle] = None) -> str:
    f = snap.get("funds") or {}
    msg = (f"[{snap.get('tag')}] mode={snap.get('mode')} disp={f.get('available')} esp={f.get('exposure')} tot={f.get('total')} "
           f"bankroll={snap.get('bankroll')} picco={snap.get('peak')} dd%={snap.get('drawdown_pct')} realizzato={snap.get('realized')} "
           f"tavoli={[(t.get('market_id'), t.get('exposure'), t.get('status')) for t in snap.get('tables', [])]} "
           f"ordini={[(o.get('market_id'), o.get('side'), o.get('stake'), o.get('price'), o.get('matched'), o.get('status')) for o in snap.get('orders', [])]} "
           f"settl_exch={snap.get('exch_settlements')}")
    if oracle is not None:
        msg += (f" | ORACOLO equity={q(oracle.equity())} realizzato={q(oracle.realized())} "
                f"rischio={q(oracle.capital_at_risk())} aperti={sorted(oracle.open_markets())}")
    return msg


def check_conservation(snap: Dict[str, Any], oracle: Oracle, tol: D = CENT) -> None:
    """Invariante 1: disponibile + impegnato dell'exchange = equity attesa (al centesimo)."""
    f = snap["funds"]
    total = D(str(f.get("available"))) + D(str(f.get("exposure") or 0))
    assert abs(q(total, CENT) - q(oracle.equity(), CENT)) <= tol, "conservazione violata: " + describe(snap, oracle)


def check_realized(snap: Dict[str, Any], oracle: Oracle) -> None:
    assert q(snap["realized"]) == q(oracle.realized()), "realizzato != oracolo: " + describe(snap, oracle)


def check_no_duplicate_intents(snap: Dict[str, Any], signal_markets: Iterable[str]) -> None:
    """Invariante 3: per ogni mercato di segnale al massimo UN ordine d'ingresso vivo (hedge esclusi: lato opposto)."""
    for m in signal_markets:
        entries = [o for o in snap.get("orders", []) if str(o.get("market_id")) == str(m)
                   and o.get("status") in ("SUBMITTED", "COMPLETED", "INFLIGHT", "AMBIGUOUS", "PARTIAL")]
        sides = [str(o.get("side") or "BACK").upper() for o in entries]
        for side in set(sides):
            assert sides.count(side) <= 1, f"doppio invio {side} su {m}: " + describe(snap)


def check_tables_match_open_positions(snap: Dict[str, Any], oracle: Oracle) -> None:
    """Invariante 4: i tavoli vivi sono esattamente le posizioni non regolate."""
    assert table_markets(snap) == oracle.open_markets(), "tavoli != posizioni aperte: " + describe(snap, oracle)


def check_single_settlement_per_market(snap: Dict[str, Any]) -> None:
    """Invariante 8: il registro dei settlement dell'exchange SIM ha al massimo un record per mercato."""
    seen: Dict[str, int] = {}
    for s in snap.get("exch_settlements", []):
        seen[str(s.get("market_id"))] = seen.get(str(s.get("market_id")), 0) + 1
    dup = {m: n for m, n in seen.items() if n > 1}
    assert not dup, f"settlement duplicati {dup}: " + describe(snap)
