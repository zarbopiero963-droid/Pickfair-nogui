"""Motore di scenario: emette i record che Pickfair DEVE produrre (contratto §3/§4) e tiene la verita' dell'exchange.

Serve a provare l'oracolo (PASS sul registro completo, BLOCK togliendo un anello alla scrittura: ``skip``). Quando
BB-3/BB-4 saranno su main, gli stessi scenari gireranno sul runtime reale (f026) e l'oracolo leggera' la scatola nera
vera tramite ``core.blackbox_query``. Quote e importi SINTETICI.
"""
from __future__ import annotations

import itertools
from typing import Any, Dict, List, Optional, Set

from tests.blackbox_completeness.fake_blackbox import BlackBoxUnavailable, FakeBlackBox

_ids = itertools.count(1)


class Scenario:
    def __init__(self, bb: Optional[FakeBlackBox] = None, skip: Optional[Set[str]] = None) -> None:
        self.bb = bb or FakeBlackBox()
        self.skip = set(skip or ())
        self.exchange: List[Dict[str, Any]] = []        # storico completo degli ordini sull'exchange
        self.settlements: List[Dict[str, Any]] = []

    def _rec(self, step: str, type: str, **kw: Any) -> Dict[str, Any]:
        if step in self.skip:
            return {"event_id": None}
        return self.bb.record(type, **kw)

    # ------------------------------------------------------------------ ordine singolo
    def place(self, ref: str, market: str, side: str, price: float, size: float, *, matched: float = 0.0,
              cause: Optional[str] = None, purpose: str = "ENTRY", outcome: str = "VERIFIED",
              table_id: int = 1, sel: int = 11) -> Dict[str, Any]:
        """Richiesta durevole (critica) PRIMA dell'invio; se la scatola nera non e' disponibile nessun invio."""
        refs = dict(table_id=table_id, market_id=market, selection_id=sel, order_ref=ref)
        att = self._rec("request", "ORDER_PLACE_REQUEST", source="STRATEGY", phase="ATTEMPT", outcome="PENDING",
                        critical=True, causation_id=cause, values_after={"side": side, "price": price, "size": size},
                        data={"purpose": purpose}, **refs)
        bet = f"BET-{next(_ids)}"
        status = "CANCELLED" if purpose == "MICROSTAKE_STUB" else ("EXECUTION_COMPLETE" if matched >= size
                                                                     else "EXECUTABLE")
        self.exchange.append({"betId": bet, "customerOrderRef": ref, "marketId": market, "side": side,
                              "sizeMatched": matched, "status": status})
        if outcome == "UNKNOWN":
            self._rec("response", "ORDER_PLACE_RESPONSE", source="EXCHANGE", phase="RESULT", outcome="UNKNOWN",
                      causation_id=att["event_id"], reason_code="NO_OUTCOME", **refs)
            return {"bet": bet, "attempt": att, **refs}
        res = self._rec("response", "ORDER_PLACE_RESPONSE", source="EXCHANGE", phase="RESULT", outcome="VERIFIED",
                        causation_id=att["event_id"], bet_id=bet, **{k: v for k, v in refs.items()})
        if matched > 0:
            self._rec("matched", "ORDER_MATCHED", source="EXCHANGE", phase="OBSERVATION", outcome="VERIFIED",
                      causation_id=res["event_id"], bet_id=bet,
                      values_after={"size_matched": matched, "size_remaining": max(0.0, size - matched)}, **refs)
        return {"bet": bet, "attempt": att, "result": res, **refs}

    def cancel(self, order: Dict[str, Any]) -> None:
        refs = {k: order[k] for k in ("table_id", "market_id", "selection_id", "order_ref")}
        req = self._rec("cancel_request", "ORDER_CANCEL_REQUEST", source="SYSTEM", phase="ATTEMPT", outcome="PENDING",
                        critical=True, causation_id=(order.get("result") or {}).get("event_id"), bet_id=order["bet"],
                        **refs)
        self._rec("cancel_response", "ORDER_CANCEL_RESPONSE", source="EXCHANGE", phase="RESULT", outcome="VERIFIED",
                  causation_id=req["event_id"], bet_id=order["bet"], **refs)

    # ------------------------------------------------------------------ flussi
    def microstake_entry(self, ref: str, market: str, stake: float, price: float, table_id: int = 1) -> Dict:
        stub = self.place(ref, market, "BACK", 1000.0, 1.0, purpose="MICROSTAKE_STUB", table_id=table_id)
        self.cancel(stub)
        return self.place(ref, market, "BACK", price, stake, matched=stake, cause=stub["attempt"]["event_id"],
                          table_id=table_id)

    def protection_close(self, entry: Dict[str, Any], trigger: str, hedge_size: float, hedge_price: float) -> Dict:
        refs = {k: entry[k] for k in ("table_id", "market_id", "selection_id", "order_ref")}
        t = self._rec("trigger", trigger, source="STRATEGY", phase="OBSERVATION", outcome="VERIFIED", **refs)
        routed = self._rec("close_outcome", "SL_TP_CLOSE_ROUTED", source="STRATEGY", phase="RESULT",
                           outcome="VERIFIED", causation_id=t["event_id"], **refs)
        creq = self._rec("cashout_request", "CASHOUT_REQUEST", source="STRATEGY", phase="ATTEMPT", outcome="PENDING",
                         critical=True, causation_id=routed["event_id"], **refs)
        hedge = self.place(f"hedge:{entry['order_ref']}", entry["market_id"], "LAY", hedge_price, hedge_size,
                           matched=hedge_size, cause=creq["event_id"], purpose="HEDGE", table_id=entry["table_id"])
        self._rec("cashout_result", "CASHOUT_EXECUTED", source="EXCHANGE", phase="RESULT", outcome="VERIFIED",
                  causation_id=creq["event_id"], **refs)
        return hedge

    def settle(self, market: str, net_pnl: float) -> None:
        self.settlements.append({"market_id": market, "net_pnl": net_pnl})
        self._rec("settlement", "SETTLEMENT", source="EXCHANGE", phase="RESULT", outcome="VERIFIED",
                  market_id=market, values_after={"net_pnl": net_pnl})

    def crash_in_send(self, ref: str, market: str, size: float, matched: float) -> Dict[str, Any]:
        """Processo caduto DOPO l'invio e PRIMA dell'esito: solo la richiesta e' nel registro; dopo il riavvio la
        riconciliazione trova il bet per customerOrderRef e lo registra."""
        refs = dict(table_id=1, market_id=market, selection_id=11, order_ref=ref)
        att = self._rec("request", "ORDER_PLACE_REQUEST", source="STRATEGY", phase="ATTEMPT", outcome="PENDING",
                        critical=True, values_after={"size": size}, data={"purpose": "ENTRY"}, **refs)
        bet = f"BET-{next(_ids)}"
        self.exchange.append({"betId": bet, "customerOrderRef": ref, "marketId": market, "side": "BACK",
                              "sizeMatched": matched, "status": "EXECUTION_COMPLETE"})
        self.bb.session_id = self.bb.session_id + "-riavvio"
        self._rec("unclean", "UNCLEAN_TERMINATION", source="SYSTEM")
        run = self._rec("reconciliation", "RECONCILIATION_RUN", source="SYSTEM", phase="ATTEMPT", outcome="PENDING")
        res = self._rec("reconciliation", "RECONCILIATION_RESOLVED", source="EXCHANGE", phase="RESULT",
                        outcome="VERIFIED", causation_id=run["event_id"], bet_id=bet,
                        data={"via": "customerOrderRef"}, **refs)
        if matched > 0:
            self._rec("matched", "ORDER_MATCHED", source="EXCHANGE", phase="OBSERVATION", outcome="VERIFIED",
                      causation_id=res["event_id"], bet_id=bet, values_after={"size_matched": matched}, **refs)
        return {"bet": bet, "attempt": att, **refs}

    def try_entry_while_unavailable(self, ref: str, market: str) -> bool:
        """Ingresso con scatola nera non disponibile: la richiesta critica fallisce => NESSUN invio."""
        try:
            self.place(ref, market, "BACK", 2.0, 4.0, matched=4.0)
        except BlackBoxUnavailable:
            return False
        return True
