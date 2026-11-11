"""Gate A — T2b batch b1, componente cashout_residual_handler (CashoutResidualHandler reale)."""
from __future__ import annotations

import logging
from typing import Any, Dict, List

from cashout_residual_handler import (
    AUDIT_TYPE_RECONCILIATION,
    AUDIT_TYPE_RESIDUAL,
    CashoutResidualHandler,
)
from core.trading_constants import CASHOUT_FAILED, CASHOUT_SUCCESS


class _Bus:
    def __init__(self) -> None:
        self.subs: Dict[str, List[Any]] = {}
        self.events: List[tuple] = []

    def subscribe(self, topic, cb):
        self.subs.setdefault(topic, []).append(cb)

    def publish(self, topic, payload=None):
        self.events.append((topic, payload))
        for cb in list(self.subs.get(topic, [])):
            cb(payload)


class _Rec:
    def __init__(self, cancel_result: Any = True) -> None:
        self.cancels: List[tuple] = []
        self.records: List[Dict[str, Any]] = []
        self.notes: List[tuple] = []
        self.cancel_result = cancel_result

    def cancel(self, market_id, bet_ids):
        self.cancels.append((market_id, list(bet_ids)))
        if isinstance(self.cancel_result, Exception):
            raise self.cancel_result
        return self.cancel_result

    def persist(self, record):
        self.records.append(dict(record))

    def notify(self, text, *, severity):
        self.notes.append((text, severity))

    def handler(self) -> CashoutResidualHandler:
        return CashoutResidualHandler(cancel=self.cancel, persist=self.persist, notify=self.notify)


def test_trg_628_b_1026():
    """ID: TRG-628-B-1026 / PF-B1-R2-RESIDUAL-WIRE

    Il testo della foglia dice «solo CASHOUT_FAILED»; il codice ratificato (#522, docs/cashout_overview.md §7.1)
    sottoscrive CASHOUT_FAILED e CASHOUT_SUCCESS, quest'ultimo solo per il residuo di un match parziale. Si fissa
    l'insieme esatto dei topic e che un SUCCESS completo non produca alcun cancel.
    """
    rec, bus = _Rec(), _Bus()
    h = rec.handler()
    h.wire(bus)
    assert set(bus.subs) == {CASHOUT_FAILED, CASHOUT_SUCCESS}
    assert all(len(v) == 1 for v in bus.subs.values())
    bus.publish(CASHOUT_SUCCESS, {"partial": False, "bet_id": "H1", "market_id": "1.1"})
    bus.publish(CASHOUT_SUCCESS, "non-dict")
    assert rec.cancels == [] and rec.records == [] and rec.notes == []


def test_trg_628_b_1027():
    """ID: TRG-628-B-1027 / PF-B1-R2-RESIDUAL-UNMATCHED"""
    # cancel confermato
    rec = _Rec(cancel_result=True)
    rec.handler().on_cashout_failed({"status": "UNMATCHED", "bet_id": "H1", "market_id": "1.1", "selection_id": 10})
    assert rec.cancels == [("1.1", ["H1"])]
    r = rec.records[0]
    assert (r["type"], r["status"], r["cancel_attempted"], r["cancel_ok"], r["cancel_error"]) == \
        (AUDIT_TYPE_RESIDUAL, "UNMATCHED", True, True, "")
    assert len(rec.notes) == 1 and rec.notes[0][1] == "HIGH"
    # cancel che solleva: un solo tentativo, errore registrato, nessun retry
    rec2 = _Rec(cancel_result=TimeoutError("net"))
    rec2.handler().on_cashout_failed({"status": "UNMATCHED", "bet_id": "H2", "market_id": "1.1"})
    assert rec2.cancels == [("1.1", ["H2"])]
    r2 = rec2.records[0]
    assert (r2["cancel_attempted"], r2["cancel_ok"], r2["cancel_error"]) == (True, False, "net")
    assert rec2.notes[0][1] == "HIGH"
    # identita' mancante: nessun cancel, comunque persistito e notificato
    rec3 = _Rec()
    rec3.handler().on_cashout_failed({"status": "UNMATCHED", "bet_id": None, "market_id": "1.1"})
    assert rec3.cancels == []
    assert rec3.records[0]["cancel_attempted"] is False and rec3.notes[0][1] == "HIGH"


def test_trg_628_b_1028():
    """ID: TRG-628-B-1028 / PF-B1-R2-RESIDUAL-AMBIG"""
    rec = _Rec()
    rec.handler().on_cashout_failed({"status": "ambiguous", "bet_id": "H1", "market_id": "1.1",
                                     "reason": "order_unknown"})
    assert rec.cancels == []
    assert len(rec.records) == 1
    r = rec.records[0]
    assert (r["type"], r["status"], r["cancel_attempted"]) == (AUDIT_TYPE_RECONCILIATION, "AMBIGUOUS", False)
    assert [s for _, s in rec.notes] == ["CRITICAL"]


def test_trg_628_b_1029():
    """ID: TRG-628-B-1029 / PF-B1-R2-RESIDUAL-LEGACY"""
    rec = _Rec()
    rec.handler().on_cashout_failed("risk_middleware: limite superato")
    assert rec.cancels == []
    assert rec.records == [{"type": AUDIT_TYPE_RESIDUAL, "status": "UNKNOWN",
                            "reason": "risk_middleware: limite superato"}]
    assert [s for _, s in rec.notes] == ["HIGH"]


def test_trg_628_b_1030(caplog):
    """ID: TRG-628-B-1030 / PF-B1-R2-RESIDUAL-PERSIST-FAIL"""
    rec = _Rec()

    def broken_persist(record):
        raise OSError("disk full")

    h = CashoutResidualHandler(cancel=rec.cancel, persist=broken_persist, notify=rec.notify)
    bus = _Bus()
    h.wire(bus)
    with caplog.at_level(logging.ERROR, logger="cashout_residual_handler"):
        for payload in ({"status": "UNMATCHED", "bet_id": "H1", "market_id": "1.1"},
                        {"status": "AMBIGUOUS", "bet_id": "H2", "market_id": "1.1"},
                        "legacy"):
            bus.publish(CASHOUT_FAILED, payload)   # non deve propagare
    assert sum("persist residuo fallita" in r.getMessage() for r in caplog.records) == 3
    assert len(rec.notes) == 3                     # la notifica parte comunque
