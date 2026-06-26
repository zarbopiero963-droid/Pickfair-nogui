"""Unit test per CashoutResidualHandler (Fase 2.1-B2.4b-2, B5)."""

from collections import defaultdict

from cashout_residual_handler import (
    AUDIT_TYPE_RECONCILIATION,
    AUDIT_TYPE_RESIDUAL,
    CashoutResidualHandler,
)
from core.trading_constants import CASHOUT_FAILED


class _SyncBus:
    def __init__(self):
        self.subscriptions = []
        self._handlers = defaultdict(list)

    def subscribe(self, topic, handler):
        self.subscriptions.append((topic, handler))
        self._handlers[topic].append(handler)

    def publish(self, topic, payload=None):
        for h in list(self._handlers.get(topic, [])):
            h(payload)


class _Recorder:
    def __init__(self, cancel_result=True, cancel_raises=None):
        self.cancel_calls = []
        self.persisted = []
        self.notifications = []
        self._cancel_result = cancel_result
        self._cancel_raises = cancel_raises

    def cancel(self, market_id, bet_ids):
        self.cancel_calls.append((market_id, list(bet_ids)))
        if self._cancel_raises is not None:
            raise self._cancel_raises
        return self._cancel_result

    def persist(self, record):
        self.persisted.append(record)

    def notify(self, text, *, severity):
        self.notifications.append((severity, text))


def _make(cancel_result=True, cancel_raises=None):
    rec = _Recorder(cancel_result=cancel_result, cancel_raises=cancel_raises)
    handler = CashoutResidualHandler(cancel=rec.cancel, persist=rec.persist, notify=rec.notify)
    return handler, rec


def _failed(**over):
    payload = {
        "reason": "unmatched",
        "status": "UNMATCHED",
        "bet_id": "B1",
        "matched": 0.0,
        "market_id": "1.222",
        "selection_id": 55,
    }
    payload.update(over)
    return payload


# =========================================================
# WIRING
# =========================================================
def test_wire_subscribes_only_to_cashout_failed():
    bus = _SyncBus()
    handler, _ = _make()
    handler.wire(bus)
    assert [t for t, _ in bus.subscriptions] == [CASHOUT_FAILED]


# =========================================================
# 1. UNMATCHED con bet_id => un solo cancel
# =========================================================
def test_unmatched_with_bet_id_cancels_once():
    handler, rec = _make()
    handler.on_cashout_failed(_failed(status="UNMATCHED", bet_id="B1", market_id="1.222"))
    assert rec.cancel_calls == [("1.222", ["B1"])]
    record = rec.persisted[-1]
    assert record["type"] == AUDIT_TYPE_RESIDUAL
    assert record["status"] == "UNMATCHED"
    assert record["cancel_attempted"] is True
    assert record["cancel_ok"] is True
    assert record["cancel_error"] == ""
    assert record["bet_id"] == "B1" and record["market_id"] == "1.222"
    assert rec.notifications and rec.notifications[-1][0] == "HIGH"


# =========================================================
# 2. UNMATCHED senza bet_id => nessun cancel, ma persist + notify
# =========================================================
def test_unmatched_without_bet_id_does_not_cancel():
    handler, rec = _make()
    handler.on_cashout_failed(_failed(status="UNMATCHED", bet_id="", market_id="1.222"))
    assert rec.cancel_calls == []
    record = rec.persisted[-1]
    assert record["cancel_attempted"] is False
    assert record["cancel_ok"] is False
    assert rec.notifications  # operatore comunque avvisato


def test_unmatched_without_market_id_does_not_cancel():
    handler, rec = _make()
    handler.on_cashout_failed(_failed(status="UNMATCHED", bet_id="B1", market_id=""))
    assert rec.cancel_calls == []
    assert rec.persisted[-1]["cancel_attempted"] is False


# =========================================================
# 3. AMBIGUOUS => mai cancel, reconciliation + notify alta severità
# =========================================================
def test_ambiguous_never_cancels():
    handler, rec = _make()
    handler.on_cashout_failed(_failed(status="AMBIGUOUS", bet_id="B1", market_id="1.222"))
    assert rec.cancel_calls == []
    record = rec.persisted[-1]
    assert record["type"] == AUDIT_TYPE_RECONCILIATION
    assert record["status"] == "AMBIGUOUS"
    assert record["cancel_attempted"] is False
    assert rec.notifications[-1][0] == "CRITICAL"


# =========================================================
# 4. Cancel failure non crasha, viene persistita/notificata
# =========================================================
def test_cancel_exception_is_recorded_not_raised():
    handler, rec = _make(cancel_raises=RuntimeError("broker down"))
    handler.on_cashout_failed(_failed(status="UNMATCHED", bet_id="B1", market_id="1.222"))
    record = rec.persisted[-1]
    assert record["cancel_attempted"] is True
    assert record["cancel_ok"] is False
    assert "broker down" in record["cancel_error"]
    assert rec.notifications  # notificato


def test_cancel_negative_result_recorded():
    handler, rec = _make(cancel_result=False)
    handler.on_cashout_failed(_failed(status="UNMATCHED"))
    record = rec.persisted[-1]
    assert record["cancel_attempted"] is True
    assert record["cancel_ok"] is False
    assert record["cancel_error"] == ""


# =========================================================
# 5. Nessun retry automatico (un solo cancel anche se fallisce)
# =========================================================
def test_no_automatic_retry_on_cancel_failure():
    handler, rec = _make(cancel_result=False)
    handler.on_cashout_failed(_failed(status="UNMATCHED", bet_id="B1", market_id="1.222"))
    assert len(rec.cancel_calls) == 1  # un solo tentativo, mai retry


# =========================================================
# 6. Nessun impatto su altri order type (handler sottoscrive solo CASHOUT_FAILED)
# =========================================================
def test_other_order_type_events_not_handled():
    bus = _SyncBus()
    handler, rec = _make()
    handler.wire(bus)
    for topic in ("QUICK_BET_FAILED", "DUTCHING_FAILED", "ORDER_CANCEL_FAILED",
                  "ORDER_REPLACE_FAILED", "CMD_EXECUTE_CASHOUT", "CASHOUT_SUCCESS"):
        bus.publish(topic, {"status": "UNMATCHED", "bet_id": "B1", "market_id": "1.1"})
    assert rec.cancel_calls == []
    assert rec.persisted == []
    assert rec.notifications == []


# =========================================================
# ERROR / REJECTED / FAILURE + legacy
# =========================================================
def test_error_status_notifies_and_persists_with_context():
    handler, rec = _make()
    handler.on_cashout_failed(_failed(status="ERROR", reason="place_exception:boom"))
    assert rec.cancel_calls == []
    assert rec.persisted[-1]["status"] == "ERROR"
    assert rec.notifications[-1][0] == "HIGH"


def test_rejected_without_context_notifies_without_persist():
    handler, rec = _make()
    handler.on_cashout_failed({"status": "REJECTED", "reason": "", "bet_id": "", "market_id": ""})
    assert rec.cancel_calls == []
    assert rec.persisted == []  # niente contesto utile => nessun record diagnostico
    assert rec.notifications  # ma notificato


def test_non_dict_legacy_payload_is_diagnosed_not_cancelled():
    handler, rec = _make()
    handler.on_cashout_failed("Payload CASHOUT invalido: boom")
    assert rec.cancel_calls == []
    assert rec.persisted[-1]["status"] == "UNKNOWN"
    assert rec.notifications[-1][0] == "HIGH"


# =========================================================
# Robustezza I/O: persist/notify che sollevano non crashano l'handler
# =========================================================
def test_persist_and_notify_failures_do_not_crash():
    def boom_persist(_):
        raise RuntimeError("db down")

    def boom_notify(_text, *, severity):
        raise RuntimeError("tg down")

    handler = CashoutResidualHandler(cancel=lambda m, b: True, persist=boom_persist, notify=boom_notify)
    # Non deve sollevare.
    handler.on_cashout_failed(_failed(status="UNMATCHED"))
