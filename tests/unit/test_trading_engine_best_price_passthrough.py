"""Propagazione provenienza best-price nei record/audit del TradingEngine.

Attivazione LIVE best-price PR-2 (Greptile P2): ``best_price_source`` e
``best_price_reason`` aggiunti al payload da `RuntimeController` devono
sopravvivere alla normalizzazione (`_PASSTHROUGH_KEYS`) e comparire nei record
di esecuzione/audit, non solo nei log.
"""
import pytest

pytestmark = pytest.mark.unit


class FakeBus:
    def __init__(self):
        self.subscriptions = {}
        self.events = []

    def subscribe(self, topic, handler):
        self.subscriptions[topic] = handler

    def publish(self, name, payload):
        self.events.append((name, payload))


class FakeDB:
    def __init__(self):
        self.orders = {}
        self.audit_events = []
        self.seq = 0

    @staticmethod
    def is_ready():
        return True

    def insert_order(self, payload):
        self.seq += 1
        order_id = f"ORD-{self.seq}"
        self.orders[order_id] = dict(payload)
        return order_id

    def update_order(self, order_id, update):
        self.orders.setdefault(order_id, {})
        self.orders[order_id].update(update)

    def insert_audit_event(self, event):
        self.audit_events.append(event)

    @staticmethod
    def order_exists_inflight(*, customer_ref, correlation_id):
        return False

    @staticmethod
    def load_pending_customer_refs():
        return []

    @staticmethod
    def load_pending_correlation_ids():
        return []


class InlineExecutor:
    @staticmethod
    def is_ready():
        return True

    @staticmethod
    def submit(_name, fn):
        return fn()


class _ExplodingOrderManager:
    @staticmethod
    def submit(payload):
        raise RuntimeError("BROKER_SUBMIT_FAILED")


def _make_engine():
    from core.trading_engine import TradingEngine

    return TradingEngine(
        bus=FakeBus(),
        db=FakeDB(),
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )


def _payload(**over):
    base = {
        "market_id": "1.110",
        "selection_id": 11,
        "price": 2.52,
        "stake": 5.0,
        "side": "BACK",
        "customer_ref": "BP-X1",
        "correlation_id": "CID-BP-X1",
        "simulation_mode": True,
        "event_key": "1.110:11:BACK",
        "best_price_source": "LIVE_BOOK_DIRECT",
        "best_price_reason": "ok",
    }
    base.update(over)
    return base


def test_passthrough_keys_include_best_price():
    from core.trading_constants import _PASSTHROUGH_KEYS

    assert "best_price_source" in _PASSTHROUGH_KEYS
    assert "best_price_reason" in _PASSTHROUGH_KEYS


def test_copy_best_price_meta_copies_only_when_present():
    from core.trading_engine import TradingEngine

    target = {}
    TradingEngine._copy_best_price_meta(target, {"best_price_source": "LIVE_BOOK_DIRECT"})
    assert target == {"best_price_source": "LIVE_BOOK_DIRECT"}
    # source None/vuoto => no-op
    TradingEngine._copy_best_price_meta(target, None)
    TradingEngine._copy_best_price_meta(target, {})
    assert target == {"best_price_source": "LIVE_BOOK_DIRECT"}


def test_best_price_provenance_in_result_contract():
    # result.update(extra_fields): la provenienza best-price compare nel result.
    engine = _make_engine()
    engine.order_manager = _ExplodingOrderManager()
    result = engine.submit_quick_bet(_payload())
    assert result["ok"] is False
    assert result["best_price_source"] == "LIVE_BOOK_DIRECT"
    assert result["best_price_reason"] == "ok"


def test_best_price_provenance_in_audit_records():
    # La provenienza deve entrare nei record/audit, non solo nel log (Greptile P2).
    engine = _make_engine()
    engine.order_manager = _ExplodingOrderManager()
    engine.submit_quick_bet(_payload(best_price_source="FALLBACK_MASTER",
                                     best_price_reason="out_of_tolerance"))
    db = engine.db
    blob = repr(db.audit_events) + repr(db.orders)
    assert "best_price_source" in blob
    assert "FALLBACK_MASTER" in blob


def test_best_price_provenance_preserved_on_normalization_failure():
    # Percorso di normalizzazione fallita (copy_meta non-dict): la provenienza
    # best-price del raw deve sopravvivere comunque (CodeRabbit P-Major).
    engine = _make_engine()
    engine.order_manager = _ExplodingOrderManager()
    result = engine.submit_quick_bet(_payload(copy_meta="NON_DICT"))
    assert result["ok"] is False
    assert result["best_price_source"] == "LIVE_BOOK_DIRECT"
    assert result["best_price_reason"] == "ok"


def test_no_best_price_keys_when_absent():
    # Flag OFF / percorso normale: le chiavi best_price NON sono nel payload e i
    # record non le inventano. Il caso reale "assente" e' l'omissione delle chiavi.
    payload = _payload()
    payload.pop("best_price_source")
    payload.pop("best_price_reason")
    engine = _make_engine()
    engine.order_manager = _ExplodingOrderManager()
    result = engine.submit_quick_bet(payload)
    assert "best_price_source" not in result
    assert "best_price_reason" not in result
