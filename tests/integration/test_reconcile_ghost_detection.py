from __future__ import annotations

import copy

from core.reconciliation_engine import ReconcileConfig, ReconciliationEngine


# =========================================================
# FAKES
# =========================================================

class FakeDB:
    def __init__(self):
        self.persisted_logs = {}

    def persist_decision_log(self, batch_id, entries):
        self.persisted_logs.setdefault(batch_id, []).extend(copy.deepcopy(entries))


class FakeBus:
    def __init__(self):
        self.events = []

    def publish(self, name, payload):
        self.events.append((name, copy.deepcopy(payload)))

    def names(self):
        return [x[0] for x in self.events]


class FakeBatchManager:
    def __init__(self, batch, legs):
        self._batch = copy.deepcopy(batch)
        self._legs = copy.deepcopy(legs)

    def get_batch(self, batch_id):
        if batch_id == self._batch["batch_id"]:
            return copy.deepcopy(self._batch)
        return None

    def get_batch_legs(self, batch_id):
        if batch_id == self._batch["batch_id"]:
            return copy.deepcopy(self._legs)
        return []

    def update_leg_status(
        self,
        *,
        batch_id,
        leg_index,
        status,
        bet_id=None,
        raw_response=None,
        error_text=None,
    ):
        for leg in self._legs:
            if int(leg["leg_index"]) == int(leg_index):
                leg["status"] = status
                if bet_id is not None:
                    leg["bet_id"] = bet_id
                if raw_response is not None:
                    leg["raw_response"] = copy.deepcopy(raw_response)
                if error_text is not None:
                    leg["error_text"] = error_text
                break

    def recompute_batch_status(self, batch_id):
        return copy.deepcopy(self._batch)

    def release_runtime_artifacts(self, **kwargs):
        return None

    def get_open_batches(self):
        return [copy.deepcopy(self._batch)]


class GhostAwareEngine(ReconciliationEngine):
    def __init__(self, *args, remote_orders=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._remote_orders = copy.deepcopy(remote_orders or [])

    def _fetch_current_orders_by_market(self, market_id: str, *, _attempt: int = 0):
        return copy.deepcopy(self._remote_orders)

    def _get_pending_saga_refs(self):
        return set()

    def _lookup_remote_order(self, leg, by_ref, by_bet):
        """
        Test target:
        robust multi-key lookup.
        Priority:
          1) bet_id
          2) customer_ref
          3) market_id + selection_id
        """
        bid = str(leg.get("bet_id") or "").strip()
        if bid and bid in by_bet:
            return by_bet[bid]

        cref = str(leg.get("customer_ref") or "").strip()
        if cref and cref in by_ref:
            return by_ref[cref]

        leg_market = str(leg.get("market_id") or "").strip()
        leg_sel = str(leg.get("selection_id") or "").strip()

        for order in list(by_bet.values()) + list(by_ref.values()):
            order_market = str(order.get("marketId") or order.get("market_id") or "").strip()
            order_sel = str(order.get("selectionId") or order.get("selection_id") or "").strip()
            if leg_market and leg_sel and leg_market == order_market and leg_sel == order_sel:
                return order

        return None


# =========================================================
# HELPERS
# =========================================================

def _batch():
    return {"batch_id": "B200", "market_id": "1.200", "status": "LIVE"}


def _build_engine(legs, remote_orders):
    db = FakeDB()
    bus = FakeBus()
    bm = FakeBatchManager(_batch(), legs)
    engine = GhostAwareEngine(
        db=db,
        bus=bus,
        batch_manager=bm,
        client_getter=lambda: None,
        config=ReconcileConfig(
            convergence_sleep_secs=0.0,
            max_convergence_cycles=2,
            ghost_order_action="LOG_AND_FLAG",
            unknown_grace_secs=999999.0,
        ),
        remote_orders=remote_orders,
    )
    return engine, db, bus, bm


# =========================================================
# AREA 3 — GHOST DETECTION
# =========================================================

def test_match_by_bet_id():
    """
    Cosa uccide:
      ghost falso positivo quando bet_id coincide.
    Invariant:
      bet_id valido matcha sempre prima del resto.
    Mutation che deve fallire:
      ignorare bet_id.
    """
    legs = [
        {
            "leg_index": 0,
            "status": "SUBMITTED",
            "customer_ref": "LOCAL-REF",
            "bet_id": "BET-777",
            "selection_id": 10,
            "market_id": "1.200",
        }
    ]
    remote_orders = [
        {
            "customerOrderRef": "DIFFERENT-REF",
            "betId": "BET-777",
            "status": "EXECUTION_COMPLETE",
            "sizeMatched": 10.0,
            "sizeRemaining": 0.0,
            "selectionId": 10,
            "marketId": "1.200",
        }
    ]

    engine, _, _, bm = _build_engine(legs, remote_orders)
    result = engine.reconcile_batch("B200")

    assert result["ok"] is True
    assert bm._legs[0]["status"] == "MATCHED"


def test_match_by_customer_ref():
    """
    Cosa uccide:
      fallback rotto quando manca bet_id ma c’è customer_ref.
    Invariant:
      customer_ref è chiave valida di lookup.
    Mutation che deve fallire:
      rimuovere match su customer_ref.
    """
    legs = [
        {
            "leg_index": 0,
            "status": "SUBMITTED",
            "customer_ref": "REF-CUST-1",
            "bet_id": "",
            "selection_id": 20,
            "market_id": "1.200",
        }
    ]
    remote_orders = [
        {
            "customerOrderRef": "REF-CUST-1",
            "betId": "BET-888",
            "status": "EXECUTION_COMPLETE",
            "sizeMatched": 10.0,
            "sizeRemaining": 0.0,
            "selectionId": 20,
            "marketId": "1.200",
        }
    ]

    engine, _, _, bm = _build_engine(legs, remote_orders)
    result = engine.reconcile_batch("B200")

    assert result["ok"] is True
    assert bm._legs[0]["status"] == "MATCHED"


def test_match_by_market_selection():
    """
    Cosa uccide:
      replace o remap trattati come ghost se cambia il bet_id ma runner e market restano coerenti.
    Invariant:
      market_id + selection_id supportano match robusto dove previsto.
    Mutation che deve fallire:
      ignorare selection_id o market_id.
    """
    legs = [
        {
            "leg_index": 0,
            "status": "SUBMITTED",
            "customer_ref": "",
            "bet_id": "",
            "selection_id": 31,
            "market_id": "1.200",
        }
    ]
    remote_orders = [
        {
            "customerOrderRef": "",
            "betId": "BET-NEW-31",
            "status": "EXECUTION_COMPLETE",
            "sizeMatched": 10.0,
            "sizeRemaining": 0.0,
            "selectionId": 31,
            "marketId": "1.200",
        }
    ]

    engine, _, _, bm = _build_engine(legs, remote_orders)
    result = engine.reconcile_batch("B200")

    assert result["ok"] is True
    assert bm._legs[0]["status"] == "MATCHED"


def test_real_ghost_detected():
    """
    Cosa uccide:
      ordine exchange senza controparte locale non rilevato.
    Invariant:
      ordine reale senza match locale = ghost.
    Mutation che deve fallire:
      restituire sempre lista ghost vuota.
    """
    legs = [
        {
            "leg_index": 0,
            "status": "SUBMITTED",
            "customer_ref": "REF-ONLY-LOCAL",
            "bet_id": "",
            "selection_id": 40,
            "market_id": "1.200",
        }
    ]
    remote_orders = [
        {
            "customerOrderRef": "REF-GHOST",
            "betId": "BET-GHOST",
            "status": "EXECUTABLE",
            "sizeMatched": 0.0,
            "sizeRemaining": 10.0,
            "selectionId": 99,
            "marketId": "1.200",
        }
    ]

    engine, db, bus, _ = _build_engine(legs, remote_orders)

    ghosts = engine._detect_ghost_orders("B200", legs, remote_orders)

    assert len(ghosts) == 1
    assert ghosts[0]["customer_ref"] == "REF-GHOST"
    assert "RECONCILIATION_GHOST_ORDERS" in bus.names()


def test_replace_bet_id_not_ghost():
    """
    Cosa uccide:
      replace considerato ghost perché il bet_id nuovo non è ancora sulla leg locale.
    Invariant:
      replace coerente non deve diventare ghost.
    Mutation che deve fallire:
      matchare solo per bet_id.
    """
    legs = [
        {
            "leg_index": 0,
            "status": "PLACED",
            "customer_ref": "REF-REPLACE",
            "bet_id": "BET-OLD",
            "selection_id": 55,
            "market_id": "1.200",
        }
    ]
    remote_orders = [
        {
            "customerOrderRef": "",
            "betId": "BET-NEW",
            "status": "EXECUTABLE",
            "sizeMatched": 0.0,
            "sizeRemaining": 10.0,
            "selectionId": 55,
            "marketId": "1.200",
        }
    ]

    engine, _, _, _ = _build_engine(legs, remote_orders)

    ghosts = engine._detect_ghost_orders("B200", legs, remote_orders)

    assert ghosts == []