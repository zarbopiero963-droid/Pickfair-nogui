"""Unit test per CashoutRequestBridge (Fase 2.1-B2.4b-1, B5).

Coprono il seam cashout-only REQ_EXECUTE_CASHOUT -> CMD_EXECUTE_CASHOUT:
forward+normalizzazione, CASHOUT_FAILED strutturato sui payload invalidi,
dedup, e la garanzia che gli altri order type (QUICK_BET/DUTCHING/CANCEL/
REPLACE) NON vengano toccati dal bridge. Include la verifica del wiring
dormiente in headless_main.
"""

from collections import defaultdict

from cashout_request_bridge import (
    CASHOUT_FAILED,
    CMD_EXECUTE_CASHOUT,
    REQ_EXECUTE_CASHOUT,
    CashoutRequestBridge,
)


class _SyncBus:
    """Bus a dispatch sincrono: consegna solo ai topic effettivamente sottoscritti."""

    def __init__(self):
        self.subscriptions = []          # [(topic, handler)]
        self._handlers = defaultdict(list)
        self.published = []              # [(topic, payload)]

    def subscribe(self, topic, handler):
        self.subscriptions.append((topic, handler))
        self._handlers[topic].append(handler)

    def publish(self, topic, payload=None):
        self.published.append((topic, payload))
        for h in list(self._handlers.get(topic, [])):
            h(payload)

    def topics(self):
        return [t for t, _ in self.published]

    def last(self, topic):
        for t, p in reversed(self.published):
            if t == topic:
                return p
        return None


def _req(**over):
    payload = {
        "market_id": "1.222",
        "selection_id": 55,
        "side": "LAY",
        "stake": 4.0,
        "price": 1.8,
        "green_up": 2.5,
        "original_pos": {"side": "BACK", "price": 2.0, "stake": 3.0},
        "source": "TELEGRAM",
    }
    payload.update(over)
    return payload


# =========================================================
# WIRING — solo REQ_EXECUTE_CASHOUT
# =========================================================
def test_wire_subscribes_only_to_req_execute_cashout():
    bus = _SyncBus()
    CashoutRequestBridge(bus).wire()
    assert len(bus.subscriptions) == 1
    assert bus.subscriptions[0][0] == REQ_EXECUTE_CASHOUT


# =========================================================
# FORWARD + NORMALIZZAZIONE
# =========================================================
def test_valid_req_forwards_normalized_cmd():
    bus = _SyncBus()
    CashoutRequestBridge(bus).wire()
    bus.publish(REQ_EXECUTE_CASHOUT, _req(market_id=1.222, side="lay"))

    cmd = bus.last(CMD_EXECUTE_CASHOUT)
    assert cmd is not None
    assert cmd["market_id"] == "1.222"
    assert cmd["selection_id"] == 55 and isinstance(cmd["selection_id"], int)
    assert cmd["side"] == "LAY"
    assert cmd["stake"] == 4.0 and cmd["price"] == 1.8 and cmd["green_up"] == 2.5
    assert cmd["original_pos"] == {"side": "BACK", "price": 2.0, "stake": 3.0}
    assert cmd["source"] == "TELEGRAM"
    assert CASHOUT_FAILED not in bus.topics()


def test_metadata_passthrough_for_audit():
    bus = _SyncBus()
    CashoutRequestBridge(bus).wire()
    bus.publish(REQ_EXECUTE_CASHOUT, _req(
        event_key="ek", event_name="Team A v Team B",
        market_name="Match Odds", runner_name="Team A"))
    cmd = bus.last(CMD_EXECUTE_CASHOUT)
    assert cmd is not None
    assert cmd["event_key"] == "ek"
    assert cmd["event_name"] == "Team A v Team B"
    assert cmd["market_name"] == "Match Odds"
    assert cmd["runner_name"] == "Team A"


# =========================================================
# FAIL-CLOSED — CASHOUT_FAILED strutturato (dict), side mai defaultato
# =========================================================
def _assert_structured_reject(bus, *, market_id="", selection_id=None):
    assert CMD_EXECUTE_CASHOUT not in bus.topics()
    failed = bus.last(CASHOUT_FAILED)
    assert isinstance(failed, dict)
    assert failed["status"] == "REJECTED"
    assert failed["bet_id"] is None
    assert failed["matched"] == 0.0
    assert "reason" in failed
    assert failed["market_id"] == market_id
    assert failed["selection_id"] == selection_id


def test_missing_side_is_rejected_not_defaulted():
    # side mancante NON deve diventare LAY (defaultarlo aumenterebbe l'esposizione).
    bus = _SyncBus()
    CashoutRequestBridge(bus).wire()
    payload = _req()
    payload.pop("side")
    bus.publish(REQ_EXECUTE_CASHOUT, payload)
    _assert_structured_reject(bus, market_id="1.222", selection_id=55)
    assert bus.last(CASHOUT_FAILED)["reason"] == "side_invalido"


def test_invalid_side_is_rejected():
    bus = _SyncBus()
    CashoutRequestBridge(bus).wire()
    bus.publish(REQ_EXECUTE_CASHOUT, _req(side="GREEN"))
    _assert_structured_reject(bus, market_id="1.222", selection_id=55)


def test_non_dict_payload_rejected():
    bus = _SyncBus()
    CashoutRequestBridge(bus).wire()
    bus.publish(REQ_EXECUTE_CASHOUT, ["not", "a", "dict"])
    _assert_structured_reject(bus)


def test_missing_market_id_rejected():
    bus = _SyncBus()
    CashoutRequestBridge(bus).wire()
    bus.publish(REQ_EXECUTE_CASHOUT, _req(market_id=""))
    _assert_structured_reject(bus, market_id="", selection_id=55)


def test_non_positive_selection_id_rejected():
    bus = _SyncBus()
    CashoutRequestBridge(bus).wire()
    bus.publish(REQ_EXECUTE_CASHOUT, _req(selection_id=0))
    _assert_structured_reject(bus, market_id="1.222", selection_id=0)


def test_price_at_or_below_one_rejected():
    bus = _SyncBus()
    CashoutRequestBridge(bus).wire()
    bus.publish(REQ_EXECUTE_CASHOUT, _req(price=1.0))
    _assert_structured_reject(bus, market_id="1.222", selection_id=55)
    assert bus.last(CASHOUT_FAILED)["reason"] == "price<=1"


def test_non_positive_stake_rejected():
    bus = _SyncBus()
    CashoutRequestBridge(bus).wire()
    bus.publish(REQ_EXECUTE_CASHOUT, _req(stake=0.0))
    _assert_structured_reject(bus, market_id="1.222", selection_id=55)


def test_non_finite_price_rejected():
    bus = _SyncBus()
    CashoutRequestBridge(bus).wire()
    bus.publish(REQ_EXECUTE_CASHOUT, _req(price=float("inf")))
    _assert_structured_reject(bus, market_id="1.222", selection_id=55)


def test_missing_green_up_rejected():
    bus = _SyncBus()
    CashoutRequestBridge(bus).wire()
    payload = _req()
    payload.pop("green_up")
    bus.publish(REQ_EXECUTE_CASHOUT, payload)
    _assert_structured_reject(bus, market_id="1.222", selection_id=55)


def test_non_integral_selection_id_rejected():
    # int(55.9) -> 55 cambierebbe il runner: un float non-integrale e' reject.
    bus = _SyncBus()
    CashoutRequestBridge(bus).wire()
    bus.publish(REQ_EXECUTE_CASHOUT, _req(selection_id=55.9))
    assert CMD_EXECUTE_CASHOUT not in bus.topics()
    assert bus.last(CASHOUT_FAILED)["reason"] == "selection_id_non_intero"


def test_bool_selection_id_rejected():
    # int(True) -> 1: un bool non e' un selection_id valido.
    bus = _SyncBus()
    CashoutRequestBridge(bus).wire()
    bus.publish(REQ_EXECUTE_CASHOUT, _req(selection_id=True))
    assert CMD_EXECUTE_CASHOUT not in bus.topics()
    assert bus.last(CASHOUT_FAILED)["reason"] == "selection_id_non_intero"


def test_integral_float_selection_id_accepted():
    # 55.0 e' un intero esatto: ammesso e normalizzato a int.
    bus = _SyncBus()
    CashoutRequestBridge(bus).wire()
    bus.publish(REQ_EXECUTE_CASHOUT, _req(selection_id=55.0))
    cmd = bus.last(CMD_EXECUTE_CASHOUT)
    assert cmd is not None
    assert cmd["selection_id"] == 55 and isinstance(cmd["selection_id"], int)


def test_none_market_id_rejected_before_stringify():
    # str(None) -> 'None' supererebbe il check vuoto: None e' reject.
    bus = _SyncBus()
    CashoutRequestBridge(bus).wire()
    bus.publish(REQ_EXECUTE_CASHOUT, _req(market_id=None))
    assert CMD_EXECUTE_CASHOUT not in bus.topics()
    failed = bus.last(CASHOUT_FAILED)
    assert failed["reason"] == "market_id_mancante"
    assert failed["market_id"] == ""  # None non e' echo-ato come 'None'


def test_bool_market_id_rejected():
    bus = _SyncBus()
    CashoutRequestBridge(bus).wire()
    bus.publish(REQ_EXECUTE_CASHOUT, _req(market_id=True))
    assert CMD_EXECUTE_CASHOUT not in bus.topics()
    assert bus.last(CASHOUT_FAILED)["reason"] == "market_id_mancante"


def test_non_finite_float_market_id_rejected():
    # str(float('nan'))='nan' / str(float('inf'))='inf' supererebbero il check
    # vuoto e finirebbero sotto un market id sintetico: reject prima dello stringify.
    for bad in (float("nan"), float("inf")):
        bus = _SyncBus()
        CashoutRequestBridge(bus).wire()
        bus.publish(REQ_EXECUTE_CASHOUT, _req(market_id=bad))
        assert CMD_EXECUTE_CASHOUT not in bus.topics()
        assert bus.last(CASHOUT_FAILED)["reason"] == "market_id_non_finito"


def test_finite_float_market_id_accepted():
    # Un market id numerico finito resta ammesso (stringify deterministico).
    bus = _SyncBus()
    CashoutRequestBridge(bus).wire()
    bus.publish(REQ_EXECUTE_CASHOUT, _req(market_id=1.222))
    cmd = bus.last(CMD_EXECUTE_CASHOUT)
    assert cmd is not None
    assert cmd["market_id"] == "1.222"


# =========================================================
# DEDUP
# =========================================================
def test_duplicate_within_window_is_dropped():
    bus = _SyncBus()
    CashoutRequestBridge(bus, duplicate_window_sec=60.0).wire()
    bus.publish(REQ_EXECUTE_CASHOUT, _req())
    bus.publish(REQ_EXECUTE_CASHOUT, _req())
    cmds = [t for t in bus.topics() if t == CMD_EXECUTE_CASHOUT]
    assert len(cmds) == 1


def test_distinct_positions_not_deduped():
    bus = _SyncBus()
    CashoutRequestBridge(bus, duplicate_window_sec=60.0).wire()
    bus.publish(REQ_EXECUTE_CASHOUT, _req(selection_id=55))
    bus.publish(REQ_EXECUTE_CASHOUT, _req(selection_id=66))
    cmds = [t for t in bus.topics() if t == CMD_EXECUTE_CASHOUT]
    assert len(cmds) == 2


def test_same_position_different_metadata_is_deduped():
    # Dedup su identita' posizione (market_id, selection_id, side): metadata e
    # source diversi NON devono generare un secondo hedge per la stessa posizione.
    bus = _SyncBus()
    CashoutRequestBridge(bus, duplicate_window_sec=60.0).wire()
    bus.publish(REQ_EXECUTE_CASHOUT, _req(source="TELEGRAM", event_name="A v B"))
    bus.publish(REQ_EXECUTE_CASHOUT, _req(source="COPY", event_name="Other"))
    cmds = [t for t in bus.topics() if t == CMD_EXECUTE_CASHOUT]
    assert len(cmds) == 1


def test_same_position_price_drift_is_deduped():
    # Anche un price-drift tra due emissioni della stessa posizione e' un doppio
    # hedge da sopprimere: la chiave non include il prezzo.
    bus = _SyncBus()
    CashoutRequestBridge(bus, duplicate_window_sec=60.0).wire()
    bus.publish(REQ_EXECUTE_CASHOUT, _req(price=1.8))
    bus.publish(REQ_EXECUTE_CASHOUT, _req(price=1.9))
    cmds = [t for t in bus.topics() if t == CMD_EXECUTE_CASHOUT]
    assert len(cmds) == 1


def test_opposite_side_same_selection_not_deduped():
    # Stesso market/selection ma side opposto = posizioni distinte (BACK vs LAY).
    bus = _SyncBus()
    CashoutRequestBridge(bus, duplicate_window_sec=60.0).wire()
    bus.publish(REQ_EXECUTE_CASHOUT, _req(side="LAY"))
    bus.publish(REQ_EXECUTE_CASHOUT, _req(side="BACK"))
    cmds = [t for t in bus.topics() if t == CMD_EXECUTE_CASHOUT]
    assert len(cmds) == 2


# =========================================================
# REQUISITO CHIAVE — gli altri order type NON sono toccati
# =========================================================
def test_other_order_types_are_not_handled():
    bus = _SyncBus()
    bridge = CashoutRequestBridge(bus)
    bridge.wire()

    for topic, payload in [
        ("REQ_QUICK_BET", {"market_id": "1.1", "selection_id": 1}),
        ("REQ_PLACE_DUTCHING", {"market_id": "1.1", "results": []}),
        ("REQ_CANCEL_ORDER", {"market_id": "1.1", "bet_id": "B1"}),
        ("REQ_REPLACE_ORDER", {"market_id": "1.1", "bet_id": "B1", "new_price": 2.0}),
    ]:
        bus.publish(topic, payload)

    produced = set(bus.topics())
    # Il bridge non deve aver prodotto alcun CMD/azione su questi flussi.
    assert CMD_EXECUTE_CASHOUT not in produced
    assert "CMD_QUICK_BET" not in produced
    assert "CMD_PLACE_DUTCHING" not in produced
    assert "CMD_CANCEL_ORDER" not in produced
    assert "CMD_REPLACE_ORDER" not in produced
    # Nessun CASHOUT_FAILED: il bridge non è nemmeno stato invocato su quei topic.
    assert CASHOUT_FAILED not in produced
    # E resta sottoscritto solo a REQ_EXECUTE_CASHOUT.
    assert [t for t, _ in bridge.bus.subscriptions] == [REQ_EXECUTE_CASHOUT]


# =========================================================
# WIRING DORMIENTE in headless_main
# =========================================================
class _FakeClient:
    def place_bet(self, **kwargs):
        return {"ok": True, "result": {"status": "SUCCESS", "instructionReports": []}}


class _FakeBetfairService:
    def get_client(self):
        return _FakeClient()


def test_headless_wires_dormant_cashout_chain():
    import headless_main

    app = headless_main.HeadlessApp()
    app.bus = _SyncBus()
    app.betfair_service = _FakeBetfairService()

    app._wire_cashout_execution_chain()

    # Catena costruita...
    assert app.order_router is not None
    assert app.cashout_executor is not None
    assert app.cashout_request_bridge is not None

    # ...e sottoscritta: bridge su REQ_EXECUTE_CASHOUT, executor su CMD_EXECUTE_CASHOUT.
    subs = dict(app.bus.subscriptions)
    assert REQ_EXECUTE_CASHOUT in subs
    assert CMD_EXECUTE_CASHOUT in subs

    # Dormiente: nessun REQ_EXECUTE_CASHOUT è stato emesso dal wiring.
    assert REQ_EXECUTE_CASHOUT not in app.bus.topics()
    assert CMD_EXECUTE_CASHOUT not in app.bus.topics()
