"""Unit test per CashoutExecutor (Fase 2.1-A, B5)."""

from cashout_executor import (
    CASHOUT_FAILED,
    CASHOUT_SUCCESS,
    CMD_EXECUTE_CASHOUT,
    CashoutExecutor,
)


class _FakeBus:
    def __init__(self):
        self.published = []
        self.subscriptions = []

    def subscribe(self, topic, handler):
        self.subscriptions.append((topic, handler))

    def publish(self, topic, payload):
        self.published.append((topic, payload))

    def last(self, topic):
        for t, p in reversed(self.published):
            if t == topic:
                return p
        return None

    def topics(self):
        return [t for t, _ in self.published]


class _FakeRouter:
    def __init__(self, result=None, raises=None):
        self.calls = []
        self._result = result
        self._raises = raises

    def place(self, payload):
        self.calls.append(payload)
        if self._raises is not None:
            raise self._raises
        return self._result


def _cmd_payload(**over):
    payload = {
        "market_id": "1.222",
        "selection_id": 55,
        "side": "LAY",
        "stake": 4.0,
        "price": 1.8,
        "green_up": 2.5,
        "original_pos": {"side": "BACK", "price": 2.0, "stake": 3.0},
        "source": "UI",
    }
    payload.update(over)
    return payload


def _ok_result(matched=4.0, bet_id="CB1"):
    return {"ok": matched > 0, "placed": True, "order_unknown": False,
            "status": "SUCCESS", "matched": matched, "bet_id": bet_id, "error": None}


def test_wire_subscribes_to_cmd_execute_cashout():
    bus = _FakeBus()
    CashoutExecutor(bus, _FakeRouter()).wire()
    assert bus.subscriptions and bus.subscriptions[0][0] == CMD_EXECUTE_CASHOUT


def test_matched_cashout_publishes_success_with_green_up():
    bus = _FakeBus()
    router = _FakeRouter(_ok_result(matched=4.0, bet_id="CB1"))
    CashoutExecutor(bus, router).on_cmd_execute_cashout(_cmd_payload())

    # Piazza il lato di green-up corretto (side -> bet_type).
    assert router.calls[0]["bet_type"] == "LAY"
    assert router.calls[0]["market_id"] == "1.222"
    assert router.calls[0]["selection_id"] == 55
    assert router.calls[0]["price"] == 1.8
    assert router.calls[0]["stake"] == 4.0

    out = bus.last(CASHOUT_SUCCESS)
    assert out is not None
    assert out["green_up"] == 2.5
    assert out["matched"] == 4.0
    assert out["status"] == "DONE"
    assert out["bet_id"] == "CB1"
    assert CASHOUT_FAILED not in bus.topics()


def test_placed_but_unmatched_is_failed_not_success():
    bus = _FakeBus()
    router = _FakeRouter(
        {"ok": False, "placed": True, "order_unknown": False, "status": "SUCCESS",
         "matched": 0.0, "bet_id": "CB2", "error": None}
    )
    CashoutExecutor(bus, router).on_cmd_execute_cashout(_cmd_payload())

    assert CASHOUT_SUCCESS not in bus.topics()
    out = bus.last(CASHOUT_FAILED)
    assert out["status"] == "UNMATCHED"
    assert out["bet_id"] == "CB2"  # bet_id riportato: l'ordine esiste, no orphan.


def test_order_unknown_is_ambiguous_never_retried():
    bus = _FakeBus()
    router = _FakeRouter(
        {"ok": False, "placed": False, "order_unknown": True, "status": "AMBIGUOUS",
         "matched": 0.0, "bet_id": None, "error": "TIMEOUT"}
    )
    CashoutExecutor(bus, router).on_cmd_execute_cashout(_cmd_payload())

    assert len(router.calls) == 1  # nessun retry.
    assert CASHOUT_SUCCESS not in bus.topics()
    assert bus.last(CASHOUT_FAILED)["status"] == "AMBIGUOUS"


def test_rejected_placement_is_failed():
    bus = _FakeBus()
    router = _FakeRouter(
        {"ok": False, "placed": False, "order_unknown": False, "status": "FAILURE",
         "matched": 0.0, "bet_id": None, "error": "BET_REJECTED"}
    )
    CashoutExecutor(bus, router).on_cmd_execute_cashout(_cmd_payload())

    assert CASHOUT_SUCCESS not in bus.topics()
    assert bus.last(CASHOUT_FAILED)["status"] == "FAILURE"


def test_router_exception_fails_closed():
    bus = _FakeBus()
    router = _FakeRouter(raises=RuntimeError("boom"))
    CashoutExecutor(bus, router).on_cmd_execute_cashout(_cmd_payload())

    assert CASHOUT_SUCCESS not in bus.topics()
    assert bus.last(CASHOUT_FAILED)["status"] == "ERROR"


def test_invalid_payload_rejected_without_placement():
    class _StrictSafety:
        def validate_cashout_request(self, payload):
            raise ValueError("price <= 1")

    bus = _FakeBus()
    router = _FakeRouter(_ok_result())
    CashoutExecutor(bus, router, safety_layer=_StrictSafety()).on_cmd_execute_cashout(
        _cmd_payload(price=1.0)
    )

    assert router.calls == []  # fail-closed: nessun piazzamento.
    assert CASHOUT_SUCCESS not in bus.topics()
    assert bus.last(CASHOUT_FAILED)["status"] == "REJECTED"


def test_no_safety_layer_still_validates_floor():
    # Greptile P1: senza SafetyLayer la validazione NON deve essere saltata.
    # Un price<=1 deve essere rigettato al confine dell'executor, mai piazzato.
    bus = _FakeBus()
    router = _FakeRouter(_ok_result())
    CashoutExecutor(bus, router).on_cmd_execute_cashout(_cmd_payload(price=1.0))

    assert router.calls == []
    assert CASHOUT_SUCCESS not in bus.topics()
    assert bus.last(CASHOUT_FAILED)["status"] == "REJECTED"


def test_no_safety_layer_valid_payload_places():
    bus = _FakeBus()
    router = _FakeRouter(_ok_result(matched=4.0))
    CashoutExecutor(bus, router).on_cmd_execute_cashout(_cmd_payload())

    assert len(router.calls) == 1
    assert bus.last(CASHOUT_SUCCESS)["matched"] == 4.0


def test_missing_side_rejected_not_defaulted_to_lay():
    # Greptile P1 (security): un side mancante NON deve diventare LAY (che
    # piazzerebbe un ordine reale aumentando l'esposizione) => rigetto.
    bus = _FakeBus()
    router = _FakeRouter(_ok_result())
    payload = _cmd_payload()
    del payload["side"]
    CashoutExecutor(bus, router).on_cmd_execute_cashout(payload)

    assert router.calls == []
    assert CASHOUT_SUCCESS not in bus.topics()
    assert bus.last(CASHOUT_FAILED)["status"] == "REJECTED"


def test_non_finite_price_or_stake_rejected_without_placement():
    # Codex P2: NaN/Inf passano i confronti <= (sia nel floor sia in SafetyLayer)
    # ma corromperebbero la richiesta Betfair/sim => rigetto, mai piazzamento.
    for bad in (float("nan"), float("inf")):
        bus = _FakeBus()
        router = _FakeRouter(_ok_result())
        CashoutExecutor(bus, router).on_cmd_execute_cashout(_cmd_payload(price=bad))
        assert router.calls == []
        assert bus.last(CASHOUT_FAILED)["status"] == "REJECTED"

        bus = _FakeBus()
        router = _FakeRouter(_ok_result())
        CashoutExecutor(bus, router).on_cmd_execute_cashout(_cmd_payload(stake=bad))
        assert router.calls == []
        assert bus.last(CASHOUT_FAILED)["status"] == "REJECTED"


def test_non_finite_rejected_even_with_permissive_safety_layer():
    # Anche se un SafetyLayer permissivo non controlla la finitezza, l'executor
    # deve comunque rigettare (la guardia gira sempre).
    class _PermissiveSafety:
        def validate_cashout_request(self, payload):
            return True

    bus = _FakeBus()
    router = _FakeRouter(_ok_result())
    CashoutExecutor(bus, router, safety_layer=_PermissiveSafety()).on_cmd_execute_cashout(
        _cmd_payload(price=float("nan"))
    )
    assert router.calls == []
    assert bus.last(CASHOUT_FAILED)["status"] == "REJECTED"


def test_invalid_side_rejected_even_with_safety_layer():
    # Codex P2: il SafetyLayer verifica solo che side sia stringa, non la
    # allow-list. side='NOPE' verrebbe coerciato a BACK dal client live =>
    # l'invariante hard (sempre attiva) deve rigettarlo.
    class _PermissiveSafety:
        def validate_cashout_request(self, payload):
            return True

    bus = _FakeBus()
    router = _FakeRouter(_ok_result())
    CashoutExecutor(bus, router, safety_layer=_PermissiveSafety()).on_cmd_execute_cashout(
        _cmd_payload(side="NOPE")
    )
    assert router.calls == []
    assert CASHOUT_SUCCESS not in bus.topics()
    assert bus.last(CASHOUT_FAILED)["status"] == "REJECTED"


def test_missing_green_up_rejected_before_placement():
    # Codex P2: senza green_up l'hedge verrebbe piazzato e CASHOUT_SUCCESS
    # riporterebbe green_up=0.0 (P&L falso). green_up è richiesto sempre.
    bus = _FakeBus()
    router = _FakeRouter(_ok_result())
    payload = _cmd_payload()
    del payload["green_up"]
    CashoutExecutor(bus, router).on_cmd_execute_cashout(payload)

    assert router.calls == []
    assert CASHOUT_SUCCESS not in bus.topics()
    assert bus.last(CASHOUT_FAILED)["status"] == "REJECTED"


def test_non_dict_payload_rejected():
    bus = _FakeBus()
    router = _FakeRouter(_ok_result())
    CashoutExecutor(bus, router).on_cmd_execute_cashout(None)

    assert router.calls == []
    assert bus.last(CASHOUT_FAILED)["status"] == "REJECTED"


def test_missing_required_field_rejected():
    bus = _FakeBus()
    router = _FakeRouter(_ok_result())
    payload = _cmd_payload()
    del payload["market_id"]
    CashoutExecutor(bus, router).on_cmd_execute_cashout(payload)

    assert router.calls == []
    assert bus.last(CASHOUT_FAILED)["status"] == "REJECTED"


def test_success_payload_satisfies_cashout_success_schema():
    # Il payload CASHOUT_SUCCESS deve rispettare lo schema di SafetyLayer
    # (green_up/matched/status, matched>=0): consumato da telegram_sender.
    from core.safety_layer import SafetyLayer

    bus = _FakeBus()
    router = _FakeRouter(_ok_result(matched=4.0))
    CashoutExecutor(bus, router).on_cmd_execute_cashout(_cmd_payload())

    out = bus.last(CASHOUT_SUCCESS)
    assert SafetyLayer().validate_cashout_success(out) is True
