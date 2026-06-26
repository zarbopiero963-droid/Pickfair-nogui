"""Unit test per CashoutCancelAdapter (Fase 2.1-B2.1, design C1)."""

from cashout_cancel_adapter import CashoutCancelAdapter, response_confirms_cancel


class _Broker:
    """Registra le chiamate e restituisce una risposta configurabile."""

    def __init__(self, response=None, raises=False):
        self.calls = []
        self._response = response if response is not None else {"status": "SUCCESS"}
        self._raises = raises

    def __call__(self, **kwargs):
        self.calls.append(kwargs)
        if self._raises:
            raise RuntimeError("BROKER_BLEW_UP")
        return self._response


def _adapter(sim, live_broker, sim_broker):
    return CashoutCancelAdapter(
        is_simulation=lambda: sim,
        live_cancel=live_broker,
        sim_cancel=sim_broker,
    )


# ---------------------------------------------------------------------------
# response_confirms_cancel
# ---------------------------------------------------------------------------


def test_confirm_truthy_non_dict():
    assert response_confirms_cancel(True) is True
    assert response_confirms_cancel(["B1"]) is True


def test_confirm_falsy_is_unconfirmed():
    assert response_confirms_cancel(None) is False
    assert response_confirms_cancel({}) is False
    assert response_confirms_cancel(False) is False


def test_confirm_live_ok_false_is_unconfirmed():
    assert response_confirms_cancel({"ok": False, "error": "X"}) is False


def test_confirm_live_ok_true_success():
    assert response_confirms_cancel(
        {"ok": True, "status": "SUCCESS", "result": {"status": "SUCCESS",
         "instructionReports": [{"status": "SUCCESS"}]}}
    ) is True


def test_confirm_live_nested_report_failure_is_unconfirmed():
    # Il wrapper live annida i report sotto result["result"]["instructionReports"].
    assert response_confirms_cancel(
        {"ok": True, "status": "SUCCESS", "result": {"status": "SUCCESS",
         "instructionReports": [{"status": "TIMEOUT"}]}}
    ) is False


def test_confirm_sim_top_level_report_failure_is_unconfirmed():
    assert response_confirms_cancel(
        {"status": "SUCCESS", "instructionReports": [{"status": "FAILURE"}], "simulated": True}
    ) is False


def test_confirm_sim_all_success():
    assert response_confirms_cancel(
        {"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS"}], "simulated": True}
    ) is True


# ---------------------------------------------------------------------------
# CashoutCancelAdapter.cancel — routing sim/live + firma
# ---------------------------------------------------------------------------


def test_live_routing_uses_bet_ids_keyword():
    live = _Broker({"ok": True, "status": "SUCCESS"})
    sim = _Broker()
    ok = _adapter(False, live, sim).cancel("1.1", ["B1", "B2"])
    assert ok is True
    assert sim.calls == []                       # SIM non toccato in live
    assert live.calls == [{"market_id": "1.1", "bet_ids": ["B1", "B2"]}]


def test_sim_routing_builds_instructions_keyword():
    live = _Broker()
    sim = _Broker({"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS"}]})
    ok = _adapter(True, live, sim).cancel("1.1", ["B1", "B2"])
    assert ok is True
    assert live.calls == []                      # LIVE non toccato in sim
    assert sim.calls == [{"market_id": "1.1",
                          "instructions": [{"betId": "B1"}, {"betId": "B2"}]}]


def test_live_failure_dict_returns_false():
    live = _Broker({"ok": False, "error": "CANCEL_FAILED"})
    assert _adapter(False, live, _Broker()).cancel("1.1", ["B1"]) is False


def test_sim_per_instruction_failure_returns_false():
    sim = _Broker({"status": "SUCCESS", "instructionReports": [{"status": "FAILURE"}]})
    assert _adapter(True, _Broker(), sim).cancel("1.1", ["B1"]) is False


def test_broker_exception_is_fail_closed():
    live = _Broker(raises=True)
    assert _adapter(False, live, _Broker()).cancel("1.1", ["B1"]) is False


def test_empty_bet_ids_is_noop_and_does_not_call_broker():
    # Sicurezza: bet_ids vuoto NON deve raggiungere il broker (cancel-all del mercato).
    live = _Broker()
    sim = _Broker()
    assert _adapter(False, live, sim).cancel("1.1", []) is True
    assert _adapter(True, live, sim).cancel("1.1", []) is True
    assert live.calls == [] and sim.calls == []


def test_blank_bet_ids_filtered_out_then_noop():
    live = _Broker()
    assert _adapter(False, live, _Broker()).cancel("1.1", ["", "  ", None]) is True
    assert live.calls == []


def test_empty_market_id_returns_false():
    live = _Broker({"ok": True})
    assert _adapter(False, live, _Broker()).cancel("", ["B1"]) is False
    assert live.calls == []
