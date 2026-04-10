from observability.runtime_probe import RuntimeProbe


class _Ready:
    def is_ready(self):
        return True


class _NotReady:
    def is_ready(self):
        return False


class _TradingReady:
    def readiness(self):
        return {"state": "READY", "health": {"lag_ms": 1}}


class _TradingUnknown:
    def readiness(self):
        return {"state": "READY", "health": None}


class _BetfairConnected:
    connected = True


class _BetfairDisconnected:
    connected = False


class _SafeModeActive:
    def is_enabled(self):
        return True


class _SafeModeInactive:
    def is_enabled(self):
        return False



def test_runtime_probe_live_readiness_report_shape_is_stable():
    probe = RuntimeProbe(
        db=_Ready(),
        trading_engine=_TradingReady(),
        runtime_controller=_Ready(),
        betfair_service=_BetfairConnected(),
        safe_mode=_SafeModeInactive(),
        shutdown_manager=_Ready(),
    )

    report = probe.get_live_readiness_report()

    assert set(report.keys()) == {"ready", "level", "blockers", "details"}
    assert set(report["details"].keys()) == {"degraded", "components", "unknown_components"}
    assert isinstance(report["blockers"], list)
    assert isinstance(report["details"]["degraded"], list)
    assert isinstance(report["details"]["components"], dict)


def test_ready_degraded_not_ready_are_distinguishable_in_report_contract():
    ready_probe = RuntimeProbe(
        db=_Ready(),
        trading_engine=_TradingReady(),
        runtime_controller=_Ready(),
        betfair_service=_BetfairConnected(),
        safe_mode=_SafeModeInactive(),
        shutdown_manager=_Ready(),
    )
    degraded_probe = RuntimeProbe(
        db=_Ready(),
        trading_engine=_TradingReady(),
        runtime_controller=_Ready(),
        betfair_service=_BetfairDisconnected(),
        safe_mode=_SafeModeInactive(),
        shutdown_manager=_Ready(),
    )
    not_ready_probe = RuntimeProbe(
        db=None,
        trading_engine=_TradingReady(),
        runtime_controller=_Ready(),
        betfair_service=_BetfairConnected(),
        safe_mode=_SafeModeInactive(),
        shutdown_manager=_Ready(),
    )

    assert ready_probe.get_live_readiness_report()["level"] == "READY"
    assert degraded_probe.get_live_readiness_report()["level"] == "DEGRADED"
    assert not_ready_probe.get_live_readiness_report()["level"] == "NOT_READY"


def test_unknown_state_is_not_reported_as_ready_and_fails_closed():
    probe = RuntimeProbe(
        db=_Ready(),
        trading_engine=_TradingUnknown(),
        runtime_controller=_Ready(),
        betfair_service=_BetfairConnected(),
        safe_mode=_SafeModeInactive(),
        shutdown_manager=_Ready(),
    )

    report = probe.get_live_readiness_report()

    assert report["ready"] is False
    assert report["level"] == "NOT_READY"
    assert "trading_engine" in report["details"]["unknown_components"]
    assert any(item["code"] == "READINESS_SIGNAL_UNKNOWN" for item in report["blockers"])


def test_missing_dependency_and_kill_switch_have_expected_blockers():
    probe = RuntimeProbe(
        db=_Ready(),
        trading_engine=_TradingReady(),
        runtime_controller=_Ready(),
        betfair_service=None,
        safe_mode=_SafeModeActive(),
        shutdown_manager=_Ready(),
    )

    report = probe.get_live_readiness_report()
    blocker_codes = {item["code"] for item in report["blockers"]}
    degraded_codes = {item["code"] for item in report["details"]["degraded"]}

    assert report["ready"] is False
    assert report["level"] == "NOT_READY"
    assert "LIVE_DEPENDENCY_MISSING" in blocker_codes
    assert "SAFE_MODE_BLOCKING" in degraded_codes
