from observability.runtime_probe import RuntimeProbe


class _ReadyComponent:
    def is_ready(self):
        return True


class _DegradedComponent:
    def is_ready(self):
        return False


class _TradingEngineReady:
    def readiness(self):
        return {"state": "READY", "health": {"latency_ms": 9}}


class _TradingEngineDegraded:
    def readiness(self):
        return {"state": "DEGRADED", "health": {"latency_ms": 900}}


class _TradingEngineReadyWithoutHealth:
    def readiness(self):
        return {"state": "READY", "health": {}}


def test_live_readiness_report_shape_is_stable_and_machine_readable():
    probe = RuntimeProbe(
        db=_ReadyComponent(),
        runtime_controller=_ReadyComponent(),
        shutdown_manager=_ReadyComponent(),
        trading_engine=_TradingEngineReady(),
    )

    report = probe.get_live_readiness_report()

    assert sorted(report.keys()) == ["blockers", "details", "level", "ready"]
    assert report["ready"] is False
    assert report["level"] == "NOT_READY"
    assert isinstance(report["blockers"], list)
    assert isinstance(report["details"], dict)

    details = report["details"]
    assert sorted(details.keys()) == ["components", "degraded", "unknown_components"]
    assert isinstance(details["components"], dict)
    assert isinstance(details["degraded"], list)
    assert isinstance(details["unknown_components"], list)


def test_live_readiness_report_distinguishes_ready_degraded_and_not_ready_levels():
    ready_probe = RuntimeProbe(
        db=_ReadyComponent(),
        runtime_controller=_ReadyComponent(),
        shutdown_manager=_ReadyComponent(),
        trading_engine=_TradingEngineReady(),
        betfair_service=type("B", (), {"connected": True})(),
        safe_mode=type("SM", (), {"enabled": False})(),
    )
    degraded_probe = RuntimeProbe(
        db=_ReadyComponent(),
        runtime_controller=_ReadyComponent(),
        shutdown_manager=_ReadyComponent(),
        trading_engine=_TradingEngineDegraded(),
        betfair_service=type("B", (), {"connected": True})(),
        safe_mode=type("SM", (), {"enabled": False})(),
    )
    not_ready_probe = RuntimeProbe(
        db=None,
        runtime_controller=_ReadyComponent(),
        shutdown_manager=_ReadyComponent(),
        trading_engine=_TradingEngineReady(),
        betfair_service=type("B", (), {"connected": True})(),
        safe_mode=type("SM", (), {"enabled": False})(),
    )

    ready_report = ready_probe.get_live_readiness_report()
    degraded_report = degraded_probe.get_live_readiness_report()
    not_ready_report = not_ready_probe.get_live_readiness_report()

    assert ready_report["level"] == "READY"
    assert ready_report["ready"] is True

    assert degraded_report["level"] == "DEGRADED"
    assert degraded_report["ready"] is False
    assert degraded_report["details"]["degraded"]

    assert not_ready_report["level"] == "NOT_READY"
    assert not_ready_report["ready"] is False
    assert any(item["name"] == "database" for item in not_ready_report["blockers"])


def test_live_readiness_report_never_promotes_unknown_to_ready():
    probe = RuntimeProbe(
        db=_ReadyComponent(),
        runtime_controller=_ReadyComponent(),
        shutdown_manager=_ReadyComponent(),
        trading_engine=_TradingEngineReadyWithoutHealth(),
        betfair_service=type("B", (), {"connected": True})(),
        safe_mode=type("SM", (), {"enabled": False})(),
    )

    report = probe.get_live_readiness_report()

    assert report["ready"] is False
    assert report["level"] == "NOT_READY"
    assert "trading_engine" in report["details"]["unknown_components"]
    assert any(
        blocker["name"] == "trading_engine" and blocker["status"] == "UNKNOWN"
        for blocker in report["blockers"]
    )
