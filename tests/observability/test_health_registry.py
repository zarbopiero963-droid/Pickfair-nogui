from observability.health_registry import DEGRADED, NOT_READY, READY, HealthRegistry


def test_health_registry_snapshot_overall_status():
    reg = HealthRegistry()

    snap = reg.snapshot()
    assert snap["overall_status"] == NOT_READY

    reg.set_component("db", READY)
    reg.set_component("engine", READY)
    snap = reg.snapshot()
    assert snap["overall_status"] == READY

    reg.set_component("betfair", DEGRADED, reason="disconnected")
    snap = reg.snapshot()
    assert snap["overall_status"] == DEGRADED
