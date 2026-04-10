from observability.health_registry import DEGRADED, NOT_READY, READY, HealthRegistry


def test_live_readiness_report_shape_is_stable():
    health = HealthRegistry()
    health.set_component("runtime", READY, reason="ok", details={"loop": "running"})

    snapshot = health.snapshot()

    assert set(snapshot.keys()) == {"overall_status", "components", "updated_at"}
    assert set(snapshot["components"]["runtime"].keys()) == {
        "name",
        "status",
        "reason",
        "details",
        "updated_at",
    }


def test_ready_degraded_not_ready_are_distinct_states():
    health = HealthRegistry()

    health.set_component("runtime", READY)
    assert health.snapshot()["overall_status"] == READY

    health.set_component("betfair_service", DEGRADED, reason="latency")
    assert health.snapshot()["overall_status"] == DEGRADED

    health.set_component("shutdown", NOT_READY, reason="missing")
    assert health.snapshot()["overall_status"] == NOT_READY


def test_unknown_status_is_not_treated_as_ready():
    health = HealthRegistry()
    health.set_component("runtime", "UNKNOWN", reason="unclassified")
    health.set_component("live_gate", NOT_READY, reason="unknown_runtime_status")

    snapshot = health.snapshot()

    assert snapshot["components"]["runtime"]["status"] == "UNKNOWN"
    assert snapshot["components"]["live_gate"]["status"] == NOT_READY
    assert snapshot["overall_status"] == NOT_READY
