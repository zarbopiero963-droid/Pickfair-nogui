from observability.alerts_manager import AlertsManager


def test_alerts_manager_upsert_and_resolve():
    mgr = AlertsManager()

    mgr.upsert_alert("MEMORY_HIGH", "warning", "Memory high", details={"rss": 600})
    mgr.upsert_alert("MEMORY_HIGH", "critical", "Memory critical", details={"rss": 900})

    snap = mgr.snapshot()
    assert snap["active_count"] == 1
    assert snap["alerts"][0]["count"] == 2
    assert snap["alerts"][0]["severity"] == "critical"

    mgr.resolve_alert("MEMORY_HIGH")
    snap = mgr.snapshot()
    assert snap["active_count"] == 0
