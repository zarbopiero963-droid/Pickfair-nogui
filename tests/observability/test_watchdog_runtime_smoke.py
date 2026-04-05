import time

import pytest

from observability.alerts_manager import AlertsManager
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry
from observability.watchdog_service import WatchdogService


class Probe:
    def collect_health(self):
        return {"runtime": {"status": "READY", "reason": "ok", "details": {}}}

    def collect_metrics(self):
        return {"memory_rss_mb": 10.0, "inflight_count": 1.0}


class SnapshotStub:
    def __init__(self):
        self.calls = 0

    def collect_and_store(self):
        self.calls += 1


@pytest.mark.smoke
def test_watchdog_ticks_and_updates_health_metrics():
    health = HealthRegistry()
    metrics = MetricsRegistry()
    alerts = AlertsManager()
    incidents = IncidentsManager()
    snapshot = SnapshotStub()

    watchdog = WatchdogService(
        probe=Probe(),
        health_registry=health,
        metrics_registry=metrics,
        alerts_manager=alerts,
        incidents_manager=incidents,
        snapshot_service=snapshot,
        interval_sec=0.05,
    )

    watchdog.start()
    time.sleep(0.16)
    watchdog.stop()

    assert snapshot.calls >= 1
    hsnap = health.snapshot()
    msnap = metrics.snapshot()
    assert hsnap["components"].get("runtime", {}).get("status") == "READY"
    assert msnap["gauges"].get("memory_rss_mb", 0.0) == 10.0
