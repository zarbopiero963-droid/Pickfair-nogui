from observability.alerts_manager import AlertsManager
from observability.anomaly_engine import AnomalyEngine
from observability.anomaly_rules import db_contention_detected, exposure_mismatch, ghost_order_detected
from observability.health_registry import HealthRegistry
from observability.incidents_manager import IncidentsManager
from observability.metrics_registry import MetricsRegistry
from observability.watchdog_service import WatchdogService


class _TelegramTradingProbe:
    def collect_health(self):
        return {
            "telegram_trading": {
                "status": "READY",
                "reason": "ok",
                "details": {"flow": "telegram"},
            }
        }

    def collect_metrics(self):
        return {"memory_rss_mb": 200.0, "inflight_count": 1.0}

    def collect_runtime_state(self):
        return {
            "reconcile": {"ghost_orders_count": 1},
            "alert_pipeline": {
                "alerts_enabled": True,
                "sender_available": True,
                "deliverable": True,
            },
        }


class _SnapshotCollector:
    def __init__(self):
        self.calls = 0

    def collect_and_store(self):
        self.calls += 1


def test_telegram_trading_flow_detects_and_logs_injected_anomalies_without_crash():
    alerts = AlertsManager()
    snapshot = _SnapshotCollector()

    watchdog = WatchdogService(
        probe=_TelegramTradingProbe(),
        health_registry=HealthRegistry(),
        metrics_registry=MetricsRegistry(),
        alerts_manager=alerts,
        incidents_manager=IncidentsManager(),
        snapshot_service=snapshot,
        anomaly_engine=AnomalyEngine([ghost_order_detected, exposure_mismatch, db_contention_detected]),
        anomaly_enabled=True,
        anomaly_context_provider=lambda: {
            "risk": {
                "expected_exposure": 100.0,
                "actual_exposure": 101.5,
                "exposure_tolerance": 0.01,
            },
            "db": {
                "lock_wait_ms": 350.0,
                "contention_events": 1,
                "lock_wait_threshold_ms": 200.0,
            },
        },
    )

    watchdog._tick()

    first_snapshot = alerts.snapshot()
    first_codes = {item["code"] for item in first_snapshot["alerts"] if item.get("source") == "anomaly"}

    assert "GHOST_ORDER_DETECTED" in first_codes
    assert "EXPOSURE_MISMATCH" in first_codes
    assert "DB_CONTENTION_DETECTED" in first_codes

    for item in first_snapshot["alerts"]:
        if item.get("code") in first_codes:
            assert item.get("active") is True

    # Must continue running after anomaly detection/logging and remain stable.
    watchdog._tick()
    assert snapshot.calls == 2
