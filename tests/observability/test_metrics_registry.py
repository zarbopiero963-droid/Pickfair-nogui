from observability.metrics_registry import MetricsRegistry


def test_metrics_registry_counters_and_gauges():
    reg = MetricsRegistry()

    reg.inc("orders_total")
    reg.inc("orders_total", 2)
    reg.set_gauge("memory_rss_mb", 123.4)
    reg.set_meta("mode", "live")

    snap = reg.snapshot()

    assert snap["counters"]["orders_total"] == 3
    assert snap["gauges"]["memory_rss_mb"] == 123.4
    assert snap["metadata"]["mode"] == "live"
