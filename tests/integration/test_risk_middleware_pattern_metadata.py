from __future__ import annotations

import time

from core.event_bus import EventBus
from core.risk_middleware import RiskMiddleware


def _wait_until(condition, timeout: float = 1.0) -> bool:
    start = time.time()
    while (time.time() - start) < timeout:
        if condition():
            return True
        time.sleep(0.01)
    return False


def test_risk_middleware_pattern_metadata_is_forwarded_to_cmd_layer() -> None:
    bus = EventBus(workers=1)
    RiskMiddleware(bus=bus)
    seen = []
    bus.subscribe("CMD_QUICK_BET", lambda payload: seen.append(dict(payload or {})))

    bus.publish(
        "REQ_QUICK_BET",
        {
            "source": "PATTERN",
            "market_id": "1.234",
            "selection_id": 101,
            "price": 2.0,
            "stake": 5.0,
            "bet_type": "BACK",
            "pattern_meta": {"pattern_id": "pattern-1", "pattern_label": "OVER_15"},
        },
    )

    assert _wait_until(lambda: len(seen) == 1), "CMD_QUICK_BET non ricevuto"
    forwarded = seen[0]
    assert forwarded["order_origin"] == "PATTERN"
    assert forwarded["pattern_meta"] == {"pattern_id": "pattern-1", "pattern_label": "OVER_15"}
    assert forwarded["source"] == "PATTERN"
