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


def test_risk_middleware_copy_metadata_is_forwarded_to_cmd_layer() -> None:
    bus = EventBus(workers=1)
    RiskMiddleware(bus=bus)
    seen = []
    bus.subscribe("CMD_QUICK_BET", lambda payload: seen.append(dict(payload or {})))

    bus.publish(
        "REQ_QUICK_BET",
        {
            "source": "COPY",
            "market_id": "1.234",
            "selection_id": 101,
            "price": 2.0,
            "stake": 5.0,
            "bet_type": "BACK",
            "simulation_mode": True,
            "copy_meta": {"master_id": "master-1", "action_id": "action-1"},
        },
    )

    assert _wait_until(lambda: len(seen) == 1), "CMD_QUICK_BET non ricevuto"
    forwarded = seen[0]
    assert forwarded["order_origin"] == "COPY"
    assert forwarded["copy_meta"] == {"master_id": "master-1", "action_id": "action-1"}
    assert forwarded["simulation_mode"] is True
    assert forwarded["source"] == "COPY"
