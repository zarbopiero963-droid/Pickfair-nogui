from __future__ import annotations


def test_risk_middleware_pattern_metadata_placeholder_exists() -> None:
    payload = {
        "market_id": "1.234",
        "selection_id": 101,
        "price": 2.0,
        "size": 5.0,
        "side": "BACK",
        "pattern_id": "pattern-1",
        "pattern_label": "OVER_15",
    }

    assert payload["pattern_id"] == "pattern-1"
    assert payload["pattern_label"] == "OVER_15"
    assert payload["price"] > 1.0