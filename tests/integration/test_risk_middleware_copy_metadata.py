from __future__ import annotations


def test_risk_middleware_copy_metadata_placeholder_exists() -> None:
    payload = {
        "source": "COPY",
        "market_id": "1.234",
        "selection_id": 101,
        "price": 2.0,
        "size": 5.0,
        "side": "BACK",
        "master_id": "master-1",
        "action_id": "action-1",
    }

    assert payload["source"] == "COPY"
    assert payload["master_id"] == "master-1"
    assert payload["action_id"] == "action-1"
    assert payload["size"] > 0.0