import copy

import pytest

from services.telegram_signal_processor import TelegramSignalProcessor


def _valid_signal(**overrides):
    base = {
        "action": "lay",
        "price": "2,15",
        "market_id": "1.234",
        "selection_id": "77",
        "event_name": "Roma v Milan",
        "market_name": "Over/Under 2.5 Goals",
        "selection": "Over 2.5",
        "minute": "61",
        "home_score": "1",
        "away_score": "1",
        "raw_text": "signal text",
        "signal_type": "OVER_SUCCESSIVO",
        "copy_meta": {"master_id": "M1"},
    }
    base.update(overrides)
    return base


def test_normalize_ingestion_signal_valid_payload_is_deterministic_and_preserves_fields():
    p = TelegramSignalProcessor()
    raw = _valid_signal()

    out = p.normalize_ingestion_signal(raw)

    assert out["ok"] is True
    assert out["error_code"] is None
    n = out["normalized_signal"]
    assert n["boundary_stage"] == "telegram_ingestion_normalized_v1"
    assert n["action"] == "LAY"
    assert n["bet_type"] == "LAY"
    assert n["price"] == 2.15
    assert n["market_id"] == "1.234"
    assert n["selection_id"] == 77
    assert n["order_origin"] == "COPY"
    assert n["copy_meta"] == {"master_id": "M1"}
    assert n["raw_signal"] == raw


def test_normalize_ingestion_signal_rejects_non_dict_and_ambiguous_meta_fail_closed():
    p = TelegramSignalProcessor()

    not_dict = p.normalize_ingestion_signal("not-a-dict")
    assert not_dict == {
        "ok": False,
        "error_code": "SIGNAL_NOT_DICT",
        "error_reason": "telegram signal must be a dict payload",
        "normalized_signal": {},
    }

    both = p.normalize_ingestion_signal(
        {"event_name": "Roma v Milan", "copy_meta": {"master_id": "M1"}, "pattern_meta": {"pattern_id": "P1"}}
    )
    assert both["ok"] is False
    assert both["error_code"] == "COPY_PATTERN_MUTUALLY_EXCLUSIVE"
    assert both["normalized_signal"] == {}


@pytest.mark.parametrize(
    "raw_price, expected_price",
    [
        ("abc", None),
        ("", None),
    ],
)
def test_normalize_ingestion_signal_malformed_price_values_are_fail_closed(raw_price, expected_price):
    p = TelegramSignalProcessor()
    out = p.normalize_ingestion_signal(_valid_signal(price=raw_price))
    assert out["ok"] is True
    assert out["normalized_signal"]["price"] is expected_price


def test_normalize_ingestion_signal_missing_action_uses_current_fail_closed_default_back():
    p = TelegramSignalProcessor()
    out = p.normalize_ingestion_signal(_valid_signal(action=None))
    assert out["ok"] is True
    assert out["normalized_signal"]["action"] == "BACK"
    assert out["normalized_signal"]["bet_type"] == "BACK"


@pytest.mark.parametrize(
    "payload",
    [
        _valid_signal(market_id=None),
        _valid_signal(selection_id=None),
        _valid_signal(selection_id="not-an-int"),
    ],
)
def test_build_runtime_signal_missing_or_non_integer_keys_fail_closed(payload):
    p = TelegramSignalProcessor()
    assert p.build_runtime_signal(payload, stake=2.0) is None


def test_normalize_ingestion_signal_does_not_mutate_input_and_output_is_resolver_safe_shape():
    p = TelegramSignalProcessor()
    raw = {
        "event": "Napoli v Inter",
        "market": "Over/Under 1.5 Goals",
        "selection": "Over 1.5",
        "odds": "1.90",
        "selectionId": 11,
        "marketId": "1.900",
        "message": "NEXT GOL",
    }
    before = copy.deepcopy(raw)

    out = p.normalize_ingestion_signal(raw)

    assert raw == before
    assert out["ok"] is True
    n = out["normalized_signal"]
    for key in ["event_name", "market_name", "selection", "price", "market_id", "selection_id", "raw_text"]:
        assert key in n
    assert n["event_name"] == "Napoli v Inter"
    assert n["price"] == 1.9
    assert n["market_id"] == "1.900"
    assert n["selection_id"] == 11
