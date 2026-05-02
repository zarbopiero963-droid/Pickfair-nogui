import copy

from services.telegram_signal_processor import TelegramSignalProcessor


def test_normalize_ingestion_signal_valid_payload_is_deterministic_and_preserves_fields():
    p = TelegramSignalProcessor()
    raw = {
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
        {
            "event_name": "Roma v Milan",
            "copy_meta": {"master_id": "M1"},
            "pattern_meta": {"pattern_id": "P1"},
        }
    )
    assert both["ok"] is False
    assert both["error_code"] == "COPY_PATTERN_MUTUALLY_EXCLUSIVE"
    assert both["normalized_signal"] == {}


def test_normalize_action_unsupported_value_fails_closed_to_back_and_build_runtime_requires_ids_and_price():
    p = TelegramSignalProcessor()

    n = p.normalize_ingestion_signal({"action": "CASHOUT", "event_name": "A v B"})
    assert n["ok"] is True
    assert n["normalized_signal"]["action"] == "BACK"
    assert n["normalized_signal"]["bet_type"] == "BACK"

    # Missing market/selection/price must not produce executable runtime payload.
    assert p.build_runtime_signal({"action": "BACK", "event_name": "A v B"}, stake=2.0) is None


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

    assert raw == before  # input unchanged
    assert out["ok"] is True
    n = out["normalized_signal"]
    # Stable downstream-friendly keys consumed by resolver/runtime boundaries
    for key in ["event_name", "market_name", "selection", "price", "market_id", "selection_id", "raw_text"]:
        assert key in n
    assert n["event_name"] == "Napoli v Inter"
    assert n["price"] == 1.9
    assert n["market_id"] == "1.900"
    assert n["selection_id"] == 11
