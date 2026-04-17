import pytest
import math

from dutching import calculate_cashout, calculate_dutching_stakes, dynamic_cashout_single
from controllers.dutching_controller import DutchingController


class _Mode:
    value = "ACTIVE"


class _Runtime:
    mode = _Mode()
    duplication_guard = None
    table_manager = None
    config = None
    risk_desk = None
    dutching_batch_manager = None


def _controller_payload(odds, total_stake=100.0, commission=4.5):
    return {
        "market_id": "1.234",
        "event_name": "Controller Dutching",
        "market_name": "Match Odds",
        "total_stake": float(total_stake),
        "commission": float(commission),
        "selections": [
            {"selectionId": idx + 1, "price": float(price), "side": "BACK"}
            for idx, price in enumerate(odds)
        ],
    }


@pytest.mark.integration
def test_dutching_then_cashout_flow_back():
    dutch = calculate_dutching_stakes([3.0, 4.0, 5.0], 60)

    assert len(dutch["stakes"]) == 3

    first_stake = dutch["stakes"][0]
    cash = dynamic_cashout_single(
        matched_stake=first_stake,
        matched_price=3.0,
        current_price=2.4,
        side="BACK",
    )

    assert cash["cashout_stake"] > 0
    assert cash["side_to_place"] == "LAY"


@pytest.mark.integration
def test_cashout_function_and_dynamic_cashout_are_consistent():
    a = calculate_cashout(100, 2.0, 1.5, "BACK")
    b = dynamic_cashout_single(
        matched_stake=100,
        matched_price=2.0,
        current_price=1.5,
        side="BACK",
        commission=0,
    )

    assert abs(a["cashout_stake"] - b["cashout_stake"]) < 0.01
    assert abs(a["profit_if_win"] - b["profit_if_win"]) < 0.05
    assert abs(a["profit_if_lose"] - b["profit_if_lose"]) < 0.05


@pytest.mark.integration
def test_dutching_market_net_semantics_are_explicit_under_4p5_commission():
    result = calculate_dutching_stakes([2.4, 3.6, 5.2], 75, commission=4.5)

    assert len(result["profits"]) == len(result["net_profits"]) == 3
    assert all(math.isfinite(x) for x in result["profits"])
    assert all(math.isfinite(x) for x in result["net_profits"])

    # Market-net semantics are explicit: net is never above gross and can be
    # equal only when gross is non-positive.
    for gross, net in zip(result["profits"], result["net_profits"]):
        assert net <= gross + 1e-12
        if gross > 0:
            assert abs(net - (gross * 0.955)) <= 0.02
        else:
            assert net == gross


@pytest.mark.integration
def test_controller_preview_propagates_commission_into_authoritative_path(monkeypatch):
    captured = {}

    def fake_calculate_dutching(selections, total_stake, commission=0.0):
        captured["commission"] = commission
        return (
            [{"selectionId": 1, "price": 2.0, "stake": float(total_stake), "side": "BACK"}],
            1.0,
            50.0,
            0.955,
        )

    monkeypatch.setattr(
        "controllers.dutching_controller.calculate_dutching",
        fake_calculate_dutching,
    )

    controller = DutchingController(bus=None, runtime_controller=_Runtime())
    out = controller.preview(_controller_payload([2.0], total_stake=10.0, commission=4.5))

    assert out["ok"] is True
    assert captured["commission"] == 4.5
    assert out["commission_pct"] == 4.5


@pytest.mark.integration
def test_controller_preview_net_profit_semantics_are_explicit_and_equalized():
    controller = DutchingController(bus=None, runtime_controller=_Runtime())
    out = controller.preview(_controller_payload([3.0, 4.0, 6.0], total_stake=120.0, commission=4.5))

    assert out["ok"] is True
    assert out["avg_profit_semantics"] == "gross"
    assert "avg_profit" in out and "avg_profit_net" in out
    assert out["avg_profit_net"] <= out["avg_profit"] + 1e-12
    net_profits = [float(item["profitIfWinsNet"]) for item in out["results"]]
    assert max(net_profits) - min(net_profits) <= 0.10
    assert out["profitable_net"] is True


@pytest.mark.integration
def test_controller_preview_unprofitable_dutch_is_honest_not_misleading():
    controller = DutchingController(bus=None, runtime_controller=_Runtime())
    out = controller.preview(_controller_payload([1.9, 1.9], total_stake=100.0, commission=4.5))

    assert out["ok"] is True
    assert out["book_pct"] > 100.0
    assert out["avg_profit_net"] < 0.0
    assert out["profitable_net"] is False
    for row in out["results"]:
        assert row["profitIfWinsNet"] <= row["profitIfWins"] + 1e-12
        assert row["profitIfWinsNet"] < 0.0


@pytest.mark.integration
def test_controller_profitable_net_uses_worst_case_not_average(monkeypatch):
    def fake_calculate_dutching(_selections, _total_stake, commission=0.0):
        _ = commission
        return (
            [
                {"selectionId": 1, "price": 3.0, "stake": 40.0, "side": "BACK", "profitIfWinsNet": 0.03},
                {"selectionId": 2, "price": 4.0, "stake": 30.0, "side": "BACK", "profitIfWinsNet": 0.04},
                {"selectionId": 3, "price": 6.0, "stake": 30.0, "side": "BACK", "profitIfWinsNet": -0.01},
            ],
            0.03,
            90.0,
            0.02,
        )

    monkeypatch.setattr(
        "controllers.dutching_controller.calculate_dutching",
        fake_calculate_dutching,
    )
    controller = DutchingController(bus=None, runtime_controller=_Runtime())
    payload = _controller_payload([3.0, 4.0, 6.0], total_stake=100.0, commission=4.5)

    preview = controller.preview(payload)
    precheck = controller.precheck(payload)

    assert preview["ok"] is True
    assert precheck["ok"] is True
    assert preview["avg_profit_net"] > 0.0
    assert precheck["avg_profit_net"] > 0.0
    assert preview["profitable_net"] is False
    assert precheck["profitable_net"] is False


@pytest.mark.integration
def test_controller_profitable_net_true_only_when_all_net_outcomes_positive(monkeypatch):
    def fake_calculate_dutching(_selections, _total_stake, commission=0.0):
        _ = commission
        return (
            [
                {"selectionId": 1, "price": 3.0, "stake": 40.0, "side": "BACK", "profitIfWinsNet": 0.01},
                {"selectionId": 2, "price": 4.0, "stake": 30.0, "side": "BACK", "profitIfWinsNet": 0.02},
                {"selectionId": 3, "price": 6.0, "stake": 30.0, "side": "BACK", "profitIfWinsNet": 0.03},
            ],
            0.02,
            90.0,
            0.02,
        )

    monkeypatch.setattr(
        "controllers.dutching_controller.calculate_dutching",
        fake_calculate_dutching,
    )
    controller = DutchingController(bus=None, runtime_controller=_Runtime())
    payload = _controller_payload([3.0, 4.0, 6.0], total_stake=100.0, commission=4.5)

    preview = controller.preview(payload)
    precheck = controller.precheck(payload)

    assert preview["ok"] is True
    assert precheck["ok"] is True
    assert preview["profitable_net"] is True
    assert precheck["profitable_net"] is True


@pytest.mark.integration
def test_controller_profitable_net_is_fail_closed_when_worst_case_is_zero(monkeypatch):
    def fake_calculate_dutching(_selections, _total_stake, commission=0.0):
        _ = commission
        return (
            [
                {"selectionId": 1, "price": 3.0, "stake": 40.0, "side": "BACK", "profitIfWinsNet": 0.00},
                {"selectionId": 2, "price": 4.0, "stake": 30.0, "side": "BACK", "profitIfWinsNet": 0.03},
                {"selectionId": 3, "price": 6.0, "stake": 30.0, "side": "BACK", "profitIfWinsNet": 0.04},
            ],
            0.02,
            90.0,
            0.02,
        )

    monkeypatch.setattr(
        "controllers.dutching_controller.calculate_dutching",
        fake_calculate_dutching,
    )
    controller = DutchingController(bus=None, runtime_controller=_Runtime())
    payload = _controller_payload([3.0, 4.0, 6.0], total_stake=100.0, commission=4.5)

    preview = controller.preview(payload)
    precheck = controller.precheck(payload)

    assert preview["ok"] is True
    assert precheck["ok"] is True
    assert preview["profitable_net"] is False
    assert precheck["profitable_net"] is False
