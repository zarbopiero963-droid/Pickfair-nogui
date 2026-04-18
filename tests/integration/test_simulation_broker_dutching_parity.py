import pytest

from controllers.dutching_controller import DutchingController
from core.pnl_engine import MarketNetRealizedSettlementAggregator
from dutching import calculate_dutching_stakes
from simulation_broker import SimulationBroker


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
        "event_name": "Preview Settlement Linkage",
        "market_name": "Match Odds",
        "total_stake": float(total_stake),
        "commission": float(commission),
        "selections": [
            {"selectionId": idx + 1, "price": float(price), "side": "BACK"}
            for idx, price in enumerate(odds)
        ],
    }


@pytest.mark.integration
def test_dutching_realized_settlement_live_sim_parity_one_winner_many_losers():
    odds = [3.0, 4.0, 6.0]
    total_stake = 120.0
    commission_pct = 4.5
    winner_idx = 0

    preview = calculate_dutching_stakes(odds, total_stake, commission=commission_pct)
    stakes = preview["stakes"]
    profits = preview["profits"]
    net_profits = preview["net_profits"]

    assert len(stakes) == 3
    assert len(profits) == 3
    assert len(net_profits) == 3
    assert abs(sum(stakes) - total_stake) < 0.01

    # One selected winner + all other selected losers:
    # winner gross leg = stake * (odds - 1), each loser leg = -stake
    winner_gross_leg = stakes[winner_idx] * (odds[winner_idx] - 1.0)
    losers_gross_legs = [-stakes[i] for i in range(len(stakes)) if i != winner_idx]
    realized_market_gross = winner_gross_leg + sum(losers_gross_legs)

    # Helper math must link to realized gross path deterministically.
    assert realized_market_gross == pytest.approx(profits[winner_idx], abs=0.02)
    assert realized_market_gross > 0.0

    # Expected fixed-policy commission semantics on winning market-net path.
    expected_commission = realized_market_gross * 0.045
    expected_net = realized_market_gross - expected_commission
    assert net_profits[winner_idx] == pytest.approx(expected_net, abs=0.02)

    # "Live" settlement authority reference (same canonical market-net policy).
    live_agg = MarketNetRealizedSettlementAggregator(
        commission_pct=commission_pct,
        context="test_live_reference",
    )
    live = live_agg.apply(market_id="1.DUTCH", gross_pnl=realized_market_gross)

    # Simulation settlement authority under test.
    sim_broker = SimulationBroker(starting_balance=1000.0, commission_pct=commission_pct)
    sim = sim_broker.record_realized_settlement(realized_market_gross, market_id="1.DUTCH")

    assert sim["market_id"] == "1.DUTCH"
    assert sim["settlement_basis"] == "market_net_realized"
    assert sim["settlement_kind"] == "realized_settlement"
    assert sim["commission_pct"] == commission_pct
    assert sim["commission_amount"] == pytest.approx(expected_commission, abs=1e-12)
    assert sim["net_pnl"] == pytest.approx(expected_net, abs=1e-12)

    # Live/sim parity for dutching-specific realized settlement result.
    assert sim["gross_pnl"] == pytest.approx(live["gross_pnl"], abs=1e-12)
    assert sim["commission_amount"] == pytest.approx(live["commission_amount"], abs=1e-12)
    assert sim["net_pnl"] == pytest.approx(live["net_pnl"], abs=1e-12)
    assert sim["commission_pct"] == pytest.approx(live["commission_pct"], abs=1e-12)
    assert sim["market_net_gross"] == pytest.approx(live["market_net_gross"], abs=1e-12)
    assert sim["market_commission_amount_total"] == pytest.approx(
        live["market_commission_amount_total"],
        abs=1e-12,
    )


@pytest.mark.integration
def test_dutching_realized_settlement_unprofitable_book_is_honest_and_commission_free():
    odds = [1.9, 1.9]
    total_stake = 100.0
    commission_pct = 4.5
    winner_idx = 0

    preview = calculate_dutching_stakes(odds, total_stake, commission=commission_pct)
    stakes = preview["stakes"]
    profits = preview["profits"]

    assert abs(sum(stakes) - total_stake) < 0.01
    assert preview["book_pct"] > 100.0
    assert max(preview["net_profits"]) < 0.0

    winner_gross_leg = stakes[winner_idx] * (odds[winner_idx] - 1.0)
    losers_gross_legs = [-stakes[i] for i in range(len(stakes)) if i != winner_idx]
    realized_market_gross = winner_gross_leg + sum(losers_gross_legs)
    assert realized_market_gross == pytest.approx(profits[winner_idx], abs=0.02)
    assert realized_market_gross < 0.0

    sim_broker = SimulationBroker(starting_balance=1000.0, commission_pct=commission_pct)
    sim = sim_broker.record_realized_settlement(realized_market_gross, market_id="1.DUTCH-LOSS")

    assert sim["settlement_basis"] == "market_net_realized"
    assert sim["commission_pct"] == commission_pct
    assert sim["commission_amount"] == 0.0
    assert sim["net_pnl"] == pytest.approx(realized_market_gross, abs=1e-12)


@pytest.mark.integration
def test_controller_preview_to_settlement_linkage_is_explicit_for_one_winner_many_losers():
    controller = DutchingController(bus=None, runtime_controller=_Runtime())

    scenarios = [
        {
            "market_id": "1.DUTCH-PROFIT",
            "odds": [3.0, 4.0, 6.0],
            "total_stake": 120.0,
            "winner_idx": 0,
        },
        {
            "market_id": "1.DUTCH-LOSS",
            "odds": [1.9, 1.9],
            "total_stake": 100.0,
            "winner_idx": 0,
        },
    ]

    for case in scenarios:
        payload = _controller_payload(
            odds=case["odds"],
            total_stake=case["total_stake"],
            commission=4.5,
        )
        payload["market_id"] = case["market_id"]

        preview = controller.preview(payload)
        assert preview["ok"] is True
        rows = preview["results"]
        winner_idx = case["winner_idx"]

        stakes = [float(r["stake"]) for r in rows]
        odds = [float(r["price"]) for r in rows]
        winner_gross_leg = stakes[winner_idx] * (odds[winner_idx] - 1.0)
        losers_gross_legs = [-stakes[i] for i in range(len(stakes)) if i != winner_idx]
        realized_market_gross = winner_gross_leg + sum(losers_gross_legs)

        expected_preview_gross = float(rows[winner_idx]["profitIfWins"])
        expected_preview_net = float(rows[winner_idx]["profitIfWinsNet"])
        assert realized_market_gross == pytest.approx(expected_preview_gross, abs=0.02)

        live_agg = MarketNetRealizedSettlementAggregator(
            commission_pct=4.5,
            context="test_preview_to_settlement_linkage",
        )
        live = live_agg.apply(
            market_id=case["market_id"],
            gross_pnl=realized_market_gross,
        )
        sim_broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
        sim = sim_broker.record_realized_settlement(
            realized_market_gross,
            market_id=case["market_id"],
        )

        assert sim["gross_pnl"] == pytest.approx(realized_market_gross, abs=1e-12)
        assert sim["gross_pnl"] == pytest.approx(expected_preview_gross, abs=0.02)
        assert sim["net_pnl"] == pytest.approx(expected_preview_net, abs=0.02)
        if realized_market_gross > 0.0:
            assert sim["commission_amount"] == pytest.approx(realized_market_gross * 0.045, abs=1e-12)
        else:
            assert sim["commission_amount"] == 0.0

        assert sim["gross_pnl"] == pytest.approx(live["gross_pnl"], abs=1e-12)
        assert sim["commission_amount"] == pytest.approx(live["commission_amount"], abs=1e-12)
        assert sim["net_pnl"] == pytest.approx(live["net_pnl"], abs=1e-12)
