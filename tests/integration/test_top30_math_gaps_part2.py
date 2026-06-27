"""TOP30 matematici — Parte 2: gap di INTEGRAZIONE settlement/commissione/liability.

Test-only sulle superfici reali (zero modifiche di produzione). Copre i residui
dell'audit non coperti da Parte 1 (`test_top30_math_gaps.py`, funzioni pure):
parity settlement LAY, parity commissione live/sim, market-net path-independence,
refund commissione ±X, segregazione ledger per market_id, worst-case liability
LAY, idempotenza equalize, precisione Decimal estrema, contratto settlement-basis.

Superfici:
- `core.pnl_engine.MarketNetRealizedSettlementAggregator.apply` (commissione market-net, ledger per market)
- `simulation_broker.SimulationBroker.record_realized_settlement` (delega all'aggregator)
- `pnl_engine.PnLEngine.calculate_settlement_pnl` (settlement BACK/LAY)
- `dutching.calculate_dutching_stakes` (equalize)
- `core.simulation_state.SimulationState._calc_liability` (liability LAY)
"""

import pytest

from core.pnl_engine import MarketNetRealizedSettlementAggregator
from core.simulation_state import SimulationState
from dutching import calculate_dutching_stakes
from pnl_engine import PnLEngine
from simulation_broker import SimulationBroker

# Il job CI "Integration Tests" gira `pytest tests/integration -m "integration"`:
# senza questo marker i test sarebbero deselezionati dalla lane integrazione.
pytestmark = pytest.mark.integration

COMM = 4.5


def _agg():
    return MarketNetRealizedSettlementAggregator(commission_pct=COMM, context="test")


# =========================================================
# #2 — Settlement LAY dedicato (la parity esistente è BACK-only)
# =========================================================
def test_settlement_pnl_lay_win_charges_commission_on_winnings():
    e = PnLEngine(commission_pct=COMM)
    r = e.calculate_settlement_pnl(side="LAY", price=3.0, size=10.0, won=True)
    # LAY win => gross = size = 10; commissione 4.5% solo sulla vincita.
    assert r["gross_pnl"] == pytest.approx(10.0)
    assert r["commission_amount"] == pytest.approx(0.45)
    assert r["net_pnl"] == pytest.approx(9.55)


def test_settlement_pnl_lay_lose_has_no_commission():
    e = PnLEngine(commission_pct=COMM)
    r = e.calculate_settlement_pnl(side="LAY", price=3.0, size=10.0, won=False)
    # LAY lose => gross = -(size*(price-1)) = -20; nessuna commissione su perdita.
    assert r["gross_pnl"] == pytest.approx(-20.0)
    assert r["commission_amount"] == pytest.approx(0.0)
    assert r["net_pnl"] == pytest.approx(-20.0)


def test_settlement_back_vs_lay_are_mirror_at_same_price():
    e = PnLEngine(commission_pct=COMM)
    back = e.calculate_settlement_pnl(side="BACK", price=3.0, size=10.0, won=True)
    lay = e.calculate_settlement_pnl(side="LAY", price=3.0, size=10.0, won=False)
    # BACK win (+20) e LAY lose (-20) sono speculari in gross allo stesso prezzo.
    assert back["gross_pnl"] == pytest.approx(-lay["gross_pnl"])


# =========================================================
# #1 — Parity commissione live (aggregator) vs sim (broker)
# =========================================================
def test_live_sim_commission_parity_multi_leg_same_market():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=COMM)
    agg = _agg()
    for gross in (100.0, -40.0, 25.0):
        rb = broker.record_realized_settlement(gross_pnl=gross, market_id="1.M")
        ra = agg.apply(market_id="1.M", gross_pnl=gross)
        assert rb["commission_amount"] == pytest.approx(ra["commission_amount"])
        assert rb["net_pnl"] == pytest.approx(ra["net_pnl"])
        assert rb["market_commission_amount_total"] == pytest.approx(
            ra["market_commission_amount_total"]
        )


# =========================================================
# #4 (riformulato) — commissione market-net path-independent
# (split vs combinata → stesso totale; no doppia commissione per-leg)
# =========================================================
def test_market_net_commission_is_path_independent():
    split = _agg()
    c1 = float(split.apply(market_id="1.M", gross_pnl=100.0)["commission_amount"])
    c2 = float(split.apply(market_id="1.M", gross_pnl=-40.0)["commission_amount"])
    combined = float(_agg().apply(market_id="1.M", gross_pnl=60.0)["commission_amount"])
    # +100 poi -40 (net +60) deve dare la stessa commissione TOTALE di un +60 unico.
    assert (c1 + c2) == pytest.approx(combined) == pytest.approx(2.7)
    assert split.ledger["1.M"]["gross"] == pytest.approx(60.0)
    assert split.ledger["1.M"]["commission"] == pytest.approx(2.7)


# =========================================================
# #7 — refund commissione esatto su +X/−X + precisione Decimal estrema
# =========================================================
def test_commission_fully_refunded_on_full_reversal():
    agg = _agg()
    c_pos = float(agg.apply(market_id="1.M", gross_pnl=100.0)["commission_amount"])
    c_neg = float(agg.apply(market_id="1.M", gross_pnl=-100.0)["commission_amount"])
    # market-net torna a 0 => commissione totale 0 (il -100 rimborsa il +4.5).
    assert c_pos == pytest.approx(4.5)
    assert c_neg == pytest.approx(-4.5)
    assert (c_pos + c_neg) == pytest.approx(0.0)
    assert agg.ledger["1.M"] == {"gross": pytest.approx(0.0), "commission": pytest.approx(0.0)}


@pytest.mark.parametrize("gross", [0.001, 1_000_000.0, 0.04, 12_345.67])
def test_commission_exact_on_extreme_positive_gross(gross):
    r = _agg().apply(market_id="1.M", gross_pnl=gross)
    assert r["commission_amount"] == pytest.approx(gross * COMM / 100.0)
    assert r["net_pnl"] == pytest.approx(gross - gross * COMM / 100.0)


# =========================================================
# #9 — segregazione ledger commissioni per market_id (no crosstalk)
# =========================================================
def test_commission_ledger_segregated_per_market_no_crosstalk():
    agg = _agg()
    agg.apply(market_id="1.A", gross_pnl=100.0)
    agg.apply(market_id="1.B", gross_pnl=-50.0)
    agg.apply(market_id="1.A", gross_pnl=20.0)
    # 1.A: gross 120, comm 5.4; 1.B: gross -50, comm 0 — isolati.
    assert agg.ledger["1.A"]["gross"] == pytest.approx(120.0)
    assert agg.ledger["1.A"]["commission"] == pytest.approx(5.4)
    assert agg.ledger["1.B"]["gross"] == pytest.approx(-50.0)
    assert agg.ledger["1.B"]["commission"] == pytest.approx(0.0)


# =========================================================
# #3 — worst-case liability LAY mai sottostimata
# =========================================================
def test_lay_liability_never_understated_and_monotonic():
    st = SimulationState(starting_balance=1000.0, commission_pct=COMM)
    # liability LAY = size*(price-1), mai negativa, monotona crescente nel prezzo.
    assert st._calc_liability("LAY", 3.0, 10.0) == pytest.approx(20.0)
    assert st._calc_liability("LAY", 100.0, 10.0) == pytest.approx(990.0)  # odds estreme
    prev = -1.0
    for price in (1.5, 2.0, 5.0, 20.0, 100.0):
        liab = st._calc_liability("LAY", price, 10.0)
        assert liab >= 0.0
        assert liab >= prev  # monotona non decrescente
        assert liab == pytest.approx(10.0 * (price - 1.0))
        prev = liab


def test_back_liability_is_capped_at_stake():
    st = SimulationState(starting_balance=1000.0, commission_pct=COMM)
    # BACK: la liability è limitata allo stake, indipendente dalla quota.
    assert st._calc_liability("BACK", 100.0, 10.0) == pytest.approx(10.0)


# =========================================================
# #6 — idempotenza equalize (equalize 2× → nessun drift)
# =========================================================
def test_equalize_is_deterministic_and_reaches_equal_profit_fixed_point():
    kw = dict(commission=COMM, equalize=True, commission_aware=True)
    r1 = calculate_dutching_stakes([3.0, 4.0, 6.0], 100.0, **kw)
    r2 = calculate_dutching_stakes([3.0, 4.0, 6.0], 100.0, **kw)
    # (a) Deterministico: nessun drift fra due equalizzazioni.
    assert r1["stakes"] == r2["stakes"]
    assert r1["net_profits"] == r2["net_profits"]
    # (b) Punto fisso equal-profit: l'equalize ha reso i net profit uniformi
    #     (entro il residuo di rounding) — non si limita a ripetersi.
    net = r1["net_profits"]
    assert max(net) - min(net) <= 0.05
    # (c) budget preservato.
    assert sum(r1["stakes"]) == pytest.approx(100.0, abs=0.01)


# =========================================================
# #8/#10 — contratto settlement-basis (sempre market_net_realized)
# =========================================================
def test_settlement_basis_contract_is_market_net_realized():
    agg_res = _agg().apply(market_id="1.M", gross_pnl=10.0)
    assert agg_res["settlement_basis"] == "market_net_realized"
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=COMM)
    broker_res = broker.record_realized_settlement(gross_pnl=10.0, market_id="1.M")
    assert broker_res["settlement_basis"] == "market_net_realized"
