"""TOP30 matematici (test_suite_top30_math): gap a funzione pura.

Phase 0 dedup: l'audit matematico ha gia' verificato STRONG il core
(commissione 4.5%, dutching BACK/LAY equal-profit, hedge/green-up,
settlement market-net). Sono GIA' coperti: equal-profit 2/3/N, budget,
no stake negativi, invalid odds fail-closed, cache, rounding bounded,
stress N<=8, PnL finite, weighted-average multi-fill, liability LAY
floor, commissione per-market. Qui SOLO i gap a funzione pura
netto-nuovi (test-only, superfici reali in dutching.py):

1. order-independence: i risultati dutching dipendono solo dal VALORE
   delle quote, non dalla loro posizione nella lista.
2. stress N grande (20/50/100): stake finiti, non negativi, budget
   preservato, equal-profit entro tolleranza (l'audit testava N<=8).
3. break-even senza flip di segno: il green-up del cashout attraversa
   lo zero ESATTAMENTE a current==matched, con un solo cambio di segno.
4. caratterizzazione tick-ladder: dutching/cashout arrotonda gli stake
   ai CENTESIMI (preview-only) e NON snappa alla tick ladder Betfair.
   Confine documentato perche' una eventuale regressione si veda.

I gap di INTEGRAZIONE (refund commissione esatto +X/-X, idempotenza
settlement retry, parity settlement LAY dedicata) restano follow-up:
richiedono le fixture del simulation_broker.
"""
from __future__ import annotations

import math

import pytest

from dutching import (
    _d,
    _round_step,
    calculate_dutching_stakes,
    dynamic_cashout_single,
)

# ---------------------------------------------------------------------------
# 1. ORDER INDEPENDENCE
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.guardrail
def test_dutching_stakes_are_order_independent_up_to_one_cent_residual():
    """Comportamento REALE caratterizzato (probe Phase 0): lo stake per
    quota e' order-independent **a meno del residuo di rounding di 1
    centesimo**, la cui assegnazione puo' dipendere dalla posizione in
    lista. Invarianti forti che DEVONO valere a prescindere dall'ordine:
    (a) budget totale identico al centesimo; (b) stake per-quota uguale
    entro 1 cent; (c) net-profit equalizzati (equal-profit) preservati.

    NOTA: non e' un bug che perde soldi (budget e equal-profit
    preservati, deterministico per-ordine — vedi stability test 2000x);
    e' un micro-residuo di rounding. Renderlo strettamente
    order-independent toccherebbe dutching.py (file critico): rimandato,
    e questo test blocca eventuali peggioramenti (residuo > 1 cent)."""
    odds = [1.8, 3.5, 6.0, 2.2]
    total = 100.0
    base = calculate_dutching_stakes(odds=odds, total_stake=total, commission=4.5)
    assert not base.get("error")
    base_map = dict(zip([float(o) for o in odds], [float(s) for s in base["stakes"]]))
    base_total = sum(float(s) for s in base["stakes"])

    for perm in ([6.0, 1.8, 2.2, 3.5], list(reversed(odds))):
        res = calculate_dutching_stakes(odds=perm, total_stake=total, commission=4.5)
        assert not res.get("error")
        perm_map = dict(zip([float(o) for o in perm], [float(s) for s in res["stakes"]]))
        # (a) budget totale identico al centesimo, qualunque sia l'ordine
        assert sum(float(s) for s in res["stakes"]) == pytest.approx(base_total, abs=0.011)
        # (b) stake per-quota entro un centesimo (residuo di rounding)
        for odd, stake in base_map.items():
            assert perm_map[odd] == pytest.approx(stake, abs=0.011), f"quota {odd}"
        # (c) equal-profit preservato: spread net-profit limitato
        net = [float(p) for p in res["net_profits"]]
        assert max(net) - min(net) <= 0.05


# ---------------------------------------------------------------------------
# 2. STRESS N GRANDE (20/50/100)
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.parametrize("n", [20, 50, 100])
def test_dutching_stress_large_n_is_finite_bounded_and_equal_profit(n):
    """Oltre N=8 (max dell'audit): stake finiti, non negativi, budget
    preservato e net-profit equalizzati entro la tolleranza di rounding."""
    odds = [round(1.5 + i * 0.1, 2) for i in range(n)]  # quote distinte
    total = 1000.0
    res = calculate_dutching_stakes(odds=odds, total_stake=total, commission=4.5)
    assert not res.get("error")
    stakes = [float(s) for s in res["stakes"]]
    net = [float(p) for p in res["net_profits"]]
    assert len(stakes) == n
    assert all(math.isfinite(s) and s >= 0.0 for s in stakes)
    assert all(math.isfinite(p) for p in net)
    # budget preservato entro il rounding ai centesimi accumulato su N
    assert sum(stakes) == pytest.approx(total, abs=max(0.5, n * 0.01))
    # equal-profit: spread dei net-profit limitato (post-rounding)
    assert max(net) - min(net) <= max(0.5, n * 0.02)


# ---------------------------------------------------------------------------
# 3. BREAK-EVEN SENZA FLIP DI SEGNO
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.parametrize("side", ["BACK", "LAY"])
def test_cashout_break_even_has_single_sign_crossing(side):
    """A current==matched il green-up grezzo e' ESATTAMENTE 0 (break-even);
    ai due lati (prezzo accorciato vs allargato) ha segni opposti -> un
    solo attraversamento dello zero, nessun flip spurio."""
    ms, mp = 10.0, 4.0
    at_be = dynamic_cashout_single(
        matched_stake=ms, matched_price=mp, current_price=mp, side=side, commission=4.5
    )
    assert at_be["green_up"] == pytest.approx(0.0, abs=0.01)

    shorter = dynamic_cashout_single(
        matched_stake=ms, matched_price=mp, current_price=2.5, side=side, commission=4.5
    )["green_up"]
    longer = dynamic_cashout_single(
        matched_stake=ms, matched_price=mp, current_price=6.0, side=side, commission=4.5
    )["green_up"]
    # segni opposti => un solo crossing, e lo zero (break-even) sta in mezzo
    assert shorter * longer < 0
    assert min(shorter, longer) < 0.0 < max(shorter, longer)


# ---------------------------------------------------------------------------
# 4. CARATTERIZZAZIONE TICK-LADDER (preview-only: centesimi, non tick Betfair)
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_round_step_rounds_to_cents_granularity():
    """_round_step arrotonda al centesimo (granularita' di prezzo/stake
    preview-only), non alla tick ladder Betfair. Documenta il confine."""
    for raw in ("1.234", "1.236", "0.005", "12.999"):
        q = float(_round_step(_d(raw)))
        # valore su griglia da centesimo (multiplo intero di 0.01)
        assert round(q * 100) == pytest.approx(q * 100, abs=1e-9)
        # e resta vicino all'input (entro mezzo centesimo)
        assert abs(q - float(raw)) <= 0.005 + 1e-9


@pytest.mark.unit
def test_cashout_stake_is_cent_rounded_not_tick_snapped():
    res = dynamic_cashout_single(
        matched_stake=10.0, matched_price=3.3, current_price=2.7, side="BACK"
    )
    cs = res["cashout_stake"]
    # 2 decimali esatti: nessuno snap a un tick di prezzo Betfair
    assert round(cs * 100) == pytest.approx(cs * 100, abs=1e-9)
