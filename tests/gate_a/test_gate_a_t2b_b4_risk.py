"""Gate A T2b batch b4 — risk: core.risk_gate.RiskGate (config SimpleNamespace, nessun I/O)."""
from __future__ import annotations

import types

from core.risk_gate import RiskGate


def _cfg(**over):
    base = dict(MIN_STAKE=0.10, MIN_PRICE=1.02, MAX_WIN=None, BOOK_BLOCK=110.0, LIQUIDITY_MULTIPLIER=3.0,
                MIN_LIQUIDITY_ABSOLUTE=50.0, LIQUIDITY_GUARD_ENABLED=True, LIQUIDITY_WARNING_ONLY=True)
    base.update(over)
    return types.SimpleNamespace(**base)


def _p(**over):
    base = {"stake": 5.0, "price": 2.0, "bet_type": "BACK"}
    base.update(over)
    return base


def _reason(gate, payload):
    out = gate.check(payload)
    return None if out["allowed"] else out["reason"]


def test_trg_628_b_0763():
    """ID: TRG-628-B-0763 / PF-B1-RISK-STAKE

    Stake non finito (NaN/inf/testo/bool/assente) => RISK_STAKE_NOT_FINITE; <=0 => NOT_POSITIVE; sotto il
    MIN_STAKE configurato => RISK_BELOW_MIN_STAKE. Lo stake esatto pari al minimo (0.10, Microstake) passa e
    non viene arrotondato ne' ridimensionato (PF-EXEC-04)."""
    gate = RiskGate(_cfg())
    for bad in (float("nan"), float("inf"), "abc", True, None):
        assert _reason(gate, _p(stake=bad)) == "RISK_STAKE_NOT_FINITE", bad
    assert _reason(gate, {"price": 2.0, "bet_type": "BACK"}) == "RISK_STAKE_NOT_FINITE"
    assert _reason(gate, _p(stake=0)) == "RISK_STAKE_NOT_POSITIVE"
    assert _reason(gate, _p(stake=-1)) == "RISK_STAKE_NOT_POSITIVE"
    assert _reason(gate, _p(stake=0.09)) == "RISK_BELOW_MIN_STAKE"
    out = gate.check(_p(stake=0.10))
    assert out["allowed"] is True and out["payload"]["stake"] == 0.10


def test_trg_628_b_0764():
    """ID: TRG-628-B-0764 / PF-B1-RISK-PRICE

    Prezzo non finito/assente => RISK_PRICE_NOT_FINITE; sotto MIN_PRICE => RISK_PRICE_BELOW_MIN; al minimo passa."""
    gate = RiskGate(_cfg())
    for bad in (float("nan"), float("-inf"), "x", None, False):
        assert _reason(gate, _p(price=bad)) == "RISK_PRICE_NOT_FINITE", bad
    assert _reason(gate, _p(price=1.01)) == "RISK_PRICE_BELOW_MIN"
    assert _reason(gate, _p(price=1.02)) is None


def test_trg_628_b_0768():
    """ID: TRG-628-B-0768 / PF-B1-RISK-LIQUIDITY

    Soglia = max(stake * LIQUIDITY_MULTIPLIER, MIN_LIQUIDITY_ABSOLUTE). Sotto soglia: con WARNING_ONLY (default
    #383) solo avviso; con WARNING_ONLY=False blocco reale RISK_LIQUIDITY_INSUFFICIENT. Liquidita' illeggibile
    nega sempre, anche in solo-avviso."""
    warn, block = RiskGate(_cfg()), RiskGate(_cfg(LIQUIDITY_WARNING_ONLY=False))
    # stake 20 -> 60 > floor 50
    assert _reason(block, _p(stake=20.0, available_liquidity=60.0)) is None
    assert _reason(block, _p(stake=20.0, available_liquidity=59.99)) == "RISK_LIQUIDITY_INSUFFICIENT"
    # stake 5 -> 15 < floor 50: vale il floor
    assert _reason(block, _p(stake=5.0, available_liquidity=50.0)) is None
    assert _reason(block, _p(stake=5.0, available_liquidity=49.0)) == "RISK_LIQUIDITY_INSUFFICIENT"
    assert _reason(warn, _p(stake=5.0, available_liquidity=49.0)) is None
    assert _reason(warn, _p(available_liquidity=float("nan"))) == "RISK_LIQUIDITY_NOT_FINITE"


def test_trg_628_b_0898():
    """ID: TRG-628-B-0898 / PF-B1-RISK-CONFIG-MIN_STAKE

    MIN_STAKE deve esistere ed essere un numero finito non-bool: assente, None, NaN, inf, bool, testo non
    numerico => RISK_CONFIG_MISSING (mai un default di comodo)."""
    for bad in (None, float("nan"), float("inf"), True, "abc"):
        assert _reason(RiskGate(_cfg(MIN_STAKE=bad)), _p()) == "RISK_CONFIG_MISSING", bad
    cfg = _cfg()
    del cfg.MIN_STAKE
    assert _reason(RiskGate(cfg), _p()) == "RISK_CONFIG_MISSING"
    assert _reason(RiskGate(_cfg(MIN_STAKE=1)), _p()) is None
