"""Matrice fail-closed del gate LIVE (PR-A programma test hedge-fund grade).

Proprietà da proteggere: l'UNICA combinazione che abilita LIVE è
(execution_mode="LIVE", live_enabled veritiero, readiness veritiera,
kill_switch falso). Qualsiasi altro input — mancante, ambiguo, sporco,
contraddittorio — deve degradare a SIMULATION con reason esplicita.
"""
from __future__ import annotations

import pytest

from core.safety_layer import assert_live_gate_or_refuse

_GOLDEN = dict(
    execution_mode="LIVE",
    live_enabled=True,
    live_readiness_ok=True,
    kill_switch=False,
)


@pytest.mark.safety
@pytest.mark.invariant
def test_golden_combination_is_the_only_live_path():
    decision = assert_live_gate_or_refuse(**_GOLDEN)
    assert decision.allowed is True
    assert decision.effective_execution_mode == "LIVE"
    assert decision.reason_code == "live_allowed"


@pytest.mark.safety
@pytest.mark.invariant
@pytest.mark.parametrize("mode", [
    None, "", "   ", "SIMULATION", "simulation", "Live ", " LIVE",
    "PROD", "TRUE", 1, 0, True, False, [], {}, object(),
])
def test_non_live_or_dirty_execution_mode_forces_simulation(mode):
    decision = assert_live_gate_or_refuse(
        execution_mode=mode,
        live_enabled=True,
        live_readiness_ok=True,
        kill_switch=False,
    )
    # "Live "/" LIVE" sono accettati solo se la normalizzazione li mappa
    # esattamente a LIVE; tutto il resto degrada a SIMULATION.
    normalized = str(mode or "").strip().upper()
    if normalized == "LIVE":
        assert decision.allowed is True
    else:
        assert decision.allowed is False
        assert decision.effective_execution_mode == "SIMULATION"
        assert decision.reason_code in {
            "simulation_mode_forced",
            "invalid_or_missing_execution_mode",
        }


@pytest.mark.safety
@pytest.mark.invariant
@pytest.mark.parametrize("live_enabled", [False, None, 0, "", [], {}])
def test_live_request_without_enabled_gate_is_blocked(live_enabled):
    decision = assert_live_gate_or_refuse(
        execution_mode="LIVE",
        live_enabled=live_enabled,
        live_readiness_ok=True,
        kill_switch=False,
    )
    assert decision.allowed is False
    assert decision.effective_execution_mode == "SIMULATION"
    assert decision.reason_code == "live_not_enabled"


@pytest.mark.safety
@pytest.mark.invariant
@pytest.mark.parametrize("readiness", [False, None, 0, "", [], {}])
def test_live_request_without_readiness_is_blocked(readiness):
    decision = assert_live_gate_or_refuse(
        execution_mode="LIVE",
        live_enabled=True,
        live_readiness_ok=readiness,
        kill_switch=False,
    )
    assert decision.allowed is False
    assert decision.effective_execution_mode == "SIMULATION"
    assert decision.reason_code == "live_readiness_not_ok"


@pytest.mark.safety
@pytest.mark.invariant
@pytest.mark.parametrize("kill_switch", [True, 1, "yes", "true", [1], {"on": 1}])
def test_kill_switch_beats_everything(kill_switch):
    # Anche con la combinazione d'oro, il kill switch vince sempre.
    decision = assert_live_gate_or_refuse(
        execution_mode="LIVE",
        live_enabled=True,
        live_readiness_ok=True,
        kill_switch=kill_switch,
    )
    assert decision.allowed is False
    assert decision.effective_execution_mode == "SIMULATION"
    assert decision.reason_code == "kill_switch_active"


@pytest.mark.safety
@pytest.mark.invariant
def test_kill_switch_blocks_even_simulation_requests_with_explicit_reason():
    decision = assert_live_gate_or_refuse(
        execution_mode="SIMULATION",
        live_enabled=False,
        live_readiness_ok=False,
        kill_switch=True,
    )
    assert decision.allowed is False
    assert decision.reason_code == "kill_switch_active"


@pytest.mark.safety
@pytest.mark.invariant
def test_exhaustive_adversarial_matrix_allows_live_only_on_golden_combo():
    """Forza l'intera matrice avversaria: allowed=True implica combo d'oro."""
    modes = ["LIVE", "SIMULATION", None, "", "garbage", 0]
    flags = [True, False, None, 0, 1, ""]

    allowed_combos = []
    for mode in modes:
        for enabled in flags:
            for readiness in flags:
                for kill in flags:
                    decision = assert_live_gate_or_refuse(
                        execution_mode=mode,
                        live_enabled=enabled,
                        live_readiness_ok=readiness,
                        kill_switch=kill,
                    )
                    # Invariante di coerenza: mai stati contraddittori.
                    assert decision.allowed == (
                        decision.effective_execution_mode == "LIVE"
                    )
                    assert decision.reason_code, "reason_code sempre presente"
                    if decision.allowed:
                        allowed_combos.append((mode, enabled, readiness, kill))

    for mode, enabled, readiness, kill in allowed_combos:
        assert str(mode).strip().upper() == "LIVE"
        assert bool(enabled) is True
        assert bool(readiness) is True
        assert bool(kill) is False
