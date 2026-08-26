"""#320-A / M-06 — `TableManager.total_exposure()` non deve mai essere DEFLAZIONATA
da un `current_exposure` negativo.

`total_exposure()` alimenta il cap assoluto in euro `max_open_exposure`
(`core/runtime_controller.py`, Enforcement A2 #320:
`projected_total = total_exposure() + stake; if projected_total > cap: blocca`).
Se un tavolo avesse `current_exposure` negativo, la somma verrebbe abbassata e la
capacità residua gonfiata: un ordine passerebbe **oltre** il cap € (bypass del
gate di sicurezza). Il fix clampa il contributo per-tavolo a `>= 0` (fail-closed:
sovra-stima al più, mai sotto-stima l'esposizione aperta).

BLOCK: sul codice pre-fix questi test falliscono (la somma include il negativo);
col clamp passano. Nessuna sovra-redazione dei casi tutto-positivo.
"""

from __future__ import annotations

import pytest

from core.table_manager import TableManager


def _activate(tm: TableManager, table_id: int, exposure: float) -> None:
    tm.activate(
        table_id=table_id,
        event_key=f"E{table_id}",
        exposure=exposure,
        market_id=f"1.{table_id}",
        selection_id=str(table_id),
        meta={},
    )


@pytest.mark.unit
@pytest.mark.guardrail
def test_total_exposure_clamps_negative_component():
    # Un tavolo con esposizione negativa NON deve ridurre il totale.
    tm = TableManager(table_count=2)
    _activate(tm, 1, 100.0)
    _activate(tm, 2, -50.0)
    # Esposizione aperta reale = 100 (il -50 non "libera" capacità).
    assert tm.total_exposure() == 100.0


@pytest.mark.unit
@pytest.mark.guardrail
def test_total_exposure_unchanged_for_all_positive():
    # Nessuna sovra-clampatura: coi contributi positivi la somma è invariata.
    tm = TableManager(table_count=3)
    _activate(tm, 1, 40.0)
    _activate(tm, 2, 35.5)
    _activate(tm, 3, 0.0)
    assert tm.total_exposure() == pytest.approx(75.5)


@pytest.mark.unit
@pytest.mark.guardrail
def test_negative_exposure_cannot_bypass_eur_cap():
    # Replica l'inequazione del gate A2 (#320) di runtime_controller:
    #   projected_total = total_exposure() + stake ; blocca se > cap.
    # Con un tavolo a esposizione negativa, la somma non-clampata farebbe passare
    # un ordine che supera il cap reale -> qui verifichiamo che il clamp lo blocchi.
    tm = TableManager(table_count=2)
    _activate(tm, 1, 100.0)
    _activate(tm, 2, -50.0)

    cap = 130.0
    stake = 60.0
    projected_total = tm.total_exposure() + stake
    # Esposizione aperta reale (100) + stake (60) = 160 > 130 => DEVE bloccare.
    # Pre-fix: total_exposure()=50 -> projected 110 <= 130 -> passava (bypass).
    assert projected_total > cap, (
        f"bypass del cap €: projected={projected_total} <= cap={cap} "
        "(esposizione negativa ha gonfiato la capacità residua)"
    )
