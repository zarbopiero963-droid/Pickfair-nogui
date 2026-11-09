"""Priorita' 1 — drawdown Roserpina contro l'oracolo indipendente (runtime REALE, letture del conto come il runtime).

RO-DD-01 (Grok D-1): l'auto-reset al 15 % ribasa il picco del RiskDesk e cancella, ai fini di `max_drawdown_abs`,
          perdite gia' maturate (regola owner «il reset non cancella le perdite»).
RO-DD-02 (Grok D-1b): il picco del RiskDesk segue il DISPONIBILE, non la cassa: dopo un reset con esposizione aperta
          la perdita successiva e' misurata da una base piu' bassa.
RO-DD-03 (Grok D-2): drawdown e desk mode usano il disponibile: l'esposizione aperta conta come perdita (falso
          LOCKDOWN senza alcuna perdita).
RO-DD-04: con i default (auto-reset 15 % < lockdown 20 %) una perdita graduale non raggiunge mai il lockdown
          (conflitto di requisiti, decisione owner).
RO-DD-05: `max_drawdown_hard_stop_pct` e' applicato solo in LIVE (parita' SIM/LIVE, PF-OD12).
I difetti sono xfail STRICT: diventano PASS (e il marcatore va tolto) solo quando il comportamento e' corretto.
"""
from __future__ import annotations

import pytest

from core.money_management import DeskMode
from tests.roserpina_oracle import oracle as O
from tests.roserpina_oracle.harness import build, read


def _cap(c):
    return c.rc.absolute_cap_reason_for_orders([])


# ------------------------------------------------------------------ PASS: cosa il codice fa gia' bene
def test_perdita_realizzata_oltre_il_limite_assoluto_blocca(tmp_path) -> None:
    c = build(tmp_path)
    led = O.Ledger()
    read(c, 100), led.read(100)
    read(c, 89.99), led.read(89.99)
    assert O.stop_reached(led.drawdown_abs(), 10) and _cap(c) == "max_drawdown_abs_active"


def test_perdita_sotto_il_limite_non_blocca(tmp_path) -> None:
    c = build(tmp_path)
    read(c, 100)
    read(c, 90.01)
    assert _cap(c) == ""


def test_auto_reset_scatta_sul_drawdown_di_cassa_e_non_sotto_soglia(tmp_path) -> None:
    c = build(tmp_path, max_drawdown_abs=None)
    read(c, 100)
    read(c, 85.01)                                          # 14,99 % di cassa
    assert c.rc._maybe_auto_reset(0.0) is False
    read(c, 85.0)                                           # 15 % esatto
    assert c.rc._maybe_auto_reset(0.0) is True


# ------------------------------------------------------------------ RED
@pytest.mark.xfail(strict=True, reason="RO-DD-01: l'auto-reset cancella perdite maturate per max_drawdown_abs")
def test_ro_dd_01_reset_non_cancella_la_perdita_ai_fini_del_drawdown_assoluto(tmp_path) -> None:
    c = build(tmp_path)
    led = O.Ledger()
    read(c, 100), led.read(100)
    read(c, 84), led.read(84)                               # perdita REALIZZATA 16 EUR (cassa 84), limite 10
    assert c.rc._maybe_auto_reset(0.0) is True              # 16 % >= 15 %: reset di ciclo
    assert O.stop_reached(led.drawdown_abs(), 10)           # oracolo: la perdita resta
    assert _cap(c) == "max_drawdown_abs_active", "il reset ha cancellato 16 EUR di perdita"


@pytest.mark.xfail(strict=True, reason="RO-DD-01: perdita cumulativa oltre il limite assoluto attraverso piu' reset")
def test_ro_dd_01b_perdita_cumulativa_attraverso_piu_reset(tmp_path) -> None:
    c = build(tmp_path, max_drawdown_abs=20.0)
    led = O.Ledger()
    blocked_at = None
    for cassa in (100, 92, 85, 78.5, 72.25, 66, 61.4):     # perdite graduali, mai 20 EUR in un solo ciclo
        read(c, cassa), led.read(cassa)
        c.rc._maybe_auto_reset(0.0)
        if _cap(c) == "max_drawdown_abs_active" and blocked_at is None:
            blocked_at = cassa
    assert led.drawdown_abs() == O.D("38.6")                # perdita reale cumulativa
    assert blocked_at is not None and blocked_at >= 80, f"limite 20 EUR mai applicato (bloccato a {blocked_at})"


@pytest.mark.xfail(strict=True, reason="RO-DD-02: picco del RiskDesk sul disponibile, non sulla cassa")
def test_ro_dd_02_picco_sulla_cassa_non_sul_disponibile(tmp_path) -> None:
    """Grok DD5: reset con esposizione aperta (cassa 91,07 = disponibile 87,11 + impegnato 3,96), poi perdite fino a
    cassa 77,50 senza esposizione: perdita dal picco di cassa 13,57 >= 10 => blocco; il codice misura 9,61."""
    c = build(tmp_path)
    led = O.Ledger()
    read(c, 91.07), led.read(91.07)                         # picco di cassa (dopo un reset precedente)
    c.rc.risk_desk.reset_recovery_cycle()                   # come l'auto-reset: picco := bankroll corrente
    read(c, 87.11, 3.96)
    c.rc.risk_desk.reset_recovery_cycle()                   # reset con esposizione aperta
    read(c, 77.50), led.read(77.50)
    assert led.drawdown_abs() == O.D("13.57") and O.stop_reached(led.drawdown_abs(), 10)
    assert _cap(c) == "max_drawdown_abs_active", (c.rc.risk_desk.equity_peak, c.rc.risk_desk.bankroll_current)


@pytest.mark.xfail(strict=True, reason="RO-DD-03: l'esposizione aperta conta come perdita (falso LOCKDOWN)")
def test_ro_dd_03_esposizione_aperta_non_e_drawdown(tmp_path) -> None:
    c = build(tmp_path)
    led = O.Ledger()
    read(c, 100), led.read(100)
    read(c, 70, 30), led.read(70, 30)                       # 30 EUR impegnati, nessuna perdita
    assert led.drawdown_abs() == 0
    assert _cap(c) == "", "LOCKDOWN per esposizione aperta senza perdita"
    assert c.rc._desk_mode() != DeskMode.LOCKDOWN


@pytest.mark.xfail(strict=True, reason="RO-DD-04: conflitto di requisiti auto-reset 15% < lockdown 20% (owner)")
def test_ro_dd_04_lockdown_raggiungibile_con_perdita_graduale(tmp_path) -> None:
    c = build(tmp_path, max_drawdown_abs=None, lockdown_drawdown_pct=20.0)
    led = O.Ledger()
    locked = False
    for cassa in (100, 90, 84, 77, 70):                    # perdita cumulativa 30 %
        read(c, cassa), led.read(cassa)
        c.rc._maybe_auto_reset(0.0)
        locked = locked or c.rc._desk_mode() == DeskMode.LOCKDOWN
    assert O.stop_reached(led.drawdown_pct(), 20) and locked, "perdita cumulativa 30 % mai in LOCKDOWN"


def test_ro_dd_05_hard_stop_percentuale_solo_live_evidenza_statica() -> None:
    """RO-DD-05 (parita' SIM/LIVE): il controllo `max_drawdown_hard_stop_pct` all'ammissione e' dentro un
    `if execution_mode == "LIVE"`: in SIM non esiste. Evidenza dal sorgente (riproduzione runtime: NOT_RUN)."""
    import inspect
    from core.runtime_controller import RuntimeController
    src = inspect.getsource(RuntimeController)
    i = src.index("Enforcement A1: Drawdown Hard Stop")
    window = src[i:i + 400]
    assert 'str(self.execution_mode).upper() == "LIVE"' in window and "max_drawdown_hard_stop_pct" in window
