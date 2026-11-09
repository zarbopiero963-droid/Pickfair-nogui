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
    blocked_at = expected_at = None
    for cassa in (100, 92, 85, 78.5, 72.25, 66, 61.4):     # perdite graduali, mai 20 EUR in un solo ciclo
        read(c, cassa), led.read(cassa)
        if expected_at is None and O.stop_reached(led.drawdown_abs(), 20):
            expected_at = cassa                             # oracolo: primo punto con perdita cumulativa >= 20
        c.rc._maybe_auto_reset(0.0)
        if _cap(c) == "max_drawdown_abs_active" and blocked_at is None:
            blocked_at = cassa
    assert led.drawdown_abs() == O.D("38.6") and expected_at == 78.5   # perdita reale cumulativa
    assert blocked_at == expected_at, f"limite 20 EUR: atteso blocco a {expected_at}, bloccato a {blocked_at}"


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


@pytest.mark.xfail(strict=True, reason="RO-DD-04 (decisione owner PF-ROSERPINA-FINANCIAL-CONTRACT-FINAL-20261109): "
                                       "lockdown sul drawdown cumulativo dal picco persistente")
def test_ro_dd_04_lockdown_raggiungibile_con_perdita_graduale(tmp_path) -> None:
    c = build(tmp_path, max_drawdown_abs=None, lockdown_drawdown_pct=20.0)
    led = O.Ledger()
    locked = False
    for cassa in (100, 90, 84, 77, 70):                    # perdita cumulativa 30 %
        read(c, cassa), led.read(cassa)
        c.rc._maybe_auto_reset(0.0)
        locked = locked or c.rc._desk_mode() == DeskMode.LOCKDOWN
    assert O.stop_reached(led.drawdown_pct(), 20) and locked, "perdita cumulativa 30 % mai in LOCKDOWN"


def _signal(c, market, price=2.0):
    c.broker.update_market_book(market, {"marketId": market, "status": "OPEN", "inplay": False, "runners": [
        {"selectionId": 11, "status": "ACTIVE", "ex": {"availableToBack": [{"price": price, "size": 500.0}],
                                                       "availableToLay": [{"price": price + 0.02, "size": 500.0}]}}]})
    before = len(c.broker.get_current_orders(None) or [])
    c.rc._on_signal_received({"market_id": market, "selection_id": 11, "price": price, "bet_type": "BACK",
                              "simulation_mode": True, "chat_id": -100123, "event": f"Ev {market}",
                              "market": "Match Odds", "selection": "S11"})
    return len(c.broker.get_current_orders(None) or []) > before


def test_ro_dd_05_controllo_ingresso_ammesso_senza_perdita(tmp_path) -> None:
    """Controllo dell'harness: stesso percorso, nessuna perdita => l'ingresso passa (il test RED sotto e' significativo)."""
    c = build(tmp_path, max_drawdown_abs=None, max_drawdown_hard_stop_pct=10.0)
    read(c, 100)
    assert _signal(c, "1.501") is True


@pytest.mark.xfail(strict=True, reason="RO-DD-05: max_drawdown_hard_stop_pct applicato solo in LIVE (parita' SIM/LIVE)")
def test_ro_dd_05_hard_stop_percentuale_anche_in_sim(tmp_path) -> None:
    """Parita' (PF-OD12-12, contratto regola 5): in SIM, picco 100 e cassa 89 (11 % >= 10 %) => nessun ingresso."""
    c = build(tmp_path, max_drawdown_abs=None, max_drawdown_hard_stop_pct=10.0)
    assert str(c.rc.execution_mode).upper() != "LIVE"
    read(c, 100)
    read(c, 89)
    assert _signal(c, "1.502") is False, "ingresso ammesso in SIM oltre l'hard stop percentuale"


# ------------------------------------------------------------------ contratto owner PF-ROSERPINA-DRAWDOWN-AND-RECOVERY-20261109
# drawdown_abs = max(0, verified_cash_peak - verified_cash_current); picco persistente (crash/restart), mai azzerato
# dall'auto-reset; riserve/esposizione non sono perdite; blocco alla variazione finanziaria, mantenuto fino a uno sblocco
# esplicito; limite opzionale (None = disattivo); Decimal esatto.
def test_contratto_esempio_owner_picco_120_cassa_107_blocca(tmp_path) -> None:
    c = build(tmp_path)
    led = O.Ledger()
    for cassa in (100, 120, 107):
        read(c, cassa), led.read(cassa)
    assert led.drawdown_abs() == O.D("13") and O.stop_reached(led.drawdown_abs(), 10)
    assert _cap(c) == "max_drawdown_abs_active"


@pytest.mark.xfail(strict=True, reason="RO-DD-03 (contratto regola 3): la riserva non e' una perdita")
def test_contratto_esempio_owner_con_riserva_aperta_non_blocca(tmp_path) -> None:
    """Picco 120, cassa verificata 115 = disponibile 105 + impegnato 10: drawdown 5 < 10 => nessun blocco."""
    c = build(tmp_path)
    led = O.Ledger()
    read(c, 100), led.read(100)
    read(c, 120), led.read(120)
    read(c, 105, 10), led.read(105, 10)
    assert led.drawdown_abs() == O.D("5") and not O.stop_reached(led.drawdown_abs(), 10)
    assert _cap(c) == "", (c.rc.risk_desk.equity_peak, c.rc.risk_desk.bankroll_current)


def test_contratto_limite_non_configurato_nessun_cap(tmp_path) -> None:
    c = build(tmp_path, max_drawdown_abs=None)
    read(c, 100)
    read(c, 40)
    assert _cap(c) == ""                                              # nessun cap monetario predefinito


def test_contratto_cassa_non_verificabile_nessuna_cassa_valida(tmp_path) -> None:
    """Regola 6: lettura non valida => cassa non verificata (il MM nega i nuovi ingressi con `cassa_non_verificata`)."""
    c = build(tmp_path)
    read(c, 100)
    c.rc._apply_funds_reading({"available": None, "exposure": 0.0, "ok": False, "error": "timeout"}, source="ORACLE")
    assert c.rc.risk_desk.cash_basis()[0] is None


@pytest.mark.xfail(strict=True, reason="RO-DD-07 (contratto): blocco alla variazione finanziaria, non solo al segnale")
def test_ro_dd_07_blocco_alla_variazione_finanziaria_senza_segnale(tmp_path) -> None:
    """Una lettura del conto che porta il drawdown oltre il limite deve mettere il runtime in blocco SUBITO; oggi il
    controllo avviene solo all'ammissione del segnale successivo."""
    from core.system_state import RuntimeMode
    c = build(tmp_path)
    read(c, 100)
    read(c, 88)                                                      # perdita regolata 12 >= 10
    assert c.rc.mode == RuntimeMode.LOCKDOWN, c.rc.mode


def test_contratto_lettura_non_verificata_non_alza_mai_il_picco(tmp_path) -> None:
    """Cautela owner: il picco persistente non si inizializza ne' si alza da una cassa NON verificata. Una lettura in
    errore che dichiara 500 EUR non deve diventare il picco: dopo una lettura verificata a 95 il drawdown e' 5, non 405."""
    c = build(tmp_path)
    read(c, 100)
    c.rc._record_account_snapshot({"available": 500.0, "exposure": 0.0, "ok": False, "error": "timeout"},
                                  source="ORACLE")
    c.rc._apply_funds_reading({"available": 500.0, "exposure": 0.0, "ok": False, "error": "timeout"},
                              source="ORACLE")
    read(c, 95)
    assert c.rc.risk_desk.equity_peak == 100.0 and c.rc._cash_equity_peak == 100.0
    assert _cap(c) == ""
