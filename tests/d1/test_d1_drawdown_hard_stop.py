"""D-1 (P0) e D-3 — hard-stop del drawdown assoluto su cassa verificata con picco persistente; cap esatti in Decimal.

Decisione owner PF-ROSERPINA-D1-D2-B2-B3-FINANCIAL-SAFETY-20261109 (#586 c.6747859283). Difetti riprodotti dalla
campagna Grok a 5 tavoli (#586 c.6747305390, run DD5) e dall'audit su main 6519794b (#586 c.6747780896): ogni test
qui sotto FALLISCE sul main precedente (RED) e passa con la correzione.
Catena reale: RuntimeController + MoneyManagement + SimulationBroker (stub offline), nessun ordine reale.
"""
from __future__ import annotations

import json
import os

import pytest

from core import validators
from core.drawdown_hard_stop import (REASON_ACTIVE, REASON_CASH_UNVERIFIED, REASON_NOT_PERSISTED,
                                     REASON_PCT_ACTIVE, REASON_STATE_UNREADABLE, STATE_KEY, DrawdownHardStop)
from tests.acceptance.test_issue437_pr27 import _catena, _rifiuti
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal

pytestmark = [pytest.mark.safety]


def _lettura(c, available, committed=0.0):
    """Lettura del conto verificata come quella LIVE (available + |exposure|) applicata dal contratto unico. Il
    servizio restituisce da qui in poi gli stessi fondi: la rilettura d'ingresso del segnale vede lo stesso conto."""
    if not getattr(c, "conto_iniziato", False):
        # _catena parte da un desk a 1000 scritto a mano: il conto del test lo sostituisce dalla prima lettura
        c.rc.risk_desk.equity_peak = 0.0
        c.rc.risk_desk.bankroll_start = 0.0
        c.conto_iniziato = True
    fondi = {"available": available, "exposure": -committed, "ok": True}
    c.rc.betfair_service.get_account_funds = lambda: dict(fondi)
    esito = c.rc._apply_funds_reading(dict(fondi), source="test")
    assert esito["applied"], esito


def _ordini(c):
    return len(c.broker.state.orders)


def _bloccato_da_hard_stop(c, reason=None):
    """Nessun ordine e blocco dell'hard-stop: il LOCKDOWN scatta gia' alla lettura del conto (RO-DD-07), quindi il
    segnale successivo puo' essere respinto dal runtime non attivo prima ancora del controllo di drawdown."""
    reason = reason or REASON_ACTIVE
    return (_ordini(c) == 0 and c.rc._hard_stop().breached
            and _rifiuti(c)[-1] in (reason, "runtime_non_attivo:LOCKDOWN"))


# ----------------------------------------------------------------------------------------------- D-1a auto-reset
def test_d1a_auto_reset_does_not_erase_the_loss_of_the_hard_stop(tmp_path):
    """Grok DD5: l'auto-reset (reset del ciclo) ribasava il picco e cancellava 16,86 EUR di perdita."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    _lettura(c, 83.0)                                   # perdita regolata 17 >= 10
    c.rc._reset_recovery_cycle_keeping_losses()         # cio' che fa _maybe_auto_reset
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _bloccato_da_hard_stop(c), _rifiuti(c)


# --------------------------------------------------------------------------------------- D-1b cassa, non disponibile
def test_d1b_peak_is_the_verified_cash_not_the_available_balance(tmp_path):
    """Grok DD5: picco di cassa 91,07 con 3,96 impegnati (disponibile 87,11); ora 77,50 tutto disponibile.
    Perdita reale 13,57 >= 10: prima il codice misurava 9,61 e ammetteva il segnale."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 87.11, committed=3.96)
    _lettura(c, 77.50)
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _bloccato_da_hard_stop(c), _rifiuti(c)
    assert c.rc._hard_stop().loss(77.50) == validators.money_decimal(13.57)


def test_d1b_open_exposure_is_not_a_loss(tmp_path):
    """Riserve e puntate aperte spostano denaro dal disponibile all'impegnato: la cassa non cambia, nessun blocco."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    _lettura(c, 95.0, committed=5.0)                    # 5 esposti, nessuna perdita (sotto le soglie del desk)
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 1
    assert REASON_ACTIVE not in _rifiuti(c)


def test_d1_below_threshold_admits_and_exact_threshold_blocks(tmp_path):
    sotto = _catena(tmp_path / "sotto", max_drawdown_abs=10.0)
    _lettura(sotto, 100.0)
    _lettura(sotto, 90.01)                              # perdita 9,99
    sotto.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(sotto) == 1, _rifiuti(sotto)

    soglia = _catena(tmp_path / "soglia", max_drawdown_abs=10.0)
    _lettura(soglia, 100.0)
    _lettura(soglia, 90.0)                              # perdita esattamente 10
    soglia.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _bloccato_da_hard_stop(soglia), _rifiuti(soglia)


def test_d1_not_configured_never_blocks(tmp_path):
    """Nessuno stop monetario di default (rettifica owner 18/10): senza `max_drawdown_abs` nessun blocco."""
    c = _catena(tmp_path)
    _lettura(c, 100.0)
    _lettura(c, 95.0)
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 1, _rifiuti(c)


# ------------------------------------------------------------------------------------------- D-1c riavvio / crash
def test_d1c_peak_survives_restart(tmp_path):
    """Il picco e' persistito: un nuovo processo sullo stesso DB vede ancora la perdita."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    raw = c.rc.db.get_settings()[STATE_KEY]
    assert json.loads(raw)["peak"] == "100.0"

    riavviato = DrawdownHardStop(c.rc.db)                # stato ricostruito dal DB come all'avvio
    riavviato.restore()
    assert riavviato.reason(10.0, 95.0) == ""           # perdita 5 dal picco persistito 100
    assert riavviato.reason(10.0, 85.0) == REASON_ACTIVE  # perdita 15: bloccato (e memorizzato)


def test_d1c_restart_through_a_new_controller_keeps_blocking(tmp_path):
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    del c.rc._drawdown_hard_stop                         # processo nuovo: nessuno stato in memoria
    _lettura(c, 85.0)                                    # prima lettura dopo il "riavvio"
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _bloccato_da_hard_stop(c), _rifiuti(c)


class _Db:
    def __init__(self, settings=None, fail_save=False, fail_get=False):
        self.settings, self.fail_save, self.fail_get = dict(settings or {}), fail_save, fail_get

    def get_settings(self):
        if self.fail_get:
            raise OSError("db illeggibile")
        return dict(self.settings)

    def save_settings(self, values):
        if self.fail_save:
            raise OSError("disco pieno")
        self.settings.update(values)


@pytest.mark.parametrize("raw", ["{", json.dumps({"peak": "abc", "seq": 1}), json.dumps({"peak": "-5", "seq": 1}),
                                 json.dumps({"peak": "NaN", "seq": 1}), json.dumps({"peak": "100", "seq": True})])
def test_d1_unreadable_state_fails_closed_and_is_never_overwritten(raw):
    stop = DrawdownHardStop(_Db({STATE_KEY: raw}))
    stop.restore()
    assert stop.reason(10.0, 100.0) == REASON_STATE_UNREADABLE
    assert stop._db.settings[STATE_KEY] == raw                # nessuna sovrascrittura di uno stato non letto
    assert stop.reason(None, 100.0) == ""                     # hard-stop non configurato: nessun blocco


def test_d1_db_read_error_fails_closed():
    stop = DrawdownHardStop(_Db(fail_get=True))
    stop.restore()
    assert stop.reason(10.0, 100.0) == REASON_STATE_UNREADABLE


def test_d1_peak_not_saved_fails_closed_then_recovers():
    db = _Db(fail_save=True)
    stop = DrawdownHardStop(db)
    stop.observe(100.0)
    assert stop.reason(10.0, 100.0) == REASON_NOT_PERSISTED
    db.fail_save = False
    assert stop.reason(10.0, 100.0) == ""                     # il salvataggio si ritenta e riesce
    assert json.loads(db.settings[STATE_KEY])["peak"] == "100.0"


@pytest.mark.parametrize("cash", [None, -1, float("nan"), float("inf"), True, "x"])
def test_d1_unverified_cash_fails_closed(cash):
    stop = DrawdownHardStop(_Db())
    stop.observe(100.0)
    assert stop.reason(10.0, cash) == REASON_CASH_UNVERIFIED


def test_d1_unverified_cash_in_the_runtime_rejects_the_signal(tmp_path):
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    c.rc.betfair_service.get_account_funds = lambda: {"error": "lettura fallita"}
    c.rc._apply_funds_reading({"error": "lettura fallita"}, source="test")
    assert c.rc.risk_desk.cash_equity_current is None
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 0 and _rifiuti(c)[-1] == REASON_CASH_UNVERIFIED


def test_d1_peak_only_rises_and_restore_never_lowers_it():
    db = _Db()
    stop = DrawdownHardStop(db)
    stop.observe(100.0)
    stop.observe(80.0)
    assert stop.snapshot()["peak"] == 100.0
    db.settings[STATE_KEY] = json.dumps({"peak": "50", "seq": 9})
    stop.restore()                                            # START ripetuto nello stesso processo
    assert stop.snapshot()["peak"] == 100.0


@pytest.mark.parametrize("cap", [0, -1, float("nan"), float("inf"), True, "dieci"])
def test_d1_invalid_configured_cap_fails_closed(cap):
    stop = DrawdownHardStop(_Db())
    stop.observe(100.0)
    assert stop.reason(cap, 100.0) == "max_drawdown_abs_non_valido"


def test_d1_manual_and_dutching_path_uses_the_same_hard_stop(tmp_path):
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    _lettura(c, 83.0)
    c.rc._reset_recovery_cycle_keeping_losses()
    ordine = {"side": "BACK", "price": 2.0, "stake": 0.5, "market_id": "1.234", "selection_id": 11,
              "event_name": "A v B"}
    assert c.rc.absolute_cap_reason_for_orders([ordine]) == REASON_ACTIVE


# ----------------------------------------------------------------------------------------------- D-3 Decimal
def test_d3_any_excess_over_the_cap_is_rejected():
    assert validators.exceeds_cap(60.0000000001, 60) is True        # prima False (tolleranza 1e-9)
    assert validators.exceeds_cap(60.0, 60) is False
    assert validators.exceeds_cap((0.1, 0.2), 0.3) is False          # termini sommati in Decimal
    assert validators.exceeds_cap((30.0, 30.0000000001), 60) is True
    assert validators.exceeds_cap(float("inf"), 60) is True
    assert validators.exceeds_cap(1.0, float("inf")) is False
    for bad in (float("nan"), True, "x", None):
        assert validators.exceeds_cap(bad, 60) is True               # non valutabile => supera (fail-closed)


def test_d3_lay_liability_is_exact():
    assert validators.order_exposure("LAY", 0.52, 1.92) == 0.4784
    assert repr(validators.order_exposure("LAY", 0.52, 1.92)) == "0.4784"
    assert validators.pct_of(100, 7) == 7.0
    assert validators.pct_of(91.07, 35) == 31.8745


# ------------------------------------------- contratto owner PF-ROSERPINA-DRAWDOWN-AND-RECOVERY-20261109
def test_contract_example_peak_120_cash_107_blocks_with_13(tmp_path):
    """Esempio dell'owner: capitale 100, picco verificato 120, cassa 107, stop 10 => drawdown 13, bloccato."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    _lettura(c, 120.0)
    _lettura(c, 107.0)
    assert c.rc._hard_stop().loss(107.0) == validators.money_decimal(13)
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _bloccato_da_hard_stop(c), _rifiuti(c)


def test_breach_triggers_lockdown_at_the_reading_without_waiting_for_a_signal(tmp_path):
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    _lettura(c, 89.0)                                    # variazione finanziaria: nessun segnale arrivato
    assert c.rc.mode.value == "LOCKDOWN"
    assert c.rc._hard_stop().breached is True


def test_breach_is_latched_even_if_cash_recovers_and_survives_restart(tmp_path):
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    _lettura(c, 89.0)
    _lettura(c, 99.0)                                    # la cassa risale (settlement vincente): resta bloccato
    assert c.rc._hard_stop().reason(10.0, 99.0) == REASON_ACTIVE
    riavviato = DrawdownHardStop(c.rc.db)
    riavviato.restore()
    assert riavviato.breached is True and riavviato.reason(10.0, 99.0) == REASON_ACTIVE


_OWNER = dict(authorization="#586 c.123 decisione owner", reason="perdita verificata e accettata",
              operation_id="PF-UNLOCK-0001", confirm=True)


@pytest.mark.parametrize("missing", ["authorization", "reason", "operation_id", "confirm"])
def test_unlock_requires_authorization_reason_operation_id_and_explicit_confirmation(missing):
    stop = DrawdownHardStop(_Db())
    stop.observe(100.0)
    assert stop.evaluate(10.0, 85.0) is True
    args = dict(_OWNER, **{missing: False if missing == "confirm" else ""})
    assert stop.owner_clear_breach(**args) is False and stop.breached


def test_unlock_is_durably_recorded_and_does_not_rebase_the_peak():
    db = _Db()
    stop = DrawdownHardStop(db)
    stop.observe(100.0)
    stop.evaluate(10.0, 85.0)
    assert stop.owner_clear_breach(**_OWNER, checks={"cash_verified": True}) is True
    record = json.loads(db.settings[STATE_KEY])
    entry = record["breach"]["cleared"][0]
    assert record["breached"] is False and record["peak"] == "100.0"           # nessuna nuova baseline
    assert entry["operation_id"] == "PF-UNLOCK-0001" and entry["reason"] and entry["checks"]["cash_verified"]
    assert entry["breach"]["kind"] == "max_drawdown_abs"
    assert stop.reason(10.0, 85.0) == REASON_ACTIVE      # perdita ancora oltre il limite: nuovo blocco immediato


def test_unlock_that_cannot_be_saved_keeps_the_block():
    db = _Db()
    stop = DrawdownHardStop(db)
    stop.observe(100.0)
    stop.evaluate(10.0, 85.0)
    db.fail_save = True
    assert stop.owner_clear_breach(**_OWNER) is False
    assert stop.breached is True
    assert stop.reason(10.0, 99.0) == REASON_ACTIVE


def test_owner_rebase_is_explicit_traced_and_distinct_never_automatic(tmp_path):
    db = _Db()
    stop = DrawdownHardStop(db)
    stop.observe(100.0)
    assert stop.owner_rebase_peak(85.0, authorization="#586") is False and stop.snapshot()["peak"] == 100.0
    assert stop.owner_rebase_peak(85.0, **_OWNER) is True
    record = json.loads(db.settings[STATE_KEY])
    assert record["peak"] == "85.0" and record["breach"]["rebased"]["from"] == "100.0"

    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    _lettura(c, 95.0)
    c.rc._reset_recovery_cycle_keeping_losses()
    c.rc._reset_reservations_keeping_uncertain()
    assert c.rc._hard_stop().snapshot()["peak"] == 100.0     # i reset automatici non toccano il picco


def test_breach_is_never_cleared_by_restart_settlement_strategic_reset_config_or_new_signal(tmp_path):
    """RO-DD-06/07: nessuno sblocco implicito."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    _lettura(c, 89.0)
    assert c.rc._hard_stop().breached
    _lettura(c, 101.0)                                   # settlement vincente: cassa oltre il vecchio picco
    c.rc._reset_recovery_cycle_keeping_losses()          # reset strategico
    c.rc.config.max_drawdown_abs = None                  # limite tolto dalla configurazione
    del c.rc._drawdown_hard_stop                         # riavvio: stato riletto dal DB
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _bloccato_da_hard_stop(c), _rifiuti(c)


def test_runtime_unlock_procedure_verifies_cash_exposure_and_orders_first(tmp_path):
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    _lettura(c, 89.0)
    c.rc._mark_uncertain("K-AMB", 1, customer_ref="r-amb")
    esito = c.rc.owner_unlock_drawdown_hard_stop(**_OWNER)
    assert esito == dict(esito, unlocked=False, reason="ordini_incerti_presenti")
    c.rc._clear_uncertain("K-AMB", 1, customer_ref="r-amb")
    c.rc.betfair_service.get_account_funds = lambda: {"error": "x"}
    c.rc._apply_funds_reading({"error": "x"}, source="test")
    assert c.rc.owner_unlock_drawdown_hard_stop(**_OWNER)["reason"] == "cassa_non_verificata"
    _lettura(c, 95.0)
    esito = c.rc.owner_unlock_drawdown_hard_stop(**_OWNER)
    assert esito["unlocked"] is True and esito["checks"]["uncertain_orders"] == []
    c.rc.mode = c.rc.mode.__class__.ACTIVE
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 1, _rifiuti(c)                  # perdita 5 < 10 dopo lo sblocco


# ------------------------------------------------ D-2 / RO-DD-04 (PF-ROSERPINA-FINANCIAL-CONTRACT-FINAL-20261109)
def test_ro_dd_04_lockdown_sees_the_cumulative_loss_across_the_auto_reset(tmp_path):
    """Con i default (auto-reset 15 % < lockdown 20 %) una perdita graduale non arrivava mai in LOCKDOWN: l'auto-reset
    ribasava il picco del desk. Ora il LOCKDOWN misura anche la perdita di cassa dal picco persistente."""
    c = _catena(tmp_path)                                # nessun max_drawdown_abs: vale la soglia di LOCKDOWN
    _lettura(c, 100.0)
    _lettura(c, 85.0)
    c.rc._reset_recovery_cycle_keeping_losses()          # auto-reset del ciclo: il desk riparte da 85
    _lettura(c, 79.0)                                    # 6 % nel ciclo, 21 % cumulativo
    assert c.rc.risk_desk.drawdown_pct() < c.rc.config.lockdown_drawdown_pct
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 0 and _rifiuti(c)[-1] == "desk_lockdown"
    assert c.rc.get_status()["loss_drawdown_pct"] == pytest.approx(21.0)


def test_d2_open_exposure_is_not_a_loss_and_stays_governed_by_its_own_caps(tmp_path):
    """Contratto owner PF-ROSERPINA-D2-LOCKDOWN-UNLOCK-FINAL-20261109. Caso numerico della transizione: cassa 100,
    30 impegnati, nessuna perdita. Prima il desk misurava 30 % di «drawdown» sul disponibile (70/100) e andava in
    LOCKDOWN (soglia 20 %) per la sola occupazione del capitale. Ora la perdita e' 0 %: DEFENSE/LOCKDOWN non scattano,
    e il capitale impegnato resta limitato dai cap di esposizione GIA' configurati (nessuna soglia nuova)."""
    ammesso = _catena(tmp_path / "ok", max_drawdown_abs=10.0, max_total_exposure_pct=60.0)
    _lettura(ammesso, 100.0)
    _lettura(ammesso, 70.0, committed=30.0)
    stato = ammesso.rc.get_status()
    assert stato["loss_drawdown_pct"] == 0.0
    assert stato["capital_at_risk_pct"] == pytest.approx(30.0)            # informativo, non decide piu' il desk
    assert ammesso.rc._drawdown_hard_stop_reason() == ""
    ammesso.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(ammesso) == 1, _rifiuti(ammesso)

    pieno = _catena(tmp_path / "pieno", max_drawdown_abs=10.0, max_total_exposure_pct=30.0)
    _lettura(pieno, 100.0)
    _lettura(pieno, 70.0, committed=30.0)
    pieno.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(pieno) == 0 and _rifiuti(pieno)[-1] == "supera_max_total_exposure"   # protezione esistente


def test_d2_unreliable_peak_state_puts_the_desk_in_lockdown(tmp_path):
    c = _catena(tmp_path)
    _lettura(c, 100.0)
    c.rc._hard_stop().unreadable = True                   # stato del picco illeggibile al riavvio
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 0 and _rifiuti(c)[-1] == "desk_lockdown"


@pytest.mark.parametrize("mode", ["SIMULATION", "LIVE"])
def test_percent_hard_stop_uses_the_persistent_peak_in_sim_and_live(tmp_path, mode):
    """RO-DD-05 + RO-DD-04: `max_drawdown_hard_stop_pct` vale anche in SIM (parita' PF-OD12-12) e sul picco persistente."""
    c = _catena(tmp_path, max_drawdown_hard_stop_pct=12.0, lockdown_drawdown_pct=50.0, defense_drawdown_pct=40.0,
                auto_reset_drawdown_pct=10.0)
    c.rc.execution_mode = mode
    _lettura(c, 100.0)
    _lettura(c, 90.0)
    c.rc._reset_recovery_cycle_keeping_losses()
    _lettura(c, 87.0)                                     # 3 % nel ciclo, 13 % dal picco persistente
    # stessa decisione in SIM e in LIVE: il blocco nasce alla lettura del conto, prima di qualunque segnale
    assert c.rc._hard_stop().breached and c.rc.mode.value == "LOCKDOWN"
    assert c.rc._drawdown_hard_stop_reason() == REASON_PCT_ACTIVE
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 0                       # in LIVE il test e' respinto anche dal deploy gate: nessun ordine
    assert c.rc._hard_stop().snapshot()["breach"]["kind"] == "max_drawdown_hard_stop_pct"


def test_d2_desk_mode_uses_the_cumulative_cash_loss():
    from core.money_management import RoserpinaMoneyManagement
    from core.system_state import DeskMode, RoserpinaConfig
    mm = RoserpinaMoneyManagement(RoserpinaConfig())
    base = dict(bankroll_current=95.0, equity_peak=100.0)            # 5 % sul disponibile (desk del ciclo)
    assert mm.determine_desk_mode(**base) == DeskMode.NORMAL
    assert mm.determine_desk_mode(**base, persistent_drawdown_pct=25.0, persistent_drawdown_given=True) == \
        DeskMode.LOCKDOWN
    assert mm.determine_desk_mode(**base, persistent_drawdown_pct=8.0, persistent_drawdown_given=True) == \
        DeskMode.DEFENSE
    for bad in (None, float("nan"), -1.0, True):
        assert mm.determine_desk_mode(**base, persistent_drawdown_pct=bad, persistent_drawdown_given=True) == \
            DeskMode.LOCKDOWN
    # 30 impegnati senza perdita: la componente di drawdown e' la sola perdita di cassa (contratto owner D-2)
    assert mm.determine_desk_mode(bankroll_current=70.0, equity_peak=100.0, persistent_drawdown_pct=0.0,
                                  persistent_drawdown_given=True) == DeskMode.NORMAL
    # senza alcuna cassa verificata resta la misura storica (gli ingressi sono comunque negati dalla cassa)
    assert mm.determine_desk_mode(bankroll_current=70.0, equity_peak=100.0) == DeskMode.LOCKDOWN


def test_unlock_operation_id_is_single_use():
    stop = DrawdownHardStop(_Db())
    stop.observe(100.0)
    stop.evaluate(10.0, 85.0)
    assert stop.owner_clear_breach(**_OWNER) is True
    assert stop.evaluate(10.0, 85.0) is True                  # perdita ancora oltre il limite: nuovo blocco
    assert stop.owner_clear_breach(**_OWNER) is False and stop.breached   # stesso operation_id: rifiutato
    assert stop.owner_clear_breach(**dict(_OWNER, operation_id="PF-UNLOCK-0002")) is True


def test_ro_dd_08_unsaved_peak_blocks_entries_even_without_hard_stops(tmp_path):
    """RO-DD-08 (Claude D su 15960bac): picco salito a 120 ma non salvato, nessun hard-stop configurato. Senza blocco,
    un riavvio ripartirebbe dal picco salvato 100 e il LOCKDOWN cumulativo (RO-DD-04) sarebbe aggirabile."""
    c = _catena(tmp_path, lockdown_drawdown_pct=15.0)
    _lettura(c, 100.0)
    real_save = c.rc.db.save_settings

    def failing_save(values):
        if "roserpina.drawdown_hard_stop_state" in values:
            raise OSError("disco pieno")
        return real_save(values)

    c.rc.db.save_settings = failing_save
    _lettura(c, 120.0)
    assert c.rc._hard_stop().persist_failed
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 0 and _rifiuti(c)[-1] == "drawdown_picco_non_persistito"

    c.rc.db.save_settings = real_save                     # il disco torna: il salvataggio si ritenta e il blocco cade
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK", market_id="1.999"))
    assert json.loads(c.rc.db.get_settings()[STATE_KEY])["peak"] == "120.0"
    assert _ordini(c) == 1, _rifiuti(c)                    # picco salvato, nessuna perdita: ingresso ammesso


def test_ro_dd_09_unsaved_rise_then_loss_then_restart_keeps_the_true_peak(tmp_path):
    """RO-DD-09 (Claude D su 3f59fa67): cassa 100 -> 120 con il salvataggio nel DB fallito -> 100, poi riavvio con il
    DB di nuovo disponibile. Il picco vero (120) sopravvive nel marker su file separato: drawdown 16,7 % >= 15 % =>
    LOCKDOWN, mai un riavvio che riparte da 100."""
    c = _catena(tmp_path, lockdown_drawdown_pct=15.0)
    _lettura(c, 100.0)
    real_save = c.rc.db.save_settings

    def failing_save(values):
        if STATE_KEY in values:
            raise OSError("DB non scrivibile")
        return real_save(values)

    c.rc.db.save_settings = failing_save
    _lettura(c, 120.0)
    _lettura(c, 100.0)
    marker = c.rc.db.db_path + ".drawdown_peak_pending.json"
    assert json.loads(open(marker).read())["peak"] == "120.0"

    c.rc.db.save_settings = real_save                     # riavvio: DB di nuovo scrivibile
    riavviato = DrawdownHardStop(c.rc.db)
    riavviato.restore()
    assert riavviato.snapshot()["peak"] == 120.0 and not riavviato.persist_failed
    assert json.loads(c.rc.db.get_settings()[STATE_KEY])["peak"] == "120.0"     # riportato nel DB
    assert not os.path.exists(marker)                     # marker consumato solo dopo il salvataggio nel DB
    assert riavviato.loss_pct(100.0) == pytest.approx(16.6667, rel=1e-4)

    del c.rc._drawdown_hard_stop                          # il runtime riavviato blocca l'ingresso
    c.rc.mode = c.rc.mode.__class__.ACTIVE
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 0 and _rifiuti(c)[-1] == "desk_lockdown"


def test_ro_dd_09_unreadable_marker_fails_closed(tmp_path):
    c = _catena(tmp_path)
    open(c.rc.db.db_path + ".drawdown_peak_pending.json", "w").write("{rotto")
    stop = DrawdownHardStop(c.rc.db)
    stop.restore()
    assert stop.unreadable and stop.reason(10.0, 100.0) == REASON_STATE_UNREADABLE


# ------------------------------- RO-DD-09 C (decisione owner PF-ROSERPINA-DD09-FAIL-CLOSED-AND-BB3-20261109)
_CONFIRM = dict(authorization="#586 decisione owner baseline", reason="riconciliazione verificata dopo crash",
                operation_id="PF-BASELINE-0001", confirm=True, confirm_again=True)


def _stop_pulito(c):
    c.rc.betfair_service.disconnect = lambda: None        # stub del servizio senza disconnect
    c.rc.stop()


def _riavvio(c):
    """Nuovo processo sullo stesso DB: lo stato dell'hard-stop si rilegge come nel costruttore."""
    del c.rc._drawdown_hard_stop
    c.rc._hard_stop()
    c.rc.mode = c.rc.mode.__class__.ACTIVE


def _failing_db_and_marker(c, monkeypatch):
    real_save = c.rc.db.save_settings

    def failing_save(values):
        if STATE_KEY in values:
            raise OSError("DB non scrivibile")
        return real_save(values)

    c.rc.db.save_settings = failing_save
    import core.drawdown_hard_stop as dhs

    def failing_marker(*a, **k):
        raise OSError("filesystem non scrivibile")

    monkeypatch.setattr(dhs, "atomic_write_text", failing_marker)
    return real_save


def test_dd09c_db_and_marker_unwritable_then_crash_restart_blocks_entries(tmp_path, monkeypatch):
    """Punti 3 e 4: DB e marker non scrivibili insieme, picco salito a 120 mai salvato, crash e riavvio con lo storage
    tornato. Il picco su disco e' 100 ma la sessione risulta aperta: baseline non verificata, nessun ingresso."""
    c = _catena(tmp_path, lockdown_drawdown_pct=15.0)
    _lettura(c, 100.0)
    c.rc._hard_stop().open_session()                      # START del trading
    real_save = _failing_db_and_marker(c, monkeypatch)
    _lettura(c, 120.0)
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 0 and _rifiuti(c)[-1] == "drawdown_picco_non_persistito"     # nel processo: bloccato
    _lettura(c, 105.0)
    monkeypatch.undo()                                     # crash; al riavvio lo storage funziona di nuovo
    c.rc.db.save_settings = real_save
    _riavvio(c)
    assert c.rc._hard_stop().snapshot()["peak"] == 100.0  # il picco vero (120) e' perso: nessuna presunzione
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK", market_id="1.777"))
    assert _ordini(c) == 0 and _rifiuti(c)[-1] == "drawdown_baseline_non_verificata"


def test_dd09c_crash_after_a_normal_session_requires_owner_confirmation(tmp_path):
    """Punti 5 e 7: crash dopo una sessione regolare => baseline non verificata, anche dopo un secondo riavvio."""
    c = _catena(tmp_path)
    _lettura(c, 100.0)
    c.rc._hard_stop().open_session()
    _riavvio(c)                                            # crash
    assert not c.rc._hard_stop().baseline_verified
    _riavvio(c)                                            # secondo riavvio senza conferma
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 0 and _rifiuti(c)[-1] == "drawdown_baseline_non_verificata"


def test_dd09c_clean_stop_and_restart_keeps_the_baseline_verified(tmp_path):
    """Punto 6: STOP pulito con picco durevole => riavvio normale, ingressi ammessi."""
    c = _catena(tmp_path)
    _lettura(c, 100.0)
    c.rc._open_drawdown_session()                         # START: apertura del controller proprietario
    _stop_pulito(c)
    assert json.loads(c.rc.db.get_settings()[STATE_KEY])["session_open"] is False
    _riavvio(c)
    assert c.rc._hard_stop().baseline_verified
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 1, _rifiuti(c)


def test_dd09c_clean_stop_with_unsaved_peak_leaves_the_session_open(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    _lettura(c, 100.0)
    c.rc._open_drawdown_session()
    real_save = _failing_db_and_marker(c, monkeypatch)
    _lettura(c, 120.0)
    _stop_pulito(c)                                        # lo STOP non puo' chiudere: picco non durevole
    monkeypatch.undo()
    c.rc.db.save_settings = real_save
    _riavvio(c)
    assert not c.rc._hard_stop().baseline_verified


@pytest.mark.parametrize("missing", ["authorization", "reason", "operation_id", "confirm", "confirm_again"])
def test_dd09c_baseline_confirmation_requires_every_field_and_double_confirmation(tmp_path, missing):
    """Punto 9: conferma incompleta => rifiutata, resta bloccato."""
    c = _catena(tmp_path)
    _lettura(c, 100.0)
    c.rc._hard_stop().open_session()
    _riavvio(c)
    args = dict(_CONFIRM, **{missing: False if missing.startswith("confirm") else ""})
    assert c.rc.owner_confirm_drawdown_baseline(**args)["confirmed"] is False
    assert not c.rc._hard_stop().baseline_verified


def test_dd09c_baseline_confirmation_checks_orders_and_is_recorded_not_as_proof(tmp_path):
    """Punto 5: conferma owner con verifica degli ordini; baseline = max(picco salvato, dichiarato, cassa); registrata
    in modo durevole come conferma, non come prova del massimo storico; operation_id monouso."""
    c = _catena(tmp_path, lockdown_drawdown_pct=15.0)
    _lettura(c, 100.0)
    c.rc._hard_stop().open_session()
    _riavvio(c)
    _lettura(c, 105.0)
    c.rc._mark_uncertain("K-AMB", 1, customer_ref="r-amb")
    assert c.rc.owner_confirm_drawdown_baseline(**_CONFIRM)["reason"] == "ordini_incerti_presenti"
    c.rc._clear_uncertain("K-AMB", 1, customer_ref="r-amb")
    esito = c.rc.owner_confirm_drawdown_baseline(**_CONFIRM, declared_peak=120.0)
    assert esito["confirmed"] is True
    record = json.loads(c.rc.db.get_settings()[STATE_KEY])
    entry = record["baseline"]["confirmations"][0]
    assert record["baseline_verified"] is True and record["peak"] == "120.0"
    assert entry["declared_peak"] == "120.0" and "non e' una prova" in entry["nota"]
    assert c.rc.owner_confirm_drawdown_baseline(**_CONFIRM)["reason"] == "baseline_gia_verificata"
    # con il picco dichiarato 120 la perdita e' 12,5 %: sotto il 15 % l'ingresso passa; il picco non si e' abbassato
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 1, _rifiuti(c)


def test_dd09c_strategic_reset_never_clears_an_unverified_baseline(tmp_path):
    c = _catena(tmp_path)
    _lettura(c, 100.0)
    c.rc._hard_stop().open_session()
    _riavvio(c)
    c.rc._reset_recovery_cycle_keeping_losses()
    c.rc._reset_reservations_keeping_uncertain()
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 0 and _rifiuti(c)[-1] == "drawdown_baseline_non_verificata"


def test_dd09c_start_opens_and_stop_closes_the_session_in_the_runtime():
    """Cablaggio: START apre la sessione su disco (prima del trading), STOP la chiude."""
    import inspect
    from core.runtime_controller import RuntimeController
    start_src = inspect.getsource(RuntimeController._start_with_instance_lock)
    stop_src = inspect.getsource(RuntimeController._stop_serialized)
    assert "_open_drawdown_session()" in start_src
    # D-631-15: la sessione si apre su disco PRIMA della lettura dei fondi (prima l'ordine era invertito)
    assert start_src.index("_open_drawdown_session()") < start_src.index("_applica_fondi_avvio")
    assert "_close_drawdown_session()" in stop_src


# ----------------------------------------------------------------------------------- Sol su dc2f5f25 (PR-03a)
def test_d1_zero_cash_memorizes_the_total_loss_and_survives_a_recharge():
    """Sol su dc2f5f25: una cassa verificata pari a 0 e' una perdita totale, non un dato mancante. Va memorizzata
    (anche con soglia 100 %), e dopo una ricarica l'ingresso resta negato finche' l'owner non sblocca."""
    stop = DrawdownHardStop(_Db())
    stop.observe(100.0)
    assert stop.reason(50.0, 0) == REASON_ACTIVE and stop.breached
    assert stop.snapshot()["breach"]["loss"] == "100.0" and stop.snapshot()["peak"] == 100.0
    assert stop.reason(50.0, 200.0) == REASON_ACTIVE           # ricarica: resta bloccato
    pct = DrawdownHardStop(_Db())
    pct.observe(100.0)
    assert pct.evaluate(None, 0.0, pct_cap_raw=100.0) is True  # soglia 100 % ammessa: perdita totale memorizzata
    assert pct.reason(None, 200.0, pct_cap_raw=100.0) == REASON_PCT_ACTIVE


def test_d1_zero_cash_below_the_cap_still_denies_entries():
    """Cassa 0 senza superamento (nessun picco ancora): nessun ingresso, nessun picco inventato."""
    stop = DrawdownHardStop(_Db())
    assert stop.reason(50.0, 0) == REASON_CASH_UNVERIFIED
    assert stop.snapshot()["peak"] is None and not stop.breached


def test_d1_zero_cash_reading_in_the_runtime_memorizes_the_breach(tmp_path):
    """Catena reale: il conto letto a 0 (disponibile 0, nessuna esposizione) e' coerente ma non e' una cassa
    utilizzabile; il superamento si memorizza comunque e la ricarica successiva non riapre gli ingressi."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    fondi = {"available": 0.0, "exposure": 0.0, "ok": True}
    c.rc.betfair_service.get_account_funds = lambda: dict(fondi)
    c.rc._apply_funds_reading(dict(fondi), source="test")
    assert c.rc._hard_stop().breached
    _lettura(c, 200.0)
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _bloccato_da_hard_stop(c)


def _ordine_stop(c, monkeypatch, *, disconnect_fails=False):
    calls = []
    c.rc._drawdown_session_owned = True                   # sessione aperta da questo controller (START)
    stop = c.rc._hard_stop()
    monkeypatch.setattr(stop, "close_session", lambda: calls.append("close_session") or True)
    monkeypatch.setattr(c.rc, "_release_runtime_instance_lock_if_stopped",
                        lambda: calls.append("release_lock"))
    def _disconnect():
        if disconnect_fails:
            raise RuntimeError("disconnect fallita")
    monkeypatch.setattr(c.rc.betfair_service, "disconnect", _disconnect, raising=False)
    return calls


def test_dd09c_stop_closes_the_session_before_releasing_the_instance_lock(tmp_path, monkeypatch):
    """Sol su dc2f5f25: chiusa dopo il rilascio del lock, la sessione del vecchio processo sovrascriverebbe quella
    (e il picco) del nuovo processo appena avviato: deve chiudersi mentre il lock e' ancora tenuto."""
    c = _catena(tmp_path)
    calls = _ordine_stop(c, monkeypatch)
    c.rc._stop_serialized()
    assert calls == ["close_session", "release_lock"]


def test_dd09c_failed_teardown_leaves_the_session_open(tmp_path, monkeypatch):
    """Smontaggio fallito: STOP non pulito, sessione lasciata aperta (il prossimo avvio chiede la conferma owner)."""
    c = _catena(tmp_path)
    calls = _ordine_stop(c, monkeypatch, disconnect_fails=True)
    with pytest.raises(RuntimeError):
        c.rc._stop_serialized()
    assert "close_session" not in calls and calls == ["release_lock"]


_ORDINE_MANUALE = {"side": "BACK", "price": 2.0, "stake": 0.5, "market_id": "1.234", "selection_id": 11,
                   "event_name": "A v B"}


@pytest.mark.parametrize("cap", [None, 10.0])
def test_dd09c_manual_and_dutching_path_applies_the_baseline_gate(tmp_path, cap):
    """Codex/Grok/Claude D su dc2f5f25 (D-631-03): dopo un riavvio non pulito il segnale e' negato, ma il percorso
    manuale/Dutching (`absolute_cap_reason_for_orders`) usciva prima (senza cap) o ignorava la baseline (con cap)."""
    c = _catena(tmp_path, **({} if cap is None else {"max_drawdown_abs": cap}))
    _lettura(c, 100.0)
    c.rc._hard_stop().open_session()
    _riavvio(c)
    assert c.rc.absolute_cap_reason_for_orders([dict(_ORDINE_MANUALE)]) == "drawdown_baseline_non_verificata"


@pytest.mark.parametrize("cap", [None, 10.0])
def test_ro_dd_08_manual_and_dutching_path_applies_the_unsaved_peak_gate(tmp_path, cap):
    c = _catena(tmp_path, **({} if cap is None else {"max_drawdown_abs": cap}))
    _lettura(c, 100.0)
    real_save = c.rc.db.save_settings

    def failing_save(values):
        if STATE_KEY in values:
            raise OSError("disco pieno")
        return real_save(values)

    c.rc.db.save_settings = failing_save
    _lettura(c, 120.0)
    assert c.rc.absolute_cap_reason_for_orders([dict(_ORDINE_MANUALE)]) == "drawdown_picco_non_persistito"
    c.rc.db.save_settings = real_save
    assert c.rc.absolute_cap_reason_for_orders([dict(_ORDINE_MANUALE)]) == ""


def test_d1_zero_available_with_committed_cash_is_evaluated(tmp_path):
    """Codex su dc2f5f25: disponibile 0 e impegnato 85 (cassa verificata 85) dopo un picco 100: la perdita 15 supera il
    limite 10 e si memorizza; il ritorno del disponibile a 95 non riapre gli ingressi."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    fondi = {"available": 0.0, "exposure": -85.0, "ok": True}
    c.rc.betfair_service.get_account_funds = lambda: dict(fondi)
    c.rc._apply_funds_reading(dict(fondi), source="test")
    stop = c.rc._hard_stop()
    assert stop.breached and stop.snapshot()["breach"]["loss"] == "15.0"
    _lettura(c, 95.0)
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _bloccato_da_hard_stop(c)


def test_d1_zero_available_with_committed_cash_below_the_cap_does_not_breach(tmp_path):
    """Impegnato non e' perdita: disponibile 0 con 95 impegnati dopo picco 100 => perdita 5, nessun superamento."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    fondi = {"available": 0.0, "exposure": -95.0, "ok": True}
    c.rc.betfair_service.get_account_funds = lambda: dict(fondi)
    c.rc._apply_funds_reading(dict(fondi), source="test")
    assert not c.rc._hard_stop().breached and c.rc._hard_stop().snapshot()["peak"] == 100.0


def test_ro_dd_09_stale_marker_never_overrides_a_newer_owner_clearance(tmp_path):
    """Codex/Grok/Claude D su dc2f5f25 (D-631-05): superamento scritto nel marker (DB giu'), poi sblocco owner salvato
    nel DB con seq piu' alto ma marker non rimosso (file bloccato su Windows). Al riavvio lo sblocco vale; il marker
    vecchio non resuscita il superamento ne' riapre la sessione."""
    db_path = str(tmp_path / "pf.db")
    db = _Db()
    db.db_path = db_path
    stop = DrawdownHardStop(db)
    stop.observe(100.0)
    db.fail_save = True
    stop.evaluate(10.0, 80.0)                                # superamento -> solo nel marker
    marker = db_path + ".drawdown_peak_pending.json"
    assert os.path.exists(marker) and json.loads(open(marker).read())["breached"] is True
    db.fail_save = False
    def _bloccato(path):
        raise OSError("file bloccato")

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(os, "remove", _bloccato)
        assert stop.owner_clear_breach(authorization="PF-OWNER-X", reason="verificato", operation_id="op-1",
                                       confirm=True, checks={"cash": "ok"})
    assert os.path.exists(marker)                            # marker vecchio rimasto
    riavviato = DrawdownHardStop(db)
    riavviato.restore()
    assert not riavviato.breached and riavviato.reason(10.0, 95.0) == ""
    assert riavviato.baseline_verified


def test_ro_dd_09_newer_marker_still_wins_over_an_older_db_record(tmp_path):
    """Il marker piu' recente del DB (DB giu' al momento del superamento) resta autorevole al riavvio."""
    db = _Db()
    db.db_path = str(tmp_path / "pf.db")
    stop = DrawdownHardStop(db)
    stop.observe(100.0)
    db.fail_save = True
    stop.evaluate(10.0, 80.0)
    db.fail_save = False
    riavviato = DrawdownHardStop(db)
    riavviato.restore()
    assert riavviato.breached and riavviato.reason(10.0, 100.0) == REASON_ACTIVE


def test_dd09c_start_with_unsaved_session_blocks_entries(tmp_path, caplog):
    """Grok/Claude D su dc2f5f25 (D-631-04): `open_session()` non salvata all'avvio non e' ignorata: log critico e
    nessun ingresso finche' la sessione aperta non e' durevole (RO-DD-08)."""
    c = _catena(tmp_path)
    _lettura(c, 100.0)
    real_save = c.rc.db.save_settings

    def failing_save(values):
        if STATE_KEY in values:
            raise OSError("disco pieno")
        return real_save(values)

    c.rc.db.save_settings = failing_save
    with caplog.at_level("CRITICAL"):
        c.rc._open_drawdown_session()
    assert any("sessione" in r.getMessage() and r.levelname == "CRITICAL" for r in caplog.records)
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 0 and _rifiuti(c)[-1] == "drawdown_picco_non_persistito"
    c.rc.db.save_settings = real_save
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK", market_id="1.999"))
    assert json.loads(c.rc.db.get_settings()[STATE_KEY])["session_open"] is True
    assert _ordini(c) == 1, _rifiuti(c)


# ----------------------------------------------------------------------------------- Sol su 92155c24 (PR-03a)
def test_unreadable_state_blocks_manual_and_dutching_without_hard_stops(tmp_path):
    """Sol su 92155c24: stato del picco illeggibile e nessun hard-stop configurato: anche il percorso manuale/Dutching
    nega (picco sconosciuto), come il desk in LOCKDOWN per i segnali; lo stato illeggibile non viene sovrascritto."""
    c = _catena(tmp_path)
    _lettura(c, 100.0)
    c.rc.db.save_settings({STATE_KEY: "{"})
    _riavvio(c)
    assert c.rc.absolute_cap_reason_for_orders([dict(_ORDINE_MANUALE)]) == "drawdown_stato_illeggibile"
    assert c.rc.db.get_settings()[STATE_KEY] == "{"


def _stop_con_marker(tmp_path):
    db = _Db()
    db.db_path = str(tmp_path / "pf.db")
    return db, DrawdownHardStop(db)


def test_failed_owner_clearance_never_reaches_the_marker(tmp_path):
    """Sol su 92155c24: DB guasto e marker scrivibile durante lo sblocco owner. Lo sblocco fallisce e NON deve finire
    nel marker come record piu' recente: al riavvio il superamento resta."""
    db, stop = _stop_con_marker(tmp_path)
    stop.observe(100.0)
    assert stop.evaluate(10.0, 80.0) is True                 # superamento salvato nel DB
    db.fail_save = True
    assert stop.owner_clear_breach(authorization="PF-OWNER-X", reason="verificato", operation_id="op-1",
                                   confirm=True) is False
    assert stop.breached
    db.fail_save = False
    riavviato = DrawdownHardStop(db)
    riavviato.restore()
    assert riavviato.breached and riavviato.reason(10.0, 100.0) == REASON_ACTIVE


def test_failed_owner_rebase_never_reaches_the_marker(tmp_path):
    db, stop = _stop_con_marker(tmp_path)
    stop.observe(100.0)
    db.fail_save = True
    assert stop.owner_rebase_peak(50.0, authorization="PF-OWNER-X", reason="nuova baseline", operation_id="op-2",
                                  confirm=True) is False
    db.fail_save = False
    riavviato = DrawdownHardStop(db)
    riavviato.restore()
    assert riavviato.snapshot()["peak"] == 100.0


def test_failed_baseline_confirmation_never_reaches_the_marker(tmp_path):
    db, stop = _stop_con_marker(tmp_path)
    stop.observe(100.0)
    stop.open_session()
    riavviato = DrawdownHardStop(db)                         # crash: sessione aperta => baseline non verificata
    riavviato.restore()
    assert not riavviato.baseline_verified
    db.fail_save = True
    assert riavviato.owner_confirm_baseline(authorization="PF-OWNER-X", reason="verificato", operation_id="op-3",
                                            confirm=True, confirm_again=True, cash_equity=100.0) is False
    db.fail_save = False
    di_nuovo = DrawdownHardStop(db)
    di_nuovo.restore()
    assert not di_nuovo.baseline_verified


def test_failed_session_close_never_reaches_the_marker(tmp_path):
    """Chiusura della sessione con il DB giu': resta aperta su disco (nessun marker 'chiusa'), il riavvio chiede la
    conferma owner della baseline."""
    db, stop = _stop_con_marker(tmp_path)
    stop.observe(100.0)
    stop.open_session()
    db.fail_save = True
    assert stop.close_session() is False
    db.fail_save = False
    riavviato = DrawdownHardStop(db)
    riavviato.restore()
    assert not riavviato.baseline_verified


def test_rising_peak_still_falls_back_to_the_marker(tmp_path):
    """Gli stati che stringono restano protetti dal marker (RO-DD-09): picco salito con il DB giu' sopravvive."""
    db, stop = _stop_con_marker(tmp_path)
    stop.observe(100.0)
    db.fail_save = True
    stop.observe(120.0)
    db.fail_save = False
    riavviato = DrawdownHardStop(db)
    riavviato.restore()
    assert riavviato.snapshot()["peak"] == 120.0


def test_start_reloads_the_drawdown_state_written_by_the_previous_owner(tmp_path):
    """Codex su 92155c24: controller B costruito (picco 100) mentre A era ancora proprietario; A salva poi picco 120 e
    un superamento. All'avvio B rilegge sotto la proprieta' del runtime: il blocco di A resta, nessuna sovrascrittura."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    vecchio = c.rc._hard_stop()
    altro = DrawdownHardStop(c.rc.db)                        # processo A, ancora proprietario
    altro.restore()
    altro.observe(120.0)
    assert altro.evaluate(10.0, 105.0) is True
    assert not vecchio.breached and vecchio.snapshot()["peak"] == 100.0
    c.rc._reload_drawdown_state()
    assert vecchio.breached and vecchio.snapshot()["peak"] == 120.0
    record = json.loads(c.rc.db.get_settings()[STATE_KEY])
    assert record["breached"] is True and record["peak"] == "120.0"


def test_start_reloads_before_the_first_funds_reading_and_the_session_write():
    import inspect
    from core.runtime_controller import RuntimeController
    src = inspect.getsource(RuntimeController._start_with_instance_lock)
    assert src.index("_reload_drawdown_state()") < src.index("_applica_fondi_avvio(funds")
    assert src.index("_reload_drawdown_state()") < src.index("_open_drawdown_session()")


# ----------------------------------------------------------------------------------- Sol su 58e635e6 (PR-03a)
def test_restore_before_ownership_never_writes_and_start_reconciles_under_the_lock(tmp_path):
    """Sol su 58e635e6: costruito prima di avere la proprieta' del runtime, il controller rileggeva il marker e lo
    migrava nel DB (seq N+1) senza lock, sovrascrivendo un record piu' recente del proprietario. Ora la lettura
    iniziale non scrive niente; la migrazione avviene allo START, sotto il lock di istanza."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    real_save = c.rc.db.save_settings

    def failing_save(values):
        if STATE_KEY in values:
            raise OSError("disco pieno")
        return real_save(values)

    c.rc.db.save_settings = failing_save
    _lettura(c, 120.0)                                     # picco 120 solo nel marker
    marker = c.rc.db.db_path + ".drawdown_peak_pending.json"
    assert os.path.exists(marker)
    c.rc.db.save_settings = real_save
    prima = c.rc.db.get_settings()[STATE_KEY]
    scritture = []
    c.rc.db.save_settings = lambda values: scritture.append(values) or real_save(values)
    del c.rc._drawdown_hard_stop                            # nuovo processo, non ancora proprietario
    nuovo = c.rc._hard_stop()
    assert nuovo.snapshot()["peak"] == 120.0                # il picco del marker vale gia' in memoria
    assert scritture == [] and c.rc.db.get_settings()[STATE_KEY] == prima and os.path.exists(marker)
    c.rc._reload_drawdown_state()                           # START, lock di istanza tenuto
    assert json.loads(c.rc.db.get_settings()[STATE_KEY])["peak"] == "120.0" and not os.path.exists(marker)


def test_constructor_restore_is_read_only():
    import inspect
    from core.runtime_controller import RuntimeController
    assert "restore(reconcile=False)" in inspect.getsource(RuntimeController.__init__)
    assert "restore(reconcile=False)" in inspect.getsource(RuntimeController._hard_stop)
    assert "restore(reconcile=True)" in inspect.getsource(RuntimeController._reload_drawdown_state)


# ----------------------------------------------------------------------------------- Codex su 58e635e6 (PR-03a)
def test_stop_without_owning_the_session_never_writes_its_closure(tmp_path):
    """Codex su 58e635e6: un controller mai avviato (o un secondo STOP dopo il rilascio) non chiude la sessione del
    proprietario attuale: picco 120, superamento e sessione aperta di A restano su disco."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    altro = DrawdownHardStop(c.rc.db)                      # proprietario attuale
    altro.restore()
    altro.open_session()
    altro.observe(120.0)
    assert altro.evaluate(10.0, 105.0) is True
    _stop_pulito(c)                                        # STOP di un controller che non ha aperto la sessione
    record = json.loads(c.rc.db.get_settings()[STATE_KEY])
    assert record["peak"] == "120.0" and record["breached"] is True and record["session_open"] is True


def test_second_stop_does_not_close_again(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    _lettura(c, 100.0)
    c.rc._open_drawdown_session()
    chiusure = []
    stop = c.rc._hard_stop()
    real_close = stop.close_session
    monkeypatch.setattr(stop, "close_session", lambda: chiusure.append(1) or real_close())
    _stop_pulito(c)
    _stop_pulito(c)
    assert chiusure == [1]


def test_entry_reading_feeds_the_hard_stop_even_with_verified_desk_cash(tmp_path):
    """Codex su 58e635e6: cassa del desk gia' verificata a 100; la lettura fresca d'ingresso dice 85 (perdita 15 %,
    limite 10 %). Prima la lettura non veniva applicata e il controllo restava su 100: ordine al broker."""
    c = _catena(tmp_path, max_drawdown_hard_stop_pct=10.0)
    _lettura(c, 100.0)
    c.rc.betfair_service.get_account_funds = lambda: {"available": 85.0, "exposure": 0.0, "ok": True}
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 0 and c.rc._hard_stop().breached
    assert c.rc._hard_stop().snapshot()["breach"]["kind"] == "max_drawdown_hard_stop_pct"


def test_controller_entry_funds_gate_rejects_a_breach_found_by_its_own_reading(tmp_path):
    """Stesso caso sul percorso manuale/Dutching: la lettura d'ingresso del controller memorizza il superamento e il
    suo gate fondi lo rifiuta."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    c.rc.betfair_service.get_account_funds = lambda: {"available": 85.0, "exposure": 0.0, "ok": True}
    lettura = c.rc.read_funds_for_controller_entry()
    assert c.rc._hard_stop().breached
    assert c.rc.controller_entry_funds_reason(lettura) == REASON_ACTIVE


def test_entry_reading_below_the_cap_admits(tmp_path):
    """Controllo PASS: lettura fresca 95 (perdita 5 %, limite 10 %) => nessun superamento, ordine ammesso."""
    c = _catena(tmp_path, max_drawdown_hard_stop_pct=10.0)
    _lettura(c, 100.0)
    c.rc.betfair_service.get_account_funds = lambda: {"available": 95.0, "exposure": 0.0, "ok": True}
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert not c.rc._hard_stop().breached and _ordini(c) == 1, _rifiuti(c)


def test_baseline_read_during_another_session_is_refreshed_after_its_clean_stop(tmp_path):
    """Codex su 58e635e6: B costruito mentre A aveva la sessione aperta (baseline provvisoriamente non verificata); A
    chiude pulito; allo START B rilegge e la baseline torna verificata. Un crash vero resta non verificato."""
    db = _Db()
    db.db_path = str(tmp_path / "pf.db")
    a = DrawdownHardStop(db)
    a.observe(100.0)
    a.open_session()
    b = DrawdownHardStop(db)
    b.restore(reconcile=False)
    assert not b.baseline_verified
    assert a.close_session()
    b.restore()
    assert b.baseline_verified and b.baseline_block_reason() == ""
    c = DrawdownHardStop(db)
    a.open_session()                                       # crash di A: sessione aperta su disco
    c.restore()
    assert not c.baseline_verified


# ----------------------------------------------------------------------------------- Codex su 53c99ceb (PR-03a)
def test_startup_reload_adopts_a_newer_durable_owner_clearance():
    """Codex su 53c99ceb: B legge alla costruzione il superamento di A; A viene sbloccato dall'owner (salvato) e si
    ferma; allo START B rilegge: vale lo sblocco con la sua storia, e l'apertura della sessione non lo sovrascrive."""
    db = _Db()
    a = DrawdownHardStop(db)
    a.observe(100.0)
    assert a.evaluate(10.0, 80.0) is True
    b = DrawdownHardStop(db)
    b.restore(reconcile=False)
    assert b.breached
    assert a.owner_clear_breach(authorization="PF-OWNER-X", reason="verificato", operation_id="op-1", confirm=True)
    b.restore()
    assert not b.breached and b.reason(10.0, 95.0) == ""
    assert b.snapshot()["breach"]["cleared"][0]["operation_id"] == "op-1"
    assert b.open_session()
    assert json.loads(db.settings[STATE_KEY])["breached"] is False


def test_reload_keeps_a_newer_unpersisted_in_memory_breach():
    """Il superamento memorizzato in questo processo con il DB giu' (e senza marker) non sparisce rileggendo un
    record vecchio non superato."""
    db = _Db()
    stop = DrawdownHardStop(db)
    stop.observe(100.0)
    db.fail_save = True
    assert stop.evaluate(10.0, 80.0) is True               # solo in memoria (nessun marker: DB senza percorso)
    db.fail_save = False
    stop.restore()
    assert stop.breached and stop.reason(10.0, 95.0) == REASON_ACTIVE


# ----------------------------------------------------------------------------------- Sol/Codex su 53c99ceb (PR-03a)
def test_entry_readings_raise_the_peak_then_block_the_drop(tmp_path):
    """Sol su 53c99ceb: picco 100, letture d'ingresso 130 poi 115, limite 10: la lettura a 130 alza (e salva) il picco
    come ogni lettura verificata, quindi 115 e' una perdita di 15 e blocca."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    c.rc.betfair_service.get_account_funds = lambda: {"available": 130.0, "exposure": 0.0, "ok": True}
    c.rc._read_funds_for_entry()
    assert c.rc._hard_stop().snapshot()["peak"] == 130.0
    assert json.loads(c.rc.db.get_settings()[STATE_KEY])["peak"] == "130.0"
    c.rc.betfair_service.get_account_funds = lambda: {"available": 115.0, "exposure": 0.0, "ok": True}
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 0 and c.rc._hard_stop().breached
    assert c.rc._hard_stop().snapshot()["breach"]["loss"] == "15.0"


def _record_del_proprietario(c):
    altro = DrawdownHardStop(c.rc.db)                      # proprietario attuale: picco 120, superamento, sessione
    altro.restore()
    altro.open_session()
    altro.observe(120.0)
    assert altro.evaluate(10.0, 105.0) is True
    return c.rc.db.get_settings()[STATE_KEY]


def test_stopped_controller_status_reading_never_writes_the_owner_record(tmp_path):
    """Codex su 53c99ceb: un controller mai avviato (STOPPED) che legge il conto per lo stato non scrive lo stato
    dell'hard-stop: il record del proprietario (picco 120, superamento, sessione aperta) resta intatto."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    c.rc.mode = c.rc.mode.__class__.STOPPED
    prima = _record_del_proprietario(c)
    fondi = {"available": 110.0, "exposure": 0.0, "ok": True}
    c.rc._apply_funds_reading(dict(fondi), source="status")
    c.rc.betfair_service.get_account_funds = lambda: dict(fondi)
    c.rc._read_funds_for_entry()
    assert c.rc.db.get_settings()[STATE_KEY] == prima


def test_start_reload_enables_drawdown_writes_before_active(tmp_path):
    """Lo START (rilettura sotto il lock) abilita le scritture gia' per la lettura dei fondi d'avvio, prima di ACTIVE;
    lo STOP le richiude."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    c.rc.mode = c.rc.mode.__class__.STOPPED
    assert not c.rc._drawdown_writes_allowed()
    c.rc._reload_drawdown_state()
    assert c.rc._drawdown_writes_allowed()
    _lettura(c, 100.0)
    assert json.loads(c.rc.db.get_settings()[STATE_KEY])["peak"] == "100.0"
    _stop_pulito(c)
    assert not c.rc._drawdown_writes_allowed()


def test_entry_reading_above_the_peak_never_compares_the_new_peak_with_older_desk_cash(tmp_path):
    """La lettura d'ingresso a 130 alza il picco ma non tocca la cassa del desk (100, PFC-008): il gate dell'hard-stop
    usa la lettura piu' recente, quindi nessun falso superamento (30) e l'ordine passa."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    c.rc.betfair_service.get_account_funds = lambda: {"available": 130.0, "exposure": 0.0, "ok": True}
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert c.rc.risk_desk.cash_equity_current == 100.0 and c.rc._hard_stop().snapshot()["peak"] == 130.0
    assert not c.rc._hard_stop().breached and _ordini(c) == 1, _rifiuti(c)


# ----------------------------------------------------------------------------------- Sol su 4a171465 (PR-03a)
def test_lockdown_measure_uses_the_fresher_entry_reading(tmp_path):
    """Sol su 4a171465: desk e picco a 100, lettura d'ingresso 79 (perdita 21 %), nessun hard-stop opzionale: la misura
    di DEFENSE/LOCKDOWN deve vedere il 21 %, non lo 0 % della cassa piu' vecchia del desk."""
    c = _catena(tmp_path, lockdown_drawdown_pct=20.0)
    _lettura(c, 100.0)
    c.rc.betfair_service.get_account_funds = lambda: {"available": 79.0, "exposure": 0.0, "ok": True}
    c.rc._read_funds_for_entry()
    assert round(c.rc._persistent_drawdown_pct(), 6) == 21.0
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 0, _rifiuti(c)


def test_stale_reading_arriving_late_never_records_a_false_breach(tmp_path):
    """Sol su 4a171465: una lettura iniziata prima (100) e arrivata dopo una piu' recente (130, che ha alzato il picco)
    non va valutata: altrimenti memorizzerebbe una perdita falsa di 30 con limite 10 (blocco persistente)."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    vecchia = {"available": 100.0, "exposure": 0.0, "ok": True}
    nuova = {"available": 130.0, "exposure": 0.0, "ok": True}
    t0 = 1_000_000.0
    c.rc._evaluate_entry_reading_drawdown(dict(nuova), t0 + 2.0)
    assert c.rc._hard_stop().snapshot()["peak"] == 130.0
    c.rc._evaluate_entry_reading_drawdown(dict(vecchia), t0 + 1.0)        # iniziata prima, arrivata dopo
    assert not c.rc._hard_stop().breached
    c.rc._apply_funds_reading(dict(vecchia), source="status", read_at=t0 + 1.5)  # contratto, anch'essa piu' vecchia
    assert not c.rc._hard_stop().breached


def test_restart_keeps_the_clearance_history_so_operation_ids_stay_single_use():
    """Sol su 4a171465: dopo un riavvio con superamento gia' sbloccato, la storia degli sblocchi resta: un nuovo
    superamento non puo' essere sbloccato riusando lo stesso operation_id."""
    db = _Db()
    stop = DrawdownHardStop(db)
    stop.observe(100.0)
    assert stop.evaluate(10.0, 80.0)
    assert stop.owner_clear_breach(authorization="PF-OWNER-X", reason="ok", operation_id="op-1", confirm=True)
    riavviato = DrawdownHardStop(db)
    riavviato.restore()
    assert riavviato.snapshot()["breach"]["cleared"][0]["operation_id"] == "op-1"
    assert riavviato.evaluate(10.0, 70.0)
    assert riavviato.owner_clear_breach(authorization="PF-OWNER-X", reason="ok", operation_id="op-1",
                                        confirm=True) is False
    assert riavviato.breached


# ----------------------------------------------------------------------------------- Codex su 4a171465 (PR-03a)
def test_profitable_entry_reading_never_puts_the_desk_in_lockdown(tmp_path):
    """Codex su 4a171465: desk 100, lettura d'ingresso 130 (picco 130, cassa del desk ancora 100). La misura del desk usa
    la cassa piu' recente: nessun drawdown fittizio del 23 %, nessun LOCKDOWN, il segnale successivo passa."""
    c = _catena(tmp_path, lockdown_drawdown_pct=20.0)
    _lettura(c, 100.0)
    c.rc.betfair_service.get_account_funds = lambda: {"available": 130.0, "exposure": 0.0, "ok": True}
    c.rc._read_funds_for_entry()
    assert c.rc._hard_stop().snapshot()["peak"] == 130.0
    assert c.rc._persistent_drawdown_pct() == 0.0
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 1, _rifiuti(c)


def test_restore_honors_a_newer_durable_owner_rebase():
    """Codex su 4a171465: B carica il picco 100; A ribasa durevolmente a 85 (procedura owner); allo START B rilegge e
    vale 85 con la sua registrazione, invece del picco vecchio: a 85 con limite 10 nessun superamento falso."""
    db = _Db()
    a = DrawdownHardStop(db)
    a.observe(100.0)
    b = DrawdownHardStop(db)
    b.restore(reconcile=False)
    assert b.snapshot()["peak"] == 100.0
    assert a.owner_rebase_peak(85.0, authorization="PF-OWNER-X", reason="nuova baseline", operation_id="op-r",
                               confirm=True)
    b.restore()
    assert b.snapshot()["peak"] == 85.0 and b.snapshot()["breach"]["rebased"]["operation_id"] == "op-r"
    assert b.reason(10.0, 85.0) == "" and not b.breached


def test_restore_still_keeps_a_newer_in_memory_peak():
    """Un picco piu' alto osservato in questo processo e non ancora superato da un record piu' recente resta (START
    ripetuto nello stesso processo)."""
    db = _Db()
    stop = DrawdownHardStop(db)
    stop.observe(100.0)
    db.fail_save = True
    stop.observe(120.0)                                       # solo in memoria (DB giu', nessun marker)
    db.fail_save = False
    stop.restore()
    assert stop.snapshot()["peak"] == 120.0


# ----------------------------------------------------------------------------------- Sol su 6d4ac43f (PR-03a)
def test_concurrent_readings_never_interleave_check_and_evaluation(tmp_path):
    """Sol su 6d4ac43f: A (100, piu' vecchia) supera il controllo dell'istante; prima che valuti, B (130, piu' nuova)
    alza il picco; poi A valuta 100 contro 130 => perdita falsa di 30 con limite 10. Con la sezione critica B aspetta
    A: A valuta contro 100 (nessuna perdita), poi B alza il picco. Nessun superamento."""
    import threading
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    stop = c.rc._hard_stop()
    reale = stop.evaluate
    a_dentro, via = threading.Event(), threading.Event()

    def evaluate(cap, cash, **kw):
        if float(cash) == 100.0 and not via.is_set():
            a_dentro.set()
            via.wait(5)
        return reale(cap, cash, **kw)

    stop.evaluate = evaluate
    t0 = 1_000_000.0
    a = threading.Thread(target=c.rc._evaluate_entry_reading_drawdown,
                         args=({"available": 100.0, "exposure": 0.0, "ok": True}, t0 + 1.0))
    b = threading.Thread(target=c.rc._evaluate_entry_reading_drawdown,
                         args=({"available": 130.0, "exposure": 0.0, "ok": True}, t0 + 2.0))
    a.start()
    assert a_dentro.wait(5)
    b.start()
    b.join(0.3)
    via.set()
    a.join(5)
    b.join(5)
    assert not stop.breached and stop.snapshot()["peak"] == 130.0


def test_restore_follows_the_rebase_epoch_even_after_the_peak_rises_again():
    """Sol su 6d4ac43f: B carica 100; A ribasa a 85 e poi osserva 90; allo START B rilegge 90 (epoca del ribasamento),
    non il vecchio 100: a 90 con limite 10 nessun superamento falso."""
    db = _Db()
    a = DrawdownHardStop(db)
    a.observe(100.0)
    b = DrawdownHardStop(db)
    b.restore(reconcile=False)
    assert a.owner_rebase_peak(85.0, authorization="PF-OWNER-X", reason="nuova baseline", operation_id="op-r",
                               confirm=True)
    a.observe(90.0)
    b.restore()
    assert b.snapshot()["peak"] == 90.0 and b.peak_epoch == 1
    assert b.reason(10.0, 90.0) == "" and not b.breached


def test_same_epoch_keeps_the_highest_peak_across_db_and_marker(tmp_path):
    """Claude D: dentro la stessa epoca vale il massimo di tutti i record (anche un marker superato con picco piu'
    alto non abbassa il picco)."""
    db = _Db()
    db.db_path = str(tmp_path / "pf.db")
    with open(db.db_path + ".drawdown_peak_pending.json", "w", encoding="utf-8") as fh:
        fh.write(json.dumps({"peak": "130", "seq": 1}))
    db.settings[STATE_KEY] = json.dumps({"peak": "100", "seq": 2})
    stop = DrawdownHardStop(db)
    stop.restore(reconcile=False)
    assert stop.snapshot()["peak"] == 130.0


def test_failed_rebase_does_not_advance_the_epoch():
    db = _Db()
    stop = DrawdownHardStop(db)
    stop.observe(100.0)
    db.fail_save = True
    assert stop.owner_rebase_peak(85.0, authorization="PF-OWNER-X", reason="x", operation_id="op",
                                  confirm=True) is False
    assert stop.peak_epoch == 0 and stop.snapshot()["peak"] == 100.0


# ----------------------------------------------------------------------------------- Grok su 6d4ac43f (PR-03a)
def test_non_owner_controller_entry_gates_never_write_and_deny(tmp_path):
    """Grok su 6d4ac43f: un controller non proprietario (mai avviato) che passa dal gate manuale/Dutching non scrive lo
    stato dell'hard-stop (ne' via `reason`, ne' via `retry_persist`) e nega l'ingresso."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    c.rc.mode = c.rc.mode.__class__.STOPPED
    prima = _record_del_proprietario(c)
    assert c.rc.absolute_cap_reason_for_orders([dict(_ORDINE_MANUALE)]) == "drawdown_controller_non_proprietario"
    assert c.rc._drawdown_hard_stop_reason() == "drawdown_controller_non_proprietario"
    c.rc._hard_stop().persist_failed = True                # retry_persist scriverebbe: non ritenta, nega
    assert c.rc.absolute_cap_reason_for_orders([dict(_ORDINE_MANUALE)]) == "drawdown_controller_non_proprietario"
    assert c.rc._drawdown_persist_block_reason() == "drawdown_picco_non_persistito"
    assert c.rc.db.get_settings()[STATE_KEY] == prima


# ----------------------------------------------------------------------------------- Sol su b9cbe0d7 (PR-03a)
def test_gate_cash_selection_and_evaluation_are_atomic(tmp_path):
    """Sol su b9cbe0d7: il gate sceglie la cassa (100); prima che valuti, una lettura d'ingresso concorrente a 130 alza
    il picco; il gate valuterebbe 100 contro 130 => perdita falsa di 30 con limite 10. Nella sezione critica la lettura
    aspetta il gate: nessun superamento."""
    import threading
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    reale = c.rc._drawdown_gate_cash
    scelta, via = threading.Event(), threading.Event()

    def gate_cash():
        valore = reale()
        scelta.set()
        via.wait(5)
        return valore

    c.rc._drawdown_gate_cash = gate_cash
    a = threading.Thread(target=c.rc._drawdown_hard_stop_reason)
    b = threading.Thread(target=c.rc._evaluate_entry_reading_drawdown,
                         args=({"available": 130.0, "exposure": 0.0, "ok": True}, 1_000_000.0))
    a.start()
    assert scelta.wait(5)
    b.start()
    b.join(0.3)
    via.set()
    a.join(5)
    b.join(5)
    stop = c.rc._hard_stop()
    assert not stop.breached and stop.snapshot()["peak"] == 130.0


# ----------------------------------------------------------------------------------- Grok/Codex su b9cbe0d7 (PR-03a)
def test_non_owner_manual_entry_is_denied_even_without_drawdown_caps(tmp_path):
    """Grok su b9cbe0d7: senza alcun limite configurato il gate manuale/Dutching usciva prima di controllare la
    proprieta': un controller non proprietario deve comunque negare, senza scrivere."""
    c = _catena(tmp_path)
    _lettura(c, 100.0)
    c.rc.mode = c.rc.mode.__class__.STOPPED
    prima = _record_del_proprietario(c)
    assert c.rc.absolute_cap_reason_for_orders([dict(_ORDINE_MANUALE)]) == "drawdown_controller_non_proprietario"
    assert c.rc.db.get_settings()[STATE_KEY] == prima


def test_owner_rebase_operation_id_is_single_use_across_restart():
    """Grok su b9cbe0d7: l'operation_id del ribasamento owner e' monouso, anche dopo un riavvio: un replay non
    riabbassa il picco ne' avanza l'epoca."""
    db = _Db()
    stop = DrawdownHardStop(db)
    stop.observe(100.0)
    assert stop.owner_rebase_peak(85.0, authorization="PF-OWNER-X", reason="baseline", operation_id="op-r",
                                  confirm=True)
    stop.observe(120.0)
    assert stop.owner_rebase_peak(85.0, authorization="PF-OWNER-X", reason="baseline", operation_id="op-r",
                                  confirm=True) is False
    assert stop.snapshot()["peak"] == 120.0 and stop.peak_epoch == 1
    riavviato = DrawdownHardStop(db)
    riavviato.restore()
    assert riavviato.owner_rebase_peak(85.0, authorization="PF-OWNER-X", reason="baseline", operation_id="op-r",
                                       confirm=True) is False
    assert riavviato.snapshot()["peak"] == 120.0


def test_exact_threshold_with_committed_funds_triggers_in_exact_decimal(tmp_path):
    """Codex su b9cbe0d7: picco 90.02, disponibile 80.01 + impegnato 0.01 = 80.02, limite 10: la perdita e' esattamente
    10 e deve bloccare (in float sarebbe 9.99999999999999 e passerebbe)."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 90.02)
    _lettura(c, 80.01, committed=0.01)
    assert c.rc._hard_stop().breached
    assert c.rc._hard_stop().snapshot()["breach"]["loss"] == "10.00"


# ----------------------------------------------------------------------------------- Codex su 2a847087 (PR-03a)
def test_persistent_drawdown_pct_selection_and_calculation_are_atomic(tmp_path):
    """Codex su 2a847087: la misura di DEFENSE/LOCKDOWN sceglie la cassa (100) e prima di calcolare una lettura
    concorrente a 130 alza il picco: 100 contro 130 darebbe un LOCKDOWN fittizio. Nella sezione critica: 0 %."""
    import threading
    c = _catena(tmp_path, lockdown_drawdown_pct=20.0)
    _lettura(c, 100.0)
    reale = c.rc._drawdown_gate_cash
    scelta, via = threading.Event(), threading.Event()
    esito = {}

    def gate_cash():
        valore = reale()
        scelta.set()
        via.wait(5)
        return valore

    c.rc._drawdown_gate_cash = gate_cash
    a = threading.Thread(target=lambda: esito.setdefault("pct", c.rc._persistent_drawdown_pct()))
    b = threading.Thread(target=c.rc._evaluate_entry_reading_drawdown,
                         args=({"available": 130.0, "exposure": 0.0, "ok": True}, 1_000_000.0))
    a.start()
    assert scelta.wait(5)
    b.start()
    b.join(0.3)
    via.set()
    a.join(5)
    b.join(5)
    assert esito["pct"] == 0.0 and c.rc._hard_stop().snapshot()["peak"] == 130.0


# ----------------------------------------------------------------------------------- Sol su 2a847087 (PR-03a)
def test_owner_procedures_require_runtime_ownership(tmp_path):
    """Sol su 2a847087: un controller fermo/non proprietario con stato vecchio non esegue sblocco ne' conferma della
    baseline: sovrascriverebbe picco e sessione del runtime attivo."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    _lettura(c, 89.0)                                       # superamento
    c.rc.mode = c.rc.mode.__class__.STOPPED
    prima = c.rc.db.get_settings()[STATE_KEY]
    assert c.rc.owner_unlock_drawdown_hard_stop(**_OWNER) == dict(unlocked=False,
                                                                 reason="drawdown_controller_non_proprietario",
                                                                 checks={})
    assert c.rc.owner_confirm_drawdown_baseline(**_CONFIRM)["reason"] == "drawdown_controller_non_proprietario"
    assert c.rc.db.get_settings()[STATE_KEY] == prima


def test_owner_unlock_reloads_the_durable_state_first(tmp_path):
    """Lo sblocco rilegge prima lo stato durevole sotto il lock: un picco piu' alto salvato nel frattempo (stessa epoca)
    non viene sovrascritto da quello vecchio in memoria."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    _lettura(c, 89.0)
    altro = DrawdownHardStop(c.rc.db)
    altro.restore()
    altro.observe(150.0)
    esito = c.rc.owner_unlock_drawdown_hard_stop(**_OWNER)
    assert esito["unlocked"] is True
    assert json.loads(c.rc.db.get_settings()[STATE_KEY])["peak"] == "150.0"


def test_stop_waits_for_an_in_flight_drawdown_write(tmp_path, monkeypatch):
    """Sol su 2a847087: una valutazione gia' ammessa (proprieta' verificata) non scrive dopo lo STOP e il rilascio del
    lock d'istanza: lo STOP aspetta che finisca prima di smontare e rilasciare."""
    import threading
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    calls = _ordine_stop(c, monkeypatch)
    stop = c.rc._hard_stop()
    reale = stop.evaluate
    dentro, via = threading.Event(), threading.Event()

    def evaluate(cap, cash, **kw):
        dentro.set()
        via.wait(5)
        esito = reale(cap, cash, **kw)
        calls.append("drawdown_write")
        return esito

    monkeypatch.setattr(stop, "evaluate", evaluate)
    a = threading.Thread(target=c.rc._evaluate_drawdown_hard_stop, args=(95.0, 1_000_000.0))
    a.start()
    assert dentro.wait(5)
    b = threading.Thread(target=c.rc._stop_serialized)
    b.start()
    b.join(0.3)
    assert "release_lock" not in calls                      # lo STOP aspetta la scrittura in corso
    via.set()
    a.join(5)
    b.join(5)
    assert calls.index("drawdown_write") < calls.index("release_lock")


# ----------------------------------------------------------------------------------- audit finale di B (ciclo 8)
def test_reload_gives_ownership_only_after_the_state_is_reloaded(tmp_path, monkeypatch):
    """Durante la rilettura dello START il controller non e' ancora proprietario: una lettura del conto concorrente non
    puo' scrivere lo stato vecchio prima che la rilettura sia finita."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    c.rc.mode = c.rc.mode.__class__.STOPPED
    stop = c.rc._hard_stop()
    reale = stop.restore
    visto = []

    def restore(**kw):
        visto.append(c.rc._drawdown_writes_allowed())
        return reale(**kw)

    monkeypatch.setattr(stop, "restore", restore)
    c.rc._reload_drawdown_state()
    assert visto == [False] and c.rc._drawdown_writes_allowed()


def test_failed_start_closes_the_session_and_ends_ownership(tmp_path):
    """Avvio fallito dopo l'apertura della sessione (es. Telegram non raggiungibile), servizi smontati: il trading non
    e' partito, quindi la sessione si chiude e il riavvio NON chiede la conferma owner della baseline."""
    c = _catena(tmp_path)
    c.rc.mode = c.rc.mode.__class__.STOPPED
    c.rc._reload_drawdown_state()
    _lettura(c, 100.0)
    c.rc._open_drawdown_session()
    assert json.loads(c.rc.db.get_settings()[STATE_KEY])["session_open"] is True
    c.rc._end_failed_start_drawdown()
    assert json.loads(c.rc.db.get_settings()[STATE_KEY])["session_open"] is False
    assert not c.rc._drawdown_writes_allowed()
    _riavvio(c)
    assert c.rc._hard_stop().baseline_verified


def test_failed_start_path_is_wired_before_the_lock_release():
    import inspect
    from core.runtime_controller import RuntimeController
    src = inspect.getsource(RuntimeController._start_serialized)
    assert src.count("self._end_failed_start_drawdown()") == 2
    for blocco in src.split("self._end_failed_start_drawdown()")[1:]:
        assert blocco.lstrip().startswith("self._release_runtime_instance_lock_if_stopped()")


def test_never_started_controller_out_of_stopped_does_not_write_under_a_foreign_lease(tmp_path):
    """Un controller mai avviato portato fuori da STOPPED (pause()) mentre un altro runtime tiene la lease nel database
    non e' proprietario: nessuna scrittura dello stato, ingresso manuale negato. D-631-18 (Codex su 9c9121f0): lo resta
    anche dopo che quel runtime ha rilasciato la lease o l'ha lasciata scadere (prima era ammesso: il suo snapshot vecchio
    cancellava il superamento). Solo un database mai governato da un runtime (nessuna lease) resta scrivibile."""
    import time as _time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    prima = c.rc.db.get_settings()[STATE_KEY]
    lease = {"expires_at": _time.time() + 60.0, "owner_id": "altro", "fencing_token": 7}
    c.rc.db.read_runtime_lease = lambda: dict(lease)
    c.rc.mode = c.rc.mode.__class__.PAUSED
    _fondi = {"available": 150.0, "exposure": 0.0, "ok": True}
    c.rc._apply_funds_reading(dict(_fondi), source="status")
    assert c.rc.db.get_settings()[STATE_KEY] == prima
    assert c.rc.absolute_cap_reason_for_orders([dict(_ORDINE_MANUALE)]) == "drawdown_controller_non_proprietario"
    lease["expires_at"] = _time.time() - 5.0                 # lease scaduta o rilasciata: non e' comunque sua
    assert not c.rc._drawdown_writes_allowed()
    c.rc.db.read_runtime_lease = lambda: None                 # nessun runtime ha mai governato questo DB
    assert c.rc._drawdown_writes_allowed()

    def illeggibile():
        raise OSError("db")

    c.rc.db.read_runtime_lease = illeggibile                 # lease illeggibile: fail-closed
    assert not c.rc._drawdown_writes_allowed()


def test_owner_procedures_verify_inside_the_drawdown_lock_without_the_entry_locks(tmp_path):
    """D-631-09: le verifiche owner (tavoli, ordini incerti) si leggono DENTRO la sezione critica dell'hard-stop, cosi'
    verifiche e transizione sono atomiche. Nessuna inversione d'ordine con i gate d'ingresso (ammissione / stato del
    conto -> hard-stop): la procedura owner non prende mai quei due lock, quindi si completa anche mentre un altro
    thread li tiene."""
    import threading
    from core.runtime_controller import RuntimeController
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    _lettura(c, 89.0)
    libero = []
    reale = c.rc.table_manager.total_exposure

    def total_exposure():
        def prova():
            preso = RuntimeController._drawdown_eval_lock.acquire(timeout=0.3)
            libero.append(preso)
            if preso:
                RuntimeController._drawdown_eval_lock.release()
        t = threading.Thread(target=prova)
        t.start()
        t.join(5)
        return reale()

    c.rc.table_manager.total_exposure = total_exposure
    ammissione = getattr(c.rc, "_risk_admission_lock", RuntimeController._fallback_risk_admission_lock)
    tenuti, via = threading.Event(), threading.Event()

    def tiene_i_lock_d_ingresso():
        with ammissione, c.rc._account_state_lock:
            tenuti.set()
            via.wait(20)

    t = threading.Thread(target=tiene_i_lock_d_ingresso)
    t.start()
    try:
        assert tenuti.wait(5)
        esito = {}
        owner = threading.Thread(target=lambda: esito.update(c.rc.owner_unlock_drawdown_hard_stop(**_OWNER)))
        owner.start()
        owner.join(10)
        assert not owner.is_alive(), "la procedura owner attende un lock d'ingresso: inversione d'ordine"
    finally:
        via.set()
        t.join(5)
    assert esito["unlocked"] is True
    assert libero == [False]                                  # verifiche dentro la sezione critica


def test_restore_never_mistakes_its_own_open_session_for_a_crash():
    """Rilettura durante la propria sessione attiva (procedura owner): la sessione aperta su disco e' la nostra, non un
    crash: la baseline resta verificata. Un record piu' recente di un altro processo con sessione aperta vale."""
    db = _Db()
    stop = DrawdownHardStop(db)
    stop.observe(100.0)
    assert stop.open_session()
    stop.restore()
    assert stop.baseline_verified and stop.baseline_block_reason() == ""
    altro = DrawdownHardStop(db)
    altro.restore(reconcile=False)
    assert not altro.baseline_verified                       # per un altro processo e' una sessione non chiusa


def test_owner_unlock_during_the_active_session_does_not_block_the_baseline(tmp_path):
    """Flusso reale: START (sessione aperta), superamento, sblocco owner. Dopo lo sblocco l'ingresso passa: la
    rilettura dello sblocco non trasforma la sessione attiva in una baseline non verificata."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    c.rc._open_drawdown_session()
    _lettura(c, 89.0)
    assert c.rc._hard_stop().breached
    _lettura(c, 95.0)
    assert c.rc.owner_unlock_drawdown_hard_stop(**_OWNER)["unlocked"] is True
    assert c.rc._hard_stop().baseline_verified
    c.rc.mode = c.rc.mode.__class__.ACTIVE
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 1, _rifiuti(c)


# =============================================================================================================
# Prove end-to-end con il broker Betfair simulato (richiesta owner su #631): percorso reale
# segnale / DutchingController -> RuntimeController -> TradingEngine -> SimulationBroker. Nessuna rete, nessuna VPS.
# =============================================================================================================
def _e2e_dutching(c):
    from tests.acceptance.test_issue523_dutching_manual_bet import _dutching
    return _dutching(c)


def test_e2e_scenario_1_2_capital_100_cap_10_drop_to_90_blocks_the_signal_at_the_broker(tmp_path):
    """Scenari 1 e 2: capitale 100, limite 10. A 90,01 l'ordine arriva al broker (PASS); a 90 l'hard-stop scatta, il
    runtime va in LOCKDOWN e il segnale Roserpina successivo non produce alcun ordine al broker (BLOCK)."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    _lettura(c, 90.01)
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 1 and len(c.bus.payloads("CMD_QUICK_BET")) == 1
    _lettura(c, 90.0)
    assert c.rc._hard_stop().breached and c.rc.mode.name == "LOCKDOWN"
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK", market_id="1.999"))
    assert _ordini(c) == 1 and len(c.bus.payloads("CMD_QUICK_BET")) == 1      # nessun nuovo ordine


def test_e2e_scenario_3_manual_bet_and_dutching_send_nothing_with_the_hard_stop_active(tmp_path):
    """Scenario 3: stesso conto e stessi ingressi. Senza superamento la bet manuale e il lotto Dutching arrivano al
    broker (PASS); con l'hard-stop attivo nessuno dei due pubblica o piazza nulla (BLOCK)."""
    from tests.acceptance.test_issue523_dutching_manual_bet import _batch, _manual
    ok = _catena(tmp_path / "ok", max_drawdown_abs=10.0)
    _lettura(ok, 1000.0)
    assert _e2e_dutching(ok).manual_bet(_manual(stake=4.0))["ok"] is True
    assert _ordini(ok) == 1
    ok2 = _catena(tmp_path / "ok2", max_drawdown_abs=10.0)
    _lettura(ok2, 1000.0)
    assert _e2e_dutching(ok2).submit_dutching(_batch(total_stake=10.0))["ok"] is True
    assert _ordini(ok2) >= 2

    for ingresso in ("manuale", "dutching"):
        c = _catena(tmp_path / ingresso, max_drawdown_abs=10.0)
        _lettura(c, 1000.0)
        _lettura(c, 985.0)                                   # perdita 15 >= 10: hard-stop
        assert c.rc._hard_stop().breached
        c.rc.mode = c.rc.mode.__class__.ACTIVE               # anche a runtime riattivato il blocco resta
        dc = _e2e_dutching(c)
        esito = dc.manual_bet(_manual(stake=4.0)) if ingresso == "manuale" \
            else dc.submit_dutching(_batch(total_stake=10.0))
        assert esito["ok"] is False, esito
        assert _ordini(c) == 0 and c.bus.payloads("CMD_QUICK_BET") == []


def test_e2e_scenario_4_crash_and_restart_on_a_new_controller_keep_the_block(tmp_path):
    """Scenario 4: superamento, poi crash (nessuno STOP) e riavvio con un controller NUOVO sullo stesso database: il
    blocco e' ancora li', il segnale non arriva al broker, e resta anche con il conto tornato sopra la soglia."""
    from tests.integration.test_issue461_pr26a_customer_ref_provenance import _chain
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    c.rc._open_drawdown_session()
    _lettura(c, 85.0)
    assert c.rc._hard_stop().breached
    # crash: nessuno STOP, quindi nessuna chiusura della sessione su disco

    nuovo = _chain(tmp_path, anti_duplication=True)          # stesso file di database
    del nuovo.rc.mm.calculate
    nuovo.rc.config.max_drawdown_abs = 10.0
    stop = nuovo.rc._hard_stop()
    assert stop.breached and stop.snapshot()["peak"] == 100.0 and not stop.baseline_verified
    nuovo.rc._reload_drawdown_state()
    _lettura(nuovo, 99.0)                                    # conto tornato su: il blocco resta
    nuovo.rc.mode = nuovo.rc.mode.__class__.ACTIVE
    nuovo.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(nuovo) == 0 and nuovo.rc._hard_stop().breached


def _catena_su(tmp_path, nome, **cfg):
    from tests.integration.test_issue461_pr26a_customer_ref_provenance import _chain
    c = _chain(tmp_path, anti_duplication=True, db_name=nome)
    del c.rc.mm.calculate
    for chiave, valore in cfg.items():
        setattr(c.rc.config, chiave, valore)
    return c


def _avvia_con_autorita(c):
    """Le stesse chiamate di `start()` per il drawdown: lock OS, lease con fencing, rilettura dello stato, sessione."""
    assert c.rc._acquire_runtime_instance_lock() == ""
    assert c.rc._acquire_runtime_lease() == ""
    c.rc._reload_drawdown_state()


def test_e2e_scenario_5_full_sequence_start_authority_trade_crash_restart_and_stale_instances(tmp_path, monkeypatch):
    """Scenario 5 e sequenza completa: avvio con autorita' reale (lock OS + lease + fence) -> operazione -> secondo
    controller concorrente -> superamento -> crash -> subentro di un nuovo runtime -> istanza vecchia ancora viva.
    Solo il proprietario aggiorna lo stato finanziario; nessuna istanza obsoleta lo modifica o piazza ordini."""
    import time as _time
    from core.runtime_controller import RuntimeController
    monkeypatch.setattr(RuntimeController, "RUNTIME_LEASE_TTL_SEC", 1.0)
    monkeypatch.setattr(RuntimeController, "RUNTIME_LEASE_HEARTBEAT_SEC", 0.3)
    stato = lambda app: json.loads(app.db.get_settings()[STATE_KEY])

    # --- A: avvio, autorita', sessione, prima operazione
    a = _catena_su(tmp_path, "seq.db", max_drawdown_abs=10.0)
    _avvia_con_autorita(a)
    _lettura(a, 100.0)
    a.rc._open_drawdown_session()
    a.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(a) == 1 and stato(a)["peak"] == "100.0" and stato(a)["session_open"] is True

    # --- B: secondo controller concorrente sullo stesso database, avvio rifiutato
    b = _catena_su(tmp_path, "seq.db", max_drawdown_abs=10.0)
    assert b.rc._acquire_runtime_instance_lock() != ""       # il lock e' di A
    prima = a.db.get_settings()[STATE_KEY]
    b.rc.betfair_service.get_account_funds = lambda: {"available": 150.0, "exposure": 0.0, "ok": True}
    b.rc._apply_funds_reading({"available": 150.0, "exposure": 0.0, "ok": True}, source="status")
    b.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK", market_id="1.777"))
    assert a.db.get_settings()[STATE_KEY] == prima           # B non ha scritto nulla
    assert _ordini(b) == 0
    assert b.rc.owner_unlock_drawdown_hard_stop(**_OWNER)["unlocked"] is False

    # --- A: superamento => blocco persistente, nessun ordine da nessun percorso
    _lettura(a, 89.0)
    assert stato(a)["breached"] is True and a.rc.mode.name == "LOCKDOWN"
    a.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK", market_id="1.888"))
    assert _ordini(a) == 1

    # --- crash di A (nessuno STOP): heartbeat fermo, lock sparito, lease scaduta
    a.rc.runtime_authority.stop_heartbeat()
    import os as _os
    _os.unlink(_os.path.normcase(_os.path.realpath(str(tmp_path / "seq.db"))) + ".runtime.lock")
    _time.sleep(1.3)

    # --- C: nuovo runtime subentra; blocco e sessione non chiusa sono ancora li'
    c = _catena_su(tmp_path, "seq.db", max_drawdown_abs=10.0)
    _avvia_con_autorita(c)
    assert c.rc.runtime_authority.token > a.rc.runtime_authority.token
    stop_c = c.rc._hard_stop()
    assert stop_c.breached and stop_c.snapshot()["peak"] == 100.0 and not stop_c.baseline_verified
    _lettura(c, 95.0)
    c.rc.mode = c.rc.mode.__class__.ACTIVE
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK", market_id="1.555"))
    assert _ordini(c) == 0

    # --- A, obsoleta ma viva: non modifica lo stato finanziario e non piazza
    prima = c.db.get_settings()[STATE_KEY]
    a.rc.betfair_service.get_account_funds = lambda: {"available": 300.0, "exposure": 0.0, "ok": True}
    a.rc._apply_funds_reading({"available": 300.0, "exposure": 0.0, "ok": True}, source="status")
    a.rc._read_funds_for_entry()
    assert a.rc.owner_unlock_drawdown_hard_stop(**_OWNER)["unlocked"] is False
    a.rc.mode = a.rc.mode.__class__.ACTIVE
    a.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK", market_id="1.444"))
    a.rc._close_drawdown_session()                           # STOP dell'istanza obsoleta: nessuna chiusura scritta
    assert c.db.get_settings()[STATE_KEY] == prima and _ordini(a) == 1

    # --- C: procedure owner del proprietario (conferma baseline + sblocco), poi l'ingresso torna possibile
    assert c.rc.owner_confirm_drawdown_baseline(**_CONFIRM)["confirmed"] is True
    assert c.rc.owner_unlock_drawdown_hard_stop(**_OWNER)["unlocked"] is True
    c.rc.mode = c.rc.mode.__class__.ACTIVE
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK", market_id="1.333"))
    assert _ordini(c) == 1, _rifiuti(c)                      # perdita 5 dal picco 100: sotto il limite
    c.rc.runtime_authority.stop_heartbeat()


# ----------------------------------------------------------------------------------- Sol su 96b03d7e (PR-03a)
def test_gate_cash_never_pairs_an_old_desk_cash_with_a_newer_desk_timestamp(tmp_path):
    """Sol su 96b03d7e: il gate leggeva la cassa del desk e poi, separatamente, il suo istante. Un aggiornamento
    concorrente del desk fra le due letture associava la cassa vecchia (100) a un istante nuovo: scelta al posto della
    lettura d'ingresso (130, che ha gia' alzato il picco) => perdita falsa di 30 e blocco persistente.
    Il gate usa l'ultima cassa valutata insieme al picco, nella stessa sezione critica."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    c.rc._evaluate_entry_reading_drawdown({"available": 130.0, "exposure": 0.0, "ok": True}, 1_000_000.0)
    vero = c.rc.risk_desk

    class _DeskStrappato:
        """Lettura strappata: restituisce la cassa vecchia e, subito dopo, il desk risulta aggiornato piu' tardi."""
        def __getattr__(self, nome):
            if nome == "cash_equity_current":
                valore = vero.cash_equity_current
                vero._cash_read_at = 2_000_000.0          # il sync concorrente finisce qui, fra le due letture
                return valore
            return getattr(vero, nome)

    c.rc.risk_desk = _DeskStrappato()
    assert c.rc._drawdown_hard_stop_reason() == ""
    assert not c.rc._hard_stop().breached


def test_gate_cash_stays_unverified_when_the_desk_cash_is_unverified(tmp_path):
    """La cassa valutata non sostituisce mai una cassa del desk non verificata (fail-closed)."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    c.rc._apply_funds_reading({"error": "x"}, source="test")
    assert c.rc._drawdown_gate_cash() is None
    assert c.rc._drawdown_hard_stop_reason() == REASON_CASH_UNVERIFIED


def test_stale_instance_stop_never_writes_the_session_closure(tmp_path):
    """Sol su 96b03d7e: lo STOP di un'istanza che ha perso l'autorita' (subentro di un altro runtime) non scrive la
    chiusura della sessione. Con il database reale la scrittura e' gia' respinta dal fence (prova nella sequenza
    e2e); qui il controllo e' esplicito anche senza fence."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    c.rc._open_drawdown_session()
    prima = c.rc.db.get_settings()[STATE_KEY]
    c.rc._authority_lost_reason = "lease_superseded"         # autorita' persa (subentro)
    c.rc._close_drawdown_session()
    assert c.rc.db.get_settings()[STATE_KEY] == prima
    assert json.loads(prima)["session_open"] is True


# ----------------------------------------------------------------------------------- Sol su e98e84c7 (PR-03a)
def test_stale_marker_with_a_higher_peak_reaches_the_db_before_being_removed(tmp_path):
    """Sol su e98e84c7: marker con `seq` inferiore al DB ma picco piu' alto nella stessa epoca. Il picco recuperato
    va salvato nel DB PRIMA di rimuovere il marker: altrimenti un crash prima del salvataggio successivo lo perde e il
    riavvio misura il drawdown da un picco piu' basso."""
    db, stop = _stop_con_marker(tmp_path)
    stop.observe(100.0)
    db.fail_save = True
    stop.observe(120.0)                                      # picco 120 solo nel marker (seq 2)
    db.fail_save = False
    marker = db.db_path + ".drawdown_peak_pending.json"
    assert json.loads(open(marker).read())["peak"] == "120.0"
    # un record del DB piu' recente (seq piu' alto) con il picco vecchio, stessa epoca
    record = json.loads(db.settings[STATE_KEY])
    record.update(seq=json.loads(open(marker).read())["seq"] + 3)
    assert record["peak"] == "100.0"
    db.settings[STATE_KEY] = json.dumps(record)

    avvio = DrawdownHardStop(db)
    avvio.restore()                                          # riconciliazione dello START
    assert avvio.snapshot()["peak"] == 120.0
    # crash subito dopo: nessun altro salvataggio. Il riavvio deve ritrovare 120.
    assert json.loads(db.settings[STATE_KEY])["peak"] == "120.0"
    assert not os.path.exists(marker)
    riavviato = DrawdownHardStop(db)
    riavviato.restore()
    assert riavviato.snapshot()["peak"] == 120.0
    assert riavviato.reason(15.0, 104.0) == REASON_ACTIVE     # 120 - 104 = 16 >= 15; da 100 non scatterebbe


def test_stale_marker_with_a_higher_peak_survives_when_the_db_cannot_save(tmp_path):
    """Stesso stato, ma il DB non salva durante la riconciliazione: il marker non viene rimosso senza un salvataggio."""
    db, stop = _stop_con_marker(tmp_path)
    stop.observe(100.0)
    db.fail_save = True
    stop.observe(120.0)
    marker = db.db_path + ".drawdown_peak_pending.json"
    record = json.loads(db.settings[STATE_KEY])
    record.update(seq=json.loads(open(marker).read())["seq"] + 3)
    db.settings[STATE_KEY] = json.dumps(record)

    avvio = DrawdownHardStop(db)
    avvio.restore()                                          # DB ancora guasto
    assert os.path.exists(marker) and json.loads(open(marker).read())["peak"] == "120.0"
    db.fail_save = False
    riavviato = DrawdownHardStop(db)
    riavviato.restore()
    assert riavviato.snapshot()["peak"] == 120.0


def test_stale_marker_without_a_higher_peak_is_only_removed(tmp_path):
    """Il marker superato che non porta nulla in piu' si rimuove soltanto: nessuna riscrittura del record del DB."""
    db, stop = _stop_con_marker(tmp_path)
    stop.observe(100.0)
    db.fail_save = True
    stop.observe(120.0)
    db.fail_save = False
    marker = db.db_path + ".drawdown_peak_pending.json"
    record = json.loads(open(marker).read())
    record.update(seq=record["seq"] + 3, peak="130.0")       # il DB ha gia' un picco piu' alto e un seq piu' alto
    db.settings[STATE_KEY] = json.dumps(record)
    prima = db.settings[STATE_KEY]

    avvio = DrawdownHardStop(db)
    avvio.restore()
    assert avvio.snapshot()["peak"] == 130.0
    assert db.settings[STATE_KEY] == prima and not os.path.exists(marker)


def test_a_fenced_writer_never_publishes_its_state_to_the_marker(tmp_path):
    """Sol su e98e84c7: una valutazione iniziata prima della perdita d'autorita' arriva al salvataggio dopo il
    subentro. Il DB la rifiuta (fencing): l'istanza superata NON deve scrivere nemmeno il marker, che il nuovo
    proprietario leggerebbe come stato dell'hard-stop. Un guasto ordinario del DB continua a usare il marker."""
    from database import RuntimeFenceViolation

    db, stop = _stop_con_marker(tmp_path)
    stop.observe(100.0)
    prima = db.settings[STATE_KEY]
    marker = db.db_path + ".drawdown_peak_pending.json"

    def fenced(values):
        raise RuntimeFenceViolation("lease_superseded")

    db.save_settings = fenced
    stop.observe(120.0)                                      # picco dell'istanza superata
    assert stop.evaluate(10.0, 80.0)                         # e un suo superamento
    assert not os.path.exists(marker)
    assert db.settings[STATE_KEY] == prima
    assert stop.persist_failed and stop.reason(10.0, 120.0) == REASON_ACTIVE   # l'istanza superata resta negata
    del db.save_settings
    nuovo = DrawdownHardStop(db)                              # il nuovo proprietario non eredita nulla dal superato
    nuovo.restore()
    assert nuovo.snapshot()["peak"] == 100.0 and not nuovo.breached


# ------------------------------------------------- Sol/Claude D su c715c59e (D-631-08, D-631-09): cicli 11-12
# Autorizzazione owner PF-OWNER-PR631-EXTRA-2-CYCLES-20261111.
def _letture_fuori_ordine(tmp_path, *letture, **cfg):
    """Catena con hard-stop e letture `(cassa, istante relativo)` valutate nell'ordine d'ARRIVO dato."""
    import time
    c = _catena(tmp_path, **(cfg or {"max_drawdown_abs": 10.0}))
    _lettura(c, 100.0)
    base = time.monotonic() + 100.0
    for cassa, istante in letture:
        c.rc._evaluate_drawdown_hard_stop(cassa, read_at=base + istante)
    return c, base


def test_late_reading_with_a_higher_cash_still_raises_the_peak_and_blocks(tmp_path):
    """D-631-08 (P1 fail-open): 100, poi 115 (iniziata a t3), poi 130 iniziata PRIMA (t2) ma arrivata dopo. La cassa
    130 e' stata osservata davvero: il picco e' 130 e la perdita 130 -> 115 = 15 >= 10 blocca. Prima il picco restava
    115 e nessun blocco scattava."""
    c, _ = _letture_fuori_ordine(tmp_path, (100.0, 1), (115.0, 3), (130.0, 2))
    stop = c.rc._hard_stop()
    assert stop.snapshot()["peak"] == 130.0 and stop.breached
    assert stop.snapshot()["breach"]["kind"] == "max_drawdown_abs"
    assert c.rc._drawdown_last_cash == 115.0                  # la cassa corrente resta l'ultima lettura, non la tardiva
    assert str(c.rc._pending_drawdown_lockdown).startswith("drawdown_hard_stop_triggered:max_drawdown_abs:15.00")
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 0 and _bloccato_da_hard_stop(c), _rifiuti(c)
    # durevole: crash e riavvio ritrovano picco e superamento
    riavviato = DrawdownHardStop(c.rc.db)
    riavviato.restore(reconcile=False)
    assert riavviato.snapshot()["peak"] == 130.0 and riavviato.breached


def test_late_reading_with_a_higher_cash_below_the_cap_only_raises_the_peak(tmp_path):
    """Nessun falso blocco: 128 (t3) poi 130 tardiva (t2), limite 10 => picco 130, perdita 2, nessun superamento."""
    c, _ = _letture_fuori_ordine(tmp_path, (100.0, 1), (128.0, 3), (130.0, 2))
    stop = c.rc._hard_stop()
    assert stop.snapshot()["peak"] == 130.0 and not stop.breached
    assert c.rc._drawdown_last_cash == 128.0 and getattr(c.rc, "_pending_drawdown_lockdown", None) is None
    assert json.loads(c.rc.db.get_settings()[STATE_KEY])["peak"] == "130.0"
    c.rc._evaluate_drawdown_hard_stop(119.0, read_at=__import__("time").monotonic() + 1000.0)
    assert stop.breached                                      # 130 - 119 = 11 >= 10: il picco tardivo conta


def test_late_reading_with_a_lower_cash_never_creates_a_false_loss(tmp_path):
    """Sol su 4a171465, invariato: 130 (t3) poi 100 tardiva (t2) non e' una perdita di 30."""
    c, base = _letture_fuori_ordine(tmp_path, (100.0, 1), (130.0, 3), (100.0, 2))
    stop = c.rc._hard_stop()
    assert stop.snapshot()["peak"] == 130.0 and not stop.breached
    assert c.rc._drawdown_last_cash == 130.0 and c.rc._drawdown_read_at == base + 3
    assert getattr(c.rc, "_pending_drawdown_lockdown", None) is None


def test_late_reading_never_moves_the_reading_clock_backwards(tmp_path):
    """La lettura tardiva alza solo il picco: istante e cassa dell'ultima lettura valutata restano quelli."""
    c, base = _letture_fuori_ordine(tmp_path, (100.0, 1), (104.0, 5), (106.0, 2), (105.0, 4))
    assert c.rc._drawdown_read_at == base + 5 and c.rc._drawdown_last_cash == 104.0
    assert c.rc._hard_stop().snapshot()["peak"] == 106.0 and not c.rc._hard_stop().breached


def test_late_reading_from_before_an_owner_rebase_never_raises_the_new_peak(tmp_path):
    """Nessun falso blocco dopo un ribasamento owner: una lettura iniziata PRIMA del ribasamento appartiene all'epoca
    precedente del picco e non lo rialza (130 contro il nuovo picco 85 sarebbe una perdita falsa)."""
    import time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    prima = time.monotonic()
    c.rc._evaluate_drawdown_hard_stop(100.0, read_at=prima)
    assert c.rc._hard_stop().owner_rebase_peak(85.0, authorization="PF-OWNER-X", reason="nuova baseline",
                                               operation_id="op-reb-1", confirm=True)
    c.rc._evaluate_drawdown_hard_stop(84.0, read_at=time.monotonic())
    c.rc._evaluate_drawdown_hard_stop(130.0, read_at=prima + 1e-6)     # tardiva, iniziata prima del ribasamento
    stop = c.rc._hard_stop()
    assert stop.snapshot()["peak"] == 85.0 and not stop.breached
    c.rc._evaluate_drawdown_hard_stop(84.5, read_at=time.monotonic())
    c.rc._evaluate_drawdown_hard_stop(90.0, read_at=time.monotonic() - 1e-4)   # tardiva ma DOPO il ribasamento
    assert stop.snapshot()["peak"] == 90.0


def test_late_reading_of_a_non_owner_controller_changes_nothing(tmp_path):
    """Autorita' runtime: un controller che ha perso l'autorita' non alza il picco nemmeno con una lettura tardiva."""
    c, base = _letture_fuori_ordine(tmp_path, (100.0, 1), (115.0, 3))
    prima = c.rc.db.get_settings()[STATE_KEY]
    c.rc._authority_lost_reason = "lease_superseded"
    c.rc._evaluate_drawdown_hard_stop(130.0, read_at=base + 2)
    assert c.rc.db.get_settings()[STATE_KEY] == prima and c.rc._hard_stop().snapshot()["peak"] == 115.0


def test_late_reading_with_unsaved_peak_blocks_entries(tmp_path):
    """Persistenza: il picco tardivo non salvato (DB giu') nega gli ingressi e finisce nel marker."""
    c, base = _letture_fuori_ordine(tmp_path, (100.0, 1), (128.0, 3))
    real_save = c.rc.db.save_settings

    def failing_save(values):
        if STATE_KEY in values:
            raise OSError("disco pieno")
        return real_save(values)

    c.rc.db.save_settings = failing_save
    c.rc._evaluate_drawdown_hard_stop(130.0, read_at=base + 2)
    assert c.rc._hard_stop().persist_failed
    marker = c.rc.db.db_path + ".drawdown_peak_pending.json"
    assert json.loads(open(marker).read())["peak"] == "130.0"


@pytest.mark.parametrize("procedura", ["unlock", "baseline"])
def test_owner_procedure_sees_an_order_that_turns_ambiguous_during_the_operation(tmp_path, procedura):
    """D-631-09 (P2): un ordine diventa AMBIGUOUS dopo l'inizio della procedura owner (qui: durante la rilettura dello
    stato, gia' dentro la sezione critica). Le verifiche si fanno DENTRO la sezione critica, subito prima della
    transizione: lo sblocco / la conferma sono negati e nulla viene salvato. Prima le verifiche erano calcolate fuori
    e la transizione passava con `uncertain_orders == []`."""
    cfg = {"max_drawdown_abs": 10.0} if procedura == "unlock" else {"lockdown_drawdown_pct": 15.0}
    c = _catena(tmp_path, **cfg)
    _lettura(c, 100.0)
    if procedura == "unlock":
        _lettura(c, 89.0)
        assert c.rc._hard_stop().breached
    else:
        c.rc._hard_stop().open_session()
        _riavvio(c)
        _lettura(c, 105.0)
        assert not c.rc._hard_stop().baseline_verified
    stop = c.rc._hard_stop()
    reale = stop.restore

    def restore_con_ordine_ambiguo(**kw):
        c.rc._mark_uncertain("K-AMB", 1, customer_ref="r-amb")
        return reale(**kw)

    stop.restore = restore_con_ordine_ambiguo
    prima = c.rc.db.get_settings()[STATE_KEY]
    if procedura == "unlock":
        esito = c.rc.owner_unlock_drawdown_hard_stop(**_OWNER)
        assert esito["unlocked"] is False
    else:
        esito = c.rc.owner_confirm_drawdown_baseline(**_CONFIRM)
        assert esito["confirmed"] is False
    assert esito["reason"] == "ordini_incerti_presenti" and esito["checks"]["uncertain_orders"] == ["K-AMB"]
    del stop.restore
    assert stop.breached if procedura == "unlock" else not stop.baseline_verified
    dopo = json.loads(c.rc.db.get_settings()[STATE_KEY])
    assert dopo["breached"] is (procedura == "unlock") and dopo["baseline_verified"] is json.loads(prima)["baseline_verified"]
    # riavvio / altro controller: la transizione negata non e' su disco
    riletto = DrawdownHardStop(c.rc.db)
    riletto.restore(reconcile=False)
    assert riletto.breached if procedura == "unlock" else not riletto.baseline_verified


def test_an_order_cannot_turn_ambiguous_in_the_middle_of_an_owner_transition(tmp_path):
    """D-631-09: la registrazione di un ordine AMBIGUOUS e la transizione owner sono serializzate sulla stessa sezione
    critica: mentre la procedura e' fra le verifiche e il salvataggio, `_mark_uncertain` attende. L'ordine risulta
    quindi o prima delle verifiche (procedura negata) o dopo la transizione conclusa, mai in mezzo."""
    import threading
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    _lettura(c, 89.0)
    stop = c.rc._hard_stop()
    reale = stop.owner_clear_breach
    dentro, via, marcato = threading.Event(), threading.Event(), threading.Event()

    def clear_lento(**kw):
        dentro.set()                                         # verifiche fatte, salvataggio non ancora
        assert via.wait(10)
        return reale(**kw)

    stop.owner_clear_breach = clear_lento
    esiti = {}
    t_owner = threading.Thread(target=lambda: esiti.setdefault("esito", c.rc.owner_unlock_drawdown_hard_stop(**_OWNER)))
    t_owner.start()
    assert dentro.wait(10)
    t_mark = threading.Thread(target=lambda: (c.rc._mark_uncertain("K-AMB", 1, customer_ref="r-amb"), marcato.set()))
    t_mark.start()
    assert not marcato.wait(0.3)                              # in attesa: non entra in mezzo alla transizione
    assert "K-AMB" not in c.rc._uncertain_reservations_map()
    via.set()
    t_owner.join(10)
    t_mark.join(10)
    assert marcato.is_set() and "K-AMB" in c.rc._uncertain_reservations_map()
    assert esiti["esito"]["unlocked"] is True and esiti["esito"]["checks"]["uncertain_orders"] == []
    # con l'ordine ormai incerto, una nuova procedura owner e' negata
    _lettura(c, 80.0)
    assert c.rc._hard_stop().breached
    assert c.rc.owner_unlock_drawdown_hard_stop(**dict(_OWNER, operation_id="PF-UNLOCK-0002"))["reason"] == \
        "ordini_incerti_presenti"


# ------------------------------------------------- Sol su 9209f0d8 (D-631-08, stessa classe): ciclo 12
def _fondi(available, committed=0.0):
    return {"available": available, "exposure": -committed, "ok": True}


def test_late_desk_reading_still_raises_the_peak_through_the_real_funds_contract(tmp_path):
    """Sol su 9209f0d8: il percorso reale (`_apply_funds_reading` -> RiskDesk) scartava la lettura tardiva prima di
    arrivare all'hard-stop. 100, poi 115 (t3), poi 130 iniziata a t2 e arrivata dopo: il desk NON cambia (bankroll e
    cassa restano 115), ma il picco e' 130 e la perdita di 15 con limite 10 blocca."""
    import time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    base = time.monotonic() + 100.0
    assert c.rc._apply_funds_reading(_fondi(115.0), source="test", read_at=base + 3)["applied"]
    esito = c.rc._apply_funds_reading(_fondi(130.0), source="test", read_at=base + 2)
    assert esito["applied"] is False                          # il desk non torna indietro
    assert c.rc.risk_desk.cash_equity_current == 115.0
    stop = c.rc._hard_stop()
    assert stop.snapshot()["peak"] == 130.0 and stop.breached
    assert c.rc._drawdown_last_cash == 115.0
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 0 and _bloccato_da_hard_stop(c), _rifiuti(c)


def test_late_desk_reading_with_a_lower_cash_changes_nothing(tmp_path):
    """Nessun falso blocco sul percorso reale: 130 (t3) poi 100 tardiva (t2)."""
    import time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    base = time.monotonic() + 100.0
    assert c.rc._apply_funds_reading(_fondi(130.0), source="test", read_at=base + 3)["applied"]
    assert c.rc._apply_funds_reading(_fondi(100.0), source="test", read_at=base + 2)["applied"] is False
    stop = c.rc._hard_stop()
    assert stop.snapshot()["peak"] == 130.0 and not stop.breached and c.rc.risk_desk.cash_equity_current == 130.0


def test_late_desk_reading_with_zero_available_and_committed_cash_raises_the_peak(tmp_path):
    """Stesso percorso con disponibile 0: la cassa verificata e' l'impegnato (PFC-008) e resta un massimo osservato."""
    import time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    base = time.monotonic() + 100.0
    assert c.rc._apply_funds_reading(_fondi(115.0), source="test", read_at=base + 3)["applied"]
    assert c.rc._apply_funds_reading(_fondi(0.0, committed=130.0), source="test", read_at=base + 2)["applied"] is False
    stop = c.rc._hard_stop()
    assert stop.snapshot()["peak"] == 130.0 and stop.breached


def test_late_desk_reading_with_zero_cash_is_not_a_loss(tmp_path):
    """Disponibile 0 senza impegnato, arrivata tardi: non e' un massimo e non e' la cassa corrente => nessun blocco."""
    import time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    base = time.monotonic() + 100.0
    assert c.rc._apply_funds_reading(_fondi(101.0), source="test", read_at=base + 3)["applied"]
    assert c.rc._apply_funds_reading(_fondi(0.0), source="test", read_at=base + 2)["applied"] is False
    stop = c.rc._hard_stop()
    assert stop.snapshot()["peak"] == 101.0 and not stop.breached


def test_reading_started_before_an_owner_rebase_is_ignored_even_when_it_is_the_newest(tmp_path):
    """Sol su 9209f0d8: lettura iniziata PRIMA del ribasamento owner ma DOPO l'ultima valutata (non tardiva). Appartiene
    all'epoca precedente del picco: non lo rialza (130 contro il picco ribasato 85 = falso hard-stop persistente) e
    non diventa la cassa corrente. La lettura successiva al ribasamento vale normalmente."""
    import time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    c.rc._evaluate_drawdown_hard_stop(100.0, read_at=time.monotonic())
    iniziata = time.monotonic()                               # lettura in volo, non ancora arrivata
    assert c.rc._hard_stop().owner_rebase_peak(85.0, authorization="PF-OWNER-X", reason="nuova baseline",
                                               operation_id="op-reb-2", confirm=True)
    c.rc._evaluate_drawdown_hard_stop(130.0, read_at=iniziata)
    stop = c.rc._hard_stop()
    assert stop.snapshot()["peak"] == 85.0 and not stop.breached
    assert c.rc._drawdown_last_cash == 100.0 and c.rc._drawdown_read_at < iniziata
    assert getattr(c.rc, "_pending_drawdown_lockdown", None) is None
    c.rc._evaluate_drawdown_hard_stop(84.0, read_at=time.monotonic())
    assert stop.snapshot()["peak"] == 85.0 and not stop.breached and c.rc._drawdown_last_cash == 84.0
    c.rc._evaluate_drawdown_hard_stop(74.0, read_at=time.monotonic())
    assert stop.breached                                      # 85 - 74 = 11 >= 10: il limite vale dal picco ribasato


@pytest.mark.parametrize("cassa, picco, superato, ultima", [(130.0, 130.0, False, 130.0), (80.0, 100.0, True, 80.0)])
def test_reading_rejected_by_the_desk_but_newest_for_the_hard_stop_uses_its_own_clock(tmp_path, cassa, picco, superato,
                                                                                      ultima):
    """L'hard-stop decide col PROPRIO istante, non con quello del desk. Il desk ha applicato a t5 una lettura non
    verificabile (nessuna cassa: l'hard-stop non l'ha valutata, il suo ultimo istante resta t1). Una lettura verificata
    di t3 arriva dopo: per il desk e' tardiva (scartata), per l'hard-stop e' la piu' recente. Quindi e' la sua cassa
    corrente: 130 dopo 100 NON e' una perdita (nessun falso blocco contro la cassa piu' vecchia), 80 dopo 100 lo e'."""
    import time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    base = time.monotonic() + 100.0
    assert c.rc._apply_funds_reading(_fondi(100.0), source="test", read_at=base + 1)["applied"]
    assert c.rc._apply_funds_reading({"error": "timeout"}, source="test", read_at=base + 5)["applied"]
    assert c.rc._apply_funds_reading(_fondi(cassa), source="test", read_at=base + 3)["applied"] is False
    stop = c.rc._hard_stop()
    assert stop.snapshot()["peak"] == picco and stop.breached is superato
    assert c.rc._drawdown_last_cash == ultima and c.rc._drawdown_read_at == base + 3


# ------------------------------------------------- D-631-10 e START (Codex/Sol/Claude D su 3e6a3e0a): ciclo 13
# Mandato owner PF-OWNER-PR631-CYCLE13-GO-20261111 (ratificato da PF-OWNER-PR631-CYCLES-11-12-RATIFY-13-CONFIRM-20261111).
from decimal import Decimal


def _letture_dal_conto(tmp_path, letture, **cfg):
    """Catena con hard-stop; le letture `(cassa, istante relativo)` passano dal contratto REALE del conto
    (`_apply_funds_reading` -> RiskDesk -> hard-stop) nell'ordine d'ARRIVO dato. Restituisce catena e istante base."""
    import time
    c = _catena(tmp_path, **(cfg or {"max_drawdown_abs": 10.0}))
    c.rc.risk_desk.equity_peak = 0.0
    c.rc.risk_desk.bankroll_start = 0.0
    base = time.monotonic() + 100.0
    for cassa, istante in letture:
        c.rc.betfair_service.get_account_funds = lambda cassa=cassa: _fondi(cassa)
        c.rc._apply_funds_reading(_fondi(cassa), source="test", read_at=base + istante)
    return c, base


def test_transient_dip_seen_only_by_a_late_reading_is_memorized(tmp_path):
    """D-631-10 (P1): 130 a t1, recupero a 130 a t3, poi arriva la lettura di t2 con 115. Il calo 130 -> 115 = 15
    e' avvenuto davvero (prima di t2 la cassa era 130): con limite 10 il superamento si memorizza, come se le letture
    fossero arrivate in ordine. Prima: picco 130, nessun blocco, ingressi riaperti dal recupero."""
    c, _ = _letture_dal_conto(tmp_path, [(130.0, 1), (130.0, 3), (115.0, 2)])
    stop = c.rc._hard_stop()
    assert stop.breached and stop.snapshot()["breach"]["kind"] == "max_drawdown_abs"
    assert Decimal(stop.snapshot()["breach"]["loss"]) == 15 and Decimal(stop.snapshot()["breach"]["peak"]) == 130
    assert c.rc.risk_desk.cash_equity_current == 130.0 and c.rc._drawdown_last_cash == 130.0   # cassa corrente intatta
    assert c.rc.mode.name == "LOCKDOWN"
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 0 and _bloccato_da_hard_stop(c), _rifiuti(c)
    riletto = DrawdownHardStop(c.rc.db)                      # durevole: crash e riavvio lo ritrovano
    riletto.restore(reconcile=False)
    assert riletto.breached


def test_transient_dip_in_order_gives_the_same_result(tmp_path):
    """Riferimento: le stesse letture in ordine memorizzano lo stesso superamento (l'esito non dipende dall'arrivo)."""
    c, _ = _letture_dal_conto(tmp_path, [(130.0, 1), (115.0, 2), (130.0, 3)])
    assert c.rc._hard_stop().breached and Decimal(c.rc._hard_stop().snapshot()["breach"]["loss"]) == 15


def test_late_higher_reading_turns_an_earlier_recorded_dip_into_a_breach(tmp_path):
    """Stessa classe, l'altro verso: 100 @t1, 115 @t3, 125 @t4, poi 130 @t2 tardiva. Il massimo 130 a t2 rende
    reale il calo a 115 di t3 (15 >= 10) anche se l'ultima cassa (125) e' a soli 5 dal picco."""
    c, _ = _letture_dal_conto(tmp_path, [(100.0, 1), (115.0, 3), (125.0, 4), (130.0, 2)])
    stop = c.rc._hard_stop()
    assert stop.breached and Decimal(stop.snapshot()["breach"]["loss"]) == 15
    assert Decimal(stop.snapshot()["breach"]["cash"]) == 115 and c.rc._drawdown_last_cash == 125.0


@pytest.mark.parametrize("letture", [
    [(100.0, 1), (130.0, 3), (100.0, 2)],                   # Sol su 4a171465: 100 prima di 130 non e' una perdita
    [(100.0, 1), (130.0, 4), (95.0, 2), (128.0, 3)],        # il calo a 95 e' PRIMA del 130: dal massimo 100 sono 5
    [(120.0, 1), (130.0, 3), (111.0, 2)],                   # 120 -> 111 = 9 < 10
    [(130.0, 2), (115.0, 1)],                               # nessuna lettura precedente a t1: niente da confrontare
])
def test_late_readings_never_create_a_false_drawdown(tmp_path, letture):
    """Nessun falso superamento: una lettura tardiva si confronta SOLO con le casse lette prima del suo istante,
    mai con un massimo successivo."""
    c, _ = _letture_dal_conto(tmp_path, letture)
    stop = c.rc._hard_stop()
    assert not stop.breached, stop.snapshot()
    assert getattr(c.rc, "_pending_drawdown_lockdown", None) is None and c.rc.mode.name != "LOCKDOWN"


def test_transient_dip_with_the_percentage_cap(tmp_path):
    """Stesso caso con il limite percentuale: 200 @t1, 200 @t3, 170 @t2 tardiva => 15 % >= 10 %."""
    c, _ = _letture_dal_conto(tmp_path, [(200.0, 1), (200.0, 3), (170.0, 2)], max_drawdown_hard_stop_pct=10.0)
    stop = c.rc._hard_stop()
    assert stop.breached and stop.snapshot()["breach"]["kind"] == "max_drawdown_hard_stop_pct"


def test_transient_dip_through_the_entry_reading(tmp_path):
    """Percorso d'ingresso reale (`_read_funds_for_entry`): la lettura del segnale iniziata a t2 arriva dopo la
    lettura di stato di t3. Il calo 130 -> 115 si memorizza e il segnale non arriva al broker."""
    import time
    c, base = _letture_dal_conto(tmp_path, [(130.0, 1), (130.0, 3)])
    c.rc.betfair_service.get_account_funds = lambda: _fondi(115.0)
    orologio = iter([base + 2])
    reale = time.monotonic
    import core.runtime_controller as modulo
    try:
        modulo.time.monotonic = lambda: next(orologio, None) or reale()
        c.rc._read_funds_for_entry()
    finally:
        modulo.time.monotonic = reale
    assert c.rc._hard_stop().breached and Decimal(c.rc._hard_stop().snapshot()["breach"]["loss"]) == 15


def test_late_dip_from_before_an_owner_rebase_is_ignored(tmp_path):
    """Nessun falso blocco: una lettura iniziata prima del ribasamento owner non si confronta con il picco ribasato
    ne' con i massimi dell'epoca precedente (resta ignorata, D-631-08)."""
    import time
    c, base = _letture_dal_conto(tmp_path, [(130.0, 1)])
    in_volo = time.monotonic()
    assert c.rc._hard_stop().owner_rebase_peak(100.0, authorization="PF-OWNER-X", reason="nuova baseline",
                                               operation_id="op-reb-13", confirm=True)
    c.rc._evaluate_drawdown_hard_stop(100.0, read_at=time.monotonic())
    c.rc._evaluate_drawdown_hard_stop(85.0, read_at=in_volo)   # tardiva, pre-ribasamento: ignorata
    assert not c.rc._hard_stop().breached and c.rc._hard_stop().snapshot()["peak"] == 100.0


def test_late_dip_of_a_non_owner_controller_writes_nothing(tmp_path):
    """Autorita' runtime: un controller che ha perso l'autorita' non memorizza nulla, nemmeno con una lettura tardiva."""
    c, base = _letture_dal_conto(tmp_path, [(130.0, 1), (130.0, 3)])
    prima = c.rc.db.get_settings()[STATE_KEY]
    c.rc._authority_lost_reason = "lease_superseded"
    c.rc._apply_funds_reading(_fondi(115.0), source="test", read_at=base + 2)
    assert c.rc.db.get_settings()[STATE_KEY] == prima and not c.rc._hard_stop().breached


@pytest.mark.real_blackbox
def test_start_first_reading_is_evaluated_after_a_rebase_by_another_process(tmp_path, monkeypatch):
    """P3 START (Sol su 3e6a3e0a, Claude D [628-M27]): controller costruito, poi un altro processo ribasa il picco a
    90 in modo durevole, poi START reale con il conto a 75. La PRIMA lettura dello START (quella dei fondi d'avvio)
    deve essere valutata: lo stato si ricarica PRIMA di leggere i fondi, quindi il ribasamento non la rende «vecchia».
    Prima la prima lettura era scartata (il superamento arrivava solo da una valutazione successiva)."""
    from core import blackbox_runtime
    from core.runtime_controller import RuntimeController
    from tests.acceptance.test_issue461_pr29c_settlement_processes_and_schema import _app_su
    monkeypatch.chdir(tmp_path)
    blackbox_runtime.install(str(tmp_path / "bb" / "blackbox.sqlite"), entrypoint="test_d1_start")
    app = _app_su(str(tmp_path / "start.db"), monkeypatch)
    rc = app.runtime
    carica = rc.settings_service.load_roserpina_config

    def con_limite():                                         # limite nelle impostazioni (START ricarica la config)
        config = carica()
        config.max_drawdown_abs = 10.0
        return config

    monkeypatch.setattr(rc.settings_service, "load_roserpina_config", con_limite)
    altro = DrawdownHardStop(rc.db)
    altro.set_scope(_AMBITO_NON_MARCATO)                       # S18-1: stesso ambito delle letture dell'harness                           # altro processo, dopo la costruzione di rc
    altro.restore()
    altro.observe(100.0)
    assert altro.owner_rebase_peak(90.0, authorization="PF-OWNER-X", reason="nuova baseline",
                                   operation_id="op-start-13", confirm=True)
    rc.betfair_service.get_account_funds = lambda: _fondi(75.0)
    valutate = []
    reale = RuntimeController._evaluate_drawdown_hard_stop

    def spia(self, cassa, read_at=None, **kwargs):
        stop = self._hard_stop()
        prima = (stop.breached, stop.snapshot()["peak"])
        reale(self, cassa, read_at, **kwargs)
        valutate.append((cassa, prima, (stop.breached, stop.snapshot()["peak"])))

    monkeypatch.setattr(RuntimeController, "_evaluate_drawdown_hard_stop", spia)
    try:
        rc.start(execution_mode="SIMULATION")
        assert valutate, "nessuna valutazione allo START"
        cassa, prima, dopo = valutate[0]
        assert cassa == 75.0 and prima == (False, 90.0) and dopo == (True, 90.0), valutate
        assert rc._hard_stop().peak_epoch == 1
    finally:
        try:
            app.stop()
        except Exception:
            pass
        blackbox_runtime.shutdown()


def test_late_dip_after_an_owner_rebase_is_measured_from_the_rebased_peak(tmp_path):
    """Il ribasamento owner chiude la cronologia dell'epoca precedente: una lettura tardiva iniziata DOPO il
    ribasamento si confronta con il picco ribasato (100), mai con i massimi di prima (130 => falso calo di 35)."""
    import time
    c, _ = _letture_dal_conto(tmp_path, [(130.0, 1)])
    assert c.rc._hard_stop().owner_rebase_peak(100.0, authorization="PF-OWNER-X", reason="nuova baseline",
                                               operation_id="op-reb-13b", confirm=True)
    dopo = time.monotonic()
    c.rc._evaluate_drawdown_hard_stop(100.0, read_at=dopo + 2)
    c.rc._evaluate_drawdown_hard_stop(95.0, read_at=dopo + 1)     # tardiva ma successiva al ribasamento
    assert not c.rc._hard_stop().breached
    c.rc._evaluate_drawdown_hard_stop(88.0, read_at=dopo + 1.5)   # 100 -> 88 = 12: calo vero dell'epoca nuova
    assert c.rc._hard_stop().breached


def test_reading_history_is_bounded(tmp_path, monkeypatch):
    """Memoria limitata: oltre `RECENT_READINGS_LIMIT` le letture piu' vecchie passano nel riepilogo (scala dei massimi
    e dei minimi); il massimo uscito resta valutabile per le letture tardive successive al suo istante."""
    import core.drawdown_hard_stop as modulo
    monkeypatch.setattr(modulo, "RECENT_READINGS_LIMIT", 2)
    c, base = _letture_dal_conto(tmp_path, [(130.0, 1), (128.0, 2), (129.0, 3)])
    stop = c.rc._hard_stop()
    assert len(stop._history) == 2 and stop._ladder and stop._ladder[0][1] == 130
    c.rc._apply_funds_reading(_fondi(118.0), source="test", read_at=base + 2.5)   # 130 (uscito, t1) -> 118 = 12
    assert stop.breached and not stop.history_insufficient.get("active")


def test_start_reloads_the_drawdown_state_before_the_funds_reading_begins():
    """P3: la rilettura dello stato precede l'istante d'inizio della lettura dei fondi dello START."""
    import inspect
    from core.runtime_controller import RuntimeController
    src = inspect.getsource(RuntimeController._start_with_instance_lock)
    assert src.index("self._reload_drawdown_state()") < src.index("start_funds = time.monotonic()")
    assert src.count("self._reload_drawdown_state()") == 1


def test_restart_in_the_same_process_measures_late_dips_from_the_reloaded_peak(tmp_path):
    """La finestra delle letture si riapre a ogni START, con il picco appena riletto come base. Sessione 1: 100, 120.
    Fra le due sessioni un altro processo salva il picco 150. Sessione 2 nello stesso processo: 141 a t4 (perdita 9,
    sotto il limite), poi arriva 139 da t3: rispetto a cio' che era noto prima di t3 (150) e' un calo di 11 => blocco.
    Senza riaprire la finestra la lettura tardiva si confronterebbe con la cronologia vecchia (120) e passerebbe."""
    import time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    c.rc._reload_drawdown_state()                                # START 1
    base = time.monotonic() + 100.0
    c.rc._evaluate_drawdown_hard_stop(100.0, read_at=base + 1)
    c.rc._evaluate_drawdown_hard_stop(120.0, read_at=base + 2)
    altro = DrawdownHardStop(c.rc.db)
    altro.restore()
    altro.observe(150.0)                                         # picco durevole di un altro processo
    c.rc._reload_drawdown_state()                                # START 2, stesso processo
    assert c.rc._hard_stop().snapshot()["peak"] == 150.0
    secondo = time.monotonic() + 200.0
    c.rc._evaluate_drawdown_hard_stop(141.0, read_at=secondo + 4)
    assert not c.rc._hard_stop().breached
    c.rc._evaluate_drawdown_hard_stop(139.0, read_at=secondo + 3)
    assert c.rc._hard_stop().breached and Decimal(c.rc._hard_stop().snapshot()["breach"]["loss"]) == 11


# ------------------------------------------------- D-631-11 (Claude D su b76466a3): ciclo 14
# Mandato owner PF-OWNER-PR631-CYCLE14-GO-20261111. Una lettura tardiva che la cronologia in memoria non permette di
# valutare non e' mai un PASS: o si valuta con il riepilogo esatto (scala dei massimi/minimi), o diventa
# DRAWDOWN_HISTORY_INSUFFICIENT (stato distinto, persistito, ingressi negati, sblocco owner).
from core.drawdown_hard_stop import REASON_HISTORY_INSUFFICIENT, STATE_HISTORY_INSUFFICIENT  # noqa: E402


def _molte_letture(c, base, da, quante, cassa=160.0):
    for i in range(quante):
        c.rc._apply_funds_reading(_fondi(cassa), source="test", read_at=base + da + i * 0.01)


def test_two_maxima_out_of_memory_and_a_late_reading_between_them(tmp_path):
    """Claude D (D-631-11): 120 a t2, 160 a t10, poi 700 letture, poi 105 arrivata da t5. Entrambi i massimi sono
    usciti dalle letture recenti: la scala dei massimi da' il picco valido a t5 (120) => perdita 15, superamento. Prima
    la lettura era scartata in silenzio (nessun superamento, gate '')."""
    c, base = _letture_dal_conto(tmp_path, [(120.0, 2), (160.0, 10)])
    _molte_letture(c, base, 11, 700)
    stop = c.rc._hard_stop()
    assert not stop.breached
    c.rc._apply_funds_reading(_fondi(105.0), source="test", read_at=base + 5)
    assert stop.breached and Decimal(stop.snapshot()["breach"]["peak"]) == 120
    assert Decimal(stop.snapshot()["breach"]["loss"]) == 15 and not stop.history_insufficient.get("active")


@pytest.mark.parametrize("cassa", [115.0, 111.0])
def test_late_reading_between_two_evicted_maxima_is_never_compared_with_the_later_one(tmp_path, cassa):
    """Nessun confronto con un picco successivo: a t5 il massimo valido e' 120 (non 160) => 115 o 111 non superano 10."""
    c, base = _letture_dal_conto(tmp_path, [(120.0, 2), (160.0, 10)])
    _molte_letture(c, base, 11, 700)
    c.rc._apply_funds_reading(_fondi(cassa), source="test", read_at=base + 5)
    stop = c.rc._hard_stop()
    assert not stop.breached and not stop.history_insufficient.get("active")


def test_breach_stays_after_700_readings_and_after_a_crash(tmp_path):
    """Un superamento accertato resta dopo che le sue letture escono dalla memoria e dopo un crash."""
    c, base = _letture_dal_conto(tmp_path, [(130.0, 1), (115.0, 2)])
    assert c.rc._hard_stop().breached
    _molte_letture(c, base, 3, 700, cassa=200.0)
    assert c.rc._hard_stop().breached
    riletto = DrawdownHardStop(c.rc.db)
    riletto.restore(reconcile=False)
    assert riletto.breached


def test_memory_stays_bounded_after_5000_readings(tmp_path):
    """Limite complessivo: letture recenti + scala dei massimi + scala dei minimi <= READING_HISTORY_LIMIT, anche con
    5.000 letture di valori diversi (rialzi e cali)."""
    import random
    import core.drawdown_hard_stop as modulo
    c, base = _letture_dal_conto(tmp_path, [(1000.0, 0)])
    rnd = random.Random(631)
    for i in range(5000):
        cassa = 1000.0 + i * 0.5 + rnd.uniform(-3.0, 3.0)
        c.rc._apply_funds_reading(_fondi(round(cassa, 2)), source="test", read_at=base + 1 + i * 0.001)
    stop = c.rc._hard_stop()
    assert len(stop._history) <= modulo.RECENT_READINGS_LIMIT
    assert len(stop._history) + len(stop._ladder) + len(stop._valleys) <= modulo.READING_HISTORY_LIMIT


def _storia_insufficiente(tmp_path, monkeypatch, **cfg):
    """Riepilogo minimo (2 punti) => la finestra avanza oltre t5 (base 108); poi arriva una lettura da t1.5 con 95. Il
    vero massimo a t1.5 era 100 (perdita 5), ma non e' piu' in memoria e contro il limite noto (108) un superamento
    sarebbe possibile (13 >= 10): non valutabile. Nessuna sequenza in ordine supera il limite."""
    import core.drawdown_hard_stop as modulo
    monkeypatch.setattr(modulo, "RECENT_READINGS_LIMIT", 2)
    monkeypatch.setattr(modulo, "SUMMARY_LIMIT", 2)
    c, base = _letture_dal_conto(tmp_path, [(100.0, 1), (104.0, 2), (98.0, 3), (108.0, 4), (102.0, 5), (112.0, 6),
                                            (106.0, 7), (116.0, 8)], **cfg)
    stop = c.rc._hard_stop()
    assert not stop.breached and stop._window_open > base + 1.5
    c.rc._apply_funds_reading(_fondi(95.0), source="test", read_at=base + 1.5)
    return c, base


def test_insufficient_history_is_a_distinct_persistent_block_not_a_breach(tmp_path, monkeypatch):
    """D-631-11: la lettura di t1.5 cade prima della cronologia conservata e un superamento e' possibile. Esito: stato
    DRAWDOWN_HISTORY_INSUFFICIENT persistito, motivo distinto, nessun superamento dichiarato, nessun LOCKDOWN."""
    c, _ = _storia_insufficiente(tmp_path, monkeypatch, max_drawdown_abs=10.0)
    stop = c.rc._hard_stop()
    gap = stop.snapshot()["history_insufficient"]
    assert gap["active"] is True and gap["state"] == STATE_HISTORY_INSUFFICIENT
    assert gap["reason"] == "lettura_precedente_alla_storia_conservata" and gap["peak_epoch"] == stop.peak_epoch
    assert gap["at"] and Decimal(gap["cash"]) == 95
    assert not stop.breached and c.rc.mode.name != "LOCKDOWN"
    assert json.loads(c.rc.db.get_settings()[STATE_KEY])["history_insufficient"]["active"] is True


def test_insufficient_history_blocks_every_entry_path(tmp_path, monkeypatch):
    """Il blocco vale per segnali Roserpina, puntate manuali / Dutching (`absolute_cap_reason_for_orders`) e auto-next
    (`_risk_allows_auto_trade`), e resta dopo cinque nuove letture del conto (una lettura nuova non ricostruisce la
    storia mancante)."""
    c, base = _storia_insufficiente(tmp_path, monkeypatch, max_drawdown_abs=10.0)
    for i in range(5):
        c.rc._apply_funds_reading(_fondi(116.0), source="test", read_at=base + 20 + i)
    assert c.rc._hard_stop().history_block_reason() == REASON_HISTORY_INSUFFICIENT
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 0 and _rifiuti(c)[-1] == REASON_HISTORY_INSUFFICIENT
    assert c.rc.absolute_cap_reason_for_orders([dict(_ORDINE_MANUALE)]) == REASON_HISTORY_INSUFFICIENT
    consentito, motivo = c.rc._risk_allows_auto_trade()
    assert consentito is False and REASON_HISTORY_INSUFFICIENT in motivo


def test_insufficient_history_survives_stop_start_crash_and_takeover(tmp_path, monkeypatch):
    """Nessun riavvio, nuovo START, crash o nuovo controller cancella lo stato: si rilegge dal disco."""
    c, _ = _storia_insufficiente(tmp_path, monkeypatch, max_drawdown_abs=10.0)
    c.rc._reload_drawdown_state()                                 # nuovo START nello stesso processo
    assert c.rc._hard_stop().history_block_reason() == REASON_HISTORY_INSUFFICIENT
    for _ in range(2):
        riletto = DrawdownHardStop(c.rc.db)                       # crash + riavvio / subentro di un altro controller
        riletto.restore(reconcile=False)
        assert riletto.history_block_reason() == REASON_HISTORY_INSUFFICIENT


def test_insufficient_history_is_cleared_only_by_the_owner_procedure_with_checks(tmp_path, monkeypatch):
    """Sblocco solo owner: campi obbligatori, verifiche (ordini AMBIGUOUS => negato), operation_id monouso, salvato
    prima che valga; non tocca il picco ne' un superamento."""
    c, _ = _storia_insufficiente(tmp_path, monkeypatch, max_drawdown_abs=10.0)
    owner = dict(authorization="#631 decisione owner", reason="riconciliazione verificata", operation_id="PF-HIST-1",
                 confirm=True)
    assert c.rc.owner_clear_drawdown_history_insufficient(**dict(owner, confirm=False))["cleared"] is False
    c.rc._mark_uncertain("K-AMB", 1, customer_ref="r-amb")
    assert c.rc.owner_clear_drawdown_history_insufficient(**owner)["reason"] == "ordini_incerti_presenti"
    c.rc._clear_uncertain("K-AMB", 1, customer_ref="r-amb")
    picco = c.rc._hard_stop().snapshot()["peak"]
    esito = c.rc.owner_clear_drawdown_history_insufficient(**owner)
    assert esito["cleared"] is True and esito["checks"]["uncertain_orders"] == []
    assert c.rc._hard_stop().history_block_reason() == "" and c.rc._hard_stop().snapshot()["peak"] == picco
    riletto = DrawdownHardStop(c.rc.db)
    riletto.restore(reconcile=False)
    assert riletto.history_block_reason() == ""
    assert c.rc.owner_clear_drawdown_history_insufficient(**owner)["reason"] == "nessuna_storia_insufficiente"


def test_owner_clear_of_insufficient_history_is_refused_to_a_non_owner(tmp_path, monkeypatch):
    c, _ = _storia_insufficiente(tmp_path, monkeypatch, max_drawdown_abs=10.0)
    c.rc._authority_lost_reason = "lease_superseded"
    esito = c.rc.owner_clear_drawdown_history_insufficient(authorization="X", reason="r", operation_id="op",
                                                           confirm=True)
    assert esito["cleared"] is False and esito["reason"].startswith("runtime_authority_lost")


def test_late_reading_before_the_retained_history_is_insufficient_when_lost_dips_could_matter(tmp_path, monkeypatch):
    """D-631-12 (oracolo corretto): prima questo test si aspettava «nessun blocco» per una lettura di t1.5 caduta prima
    della cronologia conservata (limite noto 108 - 100 = 8 < 10). Ma fra t1.5 e la finestra c'erano letture non piu' in
    memoria (98, 108, 102): se il vero massimo a t1.5 era sotto 100, i cali successivi da 100 non si possono escludere.
    Esito corretto: storia insufficiente, non un PASS."""
    import core.drawdown_hard_stop as modulo
    monkeypatch.setattr(modulo, "RECENT_READINGS_LIMIT", 2)
    monkeypatch.setattr(modulo, "SUMMARY_LIMIT", 2)
    c, base = _letture_dal_conto(tmp_path, [(100.0, 1), (104.0, 2), (98.0, 3), (108.0, 4), (102.0, 5), (112.0, 6),
                                            (106.0, 7), (116.0, 8)])
    c.rc._apply_funds_reading(_fondi(100.0), source="test", read_at=base + 1.5)
    stop = c.rc._hard_stop()
    assert not stop.breached
    assert stop.history_insufficient.get("reason") == "massimo_precedente_alla_storia_conservata"


def test_late_reading_before_the_retained_history_with_no_possible_breach_is_not_blocked(tmp_path, monkeypatch):
    """Terzo esito: nessun superamento e' possibile nemmeno nel caso peggiore (limite 200 su casse intorno a 100):
    nessun blocco, nessun superamento."""
    import core.drawdown_hard_stop as modulo
    monkeypatch.setattr(modulo, "RECENT_READINGS_LIMIT", 2)
    monkeypatch.setattr(modulo, "SUMMARY_LIMIT", 2)
    c, base = _letture_dal_conto(tmp_path, [(100.0, 1), (104.0, 2), (98.0, 3), (108.0, 4), (102.0, 5), (112.0, 6),
                                            (106.0, 7), (116.0, 8)], max_drawdown_abs=200.0)
    c.rc._apply_funds_reading(_fondi(100.0), source="test", read_at=base + 1.5)
    stop = c.rc._hard_stop()
    assert not stop.breached and not stop.history_insufficient.get("active")


def test_late_high_reading_from_before_the_start_is_insufficient(tmp_path):
    """(B) Picco persistito 100, START (finestra aperta adesso), poi 105 e 106. Arriva una lettura iniziata PRIMA dello
    START con 112: e' un massimo nuovo, e fra il suo istante e lo START ci possono essere cali che questo processo non
    ha mai visto. Le letture conservate non dimostrano un superamento (112 - 105 = 7): storia insufficiente, non un
    superamento inventato e non un PASS."""
    import time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    c.rc._hard_stop().observe(100.0)
    prima = time.monotonic()
    c.rc._reload_drawdown_state()
    adesso = time.monotonic()
    c.rc._evaluate_drawdown_hard_stop(105.0, read_at=adesso + 1)
    c.rc._evaluate_drawdown_hard_stop(106.0, read_at=adesso + 2)
    c.rc._evaluate_drawdown_hard_stop(112.0, read_at=prima)
    stop = c.rc._hard_stop()
    assert not stop.breached
    assert stop.history_insufficient.get("reason") == "massimo_precedente_alla_storia_conservata"
    assert stop.history_block_reason() == REASON_HISTORY_INSUFFICIENT


def test_late_high_reading_that_proves_a_breach_is_memorized_as_a_breach(tmp_path):
    """Stesso caso, ma la lettura conservata successiva dimostra il calo (125 prima dello START, poi 105): superamento
    vero (20), non storia insufficiente."""
    import time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    c.rc._hard_stop().observe(100.0)
    prima = time.monotonic()
    c.rc._reload_drawdown_state()
    adesso = time.monotonic()
    c.rc._evaluate_drawdown_hard_stop(105.0, read_at=adesso + 1)
    c.rc._evaluate_drawdown_hard_stop(106.0, read_at=adesso + 2)
    c.rc._evaluate_drawdown_hard_stop(125.0, read_at=prima)
    stop = c.rc._hard_stop()
    assert stop.breached and Decimal(stop.snapshot()["breach"]["loss"]) == 20
    assert not stop.history_insufficient.get("active")


def test_owner_rebase_resets_a_populated_summary(tmp_path):
    """Ribasamento owner con scala gia' popolata: i massimi della vecchia epoca (160) non valgono per le letture della
    nuova; una lettura iniziata prima del ribasamento resta ignorata."""
    import time
    c, base = _letture_dal_conto(tmp_path, [(120.0, 2), (160.0, 10)])
    _molte_letture(c, base, 11, 300)
    stop = c.rc._hard_stop()
    assert stop._ladder
    in_volo = time.monotonic()
    assert stop.owner_rebase_peak(100.0, authorization="PF-OWNER-X", reason="nuova baseline",
                                  operation_id="op-reb-14", confirm=True)
    assert stop._ladder == [] and stop._valleys == [] and stop._history == []
    dopo = time.monotonic()
    c.rc._evaluate_drawdown_hard_stop(100.0, read_at=dopo + 2)
    c.rc._evaluate_drawdown_hard_stop(95.0, read_at=dopo + 1)      # tardiva della nuova epoca: 100 -> 95
    c.rc._evaluate_drawdown_hard_stop(50.0, read_at=in_volo)       # iniziata prima del ribasamento: ignorata
    assert not stop.breached and not stop.history_insufficient.get("active")


def test_same_instant_readings_are_never_compared_with_each_other(tmp_path):
    """Letture con lo stesso istante sono concorrenti: 125 e 110 entrambe a t2 non danno una perdita di 15."""
    c, base = _letture_dal_conto(tmp_path, [(100.0, 1), (130.0, 3), (125.0, 2), (110.0, 2)])
    assert not c.rc._hard_stop().breached
    c2, _ = _letture_dal_conto(tmp_path / "b", [(100.0, 1), (130.0, 3), (110.0, 2), (125.0, 2)])
    assert not c2.rc._hard_stop().breached


@pytest.mark.parametrize("ordine", list(__import__("itertools").permutations(range(4))))
def test_all_24_arrival_orders_give_the_same_drawdown(tmp_path, ordine):
    """100@1, 130@2, 115@3, 125@4 in tutti i 24 ordini d'arrivo, dal percorso reale: sempre picco 130 e superamento
    di 15 (130 -> 115), mai un altro valore."""
    letture = [(100.0, 1), (130.0, 2), (115.0, 3), (125.0, 4)]
    c, _ = _letture_dal_conto(tmp_path, [letture[i] for i in ordine])
    stop = c.rc._hard_stop()
    assert stop.snapshot()["peak"] == 130.0 and stop.breached, (ordine, stop.snapshot())
    assert Decimal(stop.snapshot()["breach"]["loss"]) == 15 and Decimal(stop.snapshot()["breach"]["peak"]) == 130


@pytest.mark.parametrize("limiti", [(256, 256), (1, 1), (2, 3), (3, 1)])
def test_random_arrival_orders_match_the_in_order_result_or_declare_insufficient_history(tmp_path, monkeypatch,
                                                                                       limiti):
    """Proprieta' (oracolo: le stesse letture in ordine). Mai un superamento falso; mai un superamento perso senza
    storia insufficiente. Con memoria ampia l'esito coincide sempre con l'oracolo (nessuna incertezza)."""
    import random
    import core.drawdown_hard_stop as modulo
    monkeypatch.setattr(modulo, "RECENT_READINGS_LIMIT", limiti[0])
    monkeypatch.setattr(modulo, "SUMMARY_LIMIT", limiti[1])
    rnd = random.Random(sum(limiti))
    for _ in range(300):
        stop = DrawdownHardStop(_Db())
        tempi = rnd.sample(range(1, 40), rnd.randint(2, 9))
        letture = [(float(t), rnd.choice([80, 90, 95, 100, 105, 110, 115, 120, 130, 140])) for t in tempi]
        arrivo = letture[:]
        rnd.shuffle(arrivo)
        ultima = None
        ultima_cassa = None
        for t, cassa in arrivo:
            if ultima is None or t >= ultima:
                stop.record_reading(t, cassa, may_open=True)
                stop.evaluate(10.0, cassa)
                ultima, ultima_cassa = t, cassa
            else:
                stop.evaluate_late(10.0, t, cassa)
                prima = stop.peak
                stop.observe(cassa)
                if stop.peak != prima:
                    stop.evaluate(10.0, ultima_cassa)
        migliore, oracolo = None, False
        for _, cassa in sorted(letture):
            if migliore is not None and migliore - cassa >= 10:
                oracolo = True
            migliore = cassa if migliore is None or cassa > migliore else migliore
        incerta = bool(stop.history_insufficient.get("active"))
        assert not (stop.breached and not oracolo), (arrivo, limiti)
        assert not (oracolo and not stop.breached and not incerta), (arrivo, limiti)
        if limiti == (256, 256):
            assert stop.breached == oracolo and not incerta, arrivo


@pytest.mark.parametrize("tardiva", [(130.0, 3), (131.0, 1.5), (100.0, 3)])
def test_owner_clearance_is_not_undone_by_late_readings_from_before_it(tmp_path, tardiva):
    """Sol e Codex su b76466a3: 130 @t1, 115 @t2 (superamento), 130 @t4; sblocco owner; poi arriva una lettura iniziata
    PRIMA dello sblocco. Lo sblocco owner copre tutto cio' che precede il suo istante (come il ribasamento): quella
    lettura non fa rinascere il vecchio superamento e non apre una storia insufficiente."""
    import time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    base = time.monotonic() - 100.0                           # istanti nel passato: tutte iniziate prima dello sblocco
    for cassa, istante in [(130.0, 1), (115.0, 2), (130.0, 4)]:
        c.rc._apply_funds_reading(_fondi(cassa), source="test", read_at=base + istante)
    assert c.rc._hard_stop().breached
    assert c.rc.owner_unlock_drawdown_hard_stop(**_OWNER)["unlocked"] is True
    cassa, istante = tardiva
    c.rc._apply_funds_reading(_fondi(cassa), source="test", read_at=base + istante)
    stop = c.rc._hard_stop()
    assert not stop.breached and not stop.history_insufficient.get("active"), stop.snapshot()
    assert c.rc.mode.name != "LOCKDOWN" or getattr(c.rc, "_pending_drawdown_lockdown", None) is None


def test_after_an_owner_clearance_new_losses_are_still_measured(tmp_path):
    """Lo sblocco non ribasa il picco: dopo, una perdita nuova oltre il limite blocca di nuovo."""
    import time
    c, _ = _letture_dal_conto(tmp_path, [(130.0, 1), (115.0, 2), (130.0, 4)])
    assert c.rc.owner_unlock_drawdown_hard_stop(**_OWNER)["unlocked"] is True
    adesso = time.monotonic()
    c.rc._apply_funds_reading(_fondi(129.0), source="test", read_at=adesso + 1)
    c.rc._apply_funds_reading(_fondi(118.0), source="test", read_at=adesso + 3)
    c.rc._apply_funds_reading(_fondi(120.0), source="test", read_at=adesso + 2)
    stop = c.rc._hard_stop()
    assert stop.breached and stop.snapshot()["peak"] == 130.0


# ------------------------------------------------- D-631-12 (Sol/Claude D su f0f521dc): ciclo 15
# Mandato owner PF-OWNER-PR631-CYCLE15-GO-20261112. Tre esiti ammessi: superamento accertato -> hard-stop;
# superamento possibile ma non accertabile -> DRAWDOWN_HISTORY_INSUFFICIENT; altrimenti nulla. Mai una perdita silenziosa.
@pytest.mark.parametrize("limiti", [(1, 1), (2, 1), (1, 2)])
def test_late_reading_before_the_retained_history_never_hides_a_later_dip(tmp_path, monkeypatch, limiti):
    """D-631-12: limite 15; arrivo 90@1, 80@3, 100@4, 100@5, 100@6, poi 95 da t2. In ordine (90, 95, 80) e' un
    superamento di 15. Con la memoria ridotta 90 e 80 non ci sono piu': il vero massimo a t2 e' sconosciuto (puo' essere
    sotto 95), quindi 95 puo' essere un massimo nuovo che rende reale il calo successivo. Prima la verifica (B) partiva
    solo se 95 superava il limite superiore (100): nessun superamento e nessuna storia insufficiente."""
    import core.drawdown_hard_stop as modulo
    monkeypatch.setattr(modulo, "RECENT_READINGS_LIMIT", limiti[0])
    monkeypatch.setattr(modulo, "SUMMARY_LIMIT", limiti[1])
    c, _ = _letture_dal_conto(tmp_path, [(90.0, 1), (80.0, 3), (100.0, 4), (100.0, 5), (100.0, 6), (95.0, 2)],
                              max_drawdown_abs=15.0)
    stop = c.rc._hard_stop()
    assert stop.breached or stop.history_insufficient.get("active"), (limiti, stop.snapshot())
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 0


def _esito_a_proprieta(rnd, limiti, n_min, n_max, cap=15.0):
    """Una sequenza casuale fuori ordine: (superamento, storia insufficiente, oracolo in ordine, arrivo)."""
    stop = DrawdownHardStop(_Db())
    tempi = rnd.sample(range(1, 400), rnd.randint(n_min, n_max))
    letture = [(float(t), float(rnd.randint(70, 130))) for t in tempi]
    arrivo = letture[:]
    # spesso solo poche letture arrivano in ritardo, e molto in ritardo: il caso che svuota la memoria
    if rnd.random() < 0.5:
        arrivo.sort()
        for _ in range(rnd.randint(1, 3)):
            i = rnd.randrange(len(arrivo))
            arrivo.append(arrivo.pop(i))
    else:
        rnd.shuffle(arrivo)
    ultima = None
    for t, cassa in arrivo:
        if ultima is None or t >= ultima:
            stop.record_reading(t, cassa, may_open=True)
            stop.evaluate(cap, cassa)
            ultima = t
        else:
            stop.evaluate_late(cap, t, cassa)
            stop.observe(cassa)
    migliore, oracolo = None, False
    for _, cassa in sorted(letture):
        if migliore is not None and migliore - cassa >= cap:
            oracolo = True
        migliore = cassa if migliore is None or cassa > migliore else migliore
    return stop.breached, bool(stop.history_insufficient.get("active")), oracolo, arrivo


@pytest.mark.parametrize("limiti", [(r, s) for r in (1, 2, 3) for s in (1, 2, 3)])
def test_long_out_of_order_sequences_with_tiny_memory_end_in_one_of_three_correct_states(monkeypatch, limiti):
    """Criterio owner (ciclo 15): ogni sequenza finisce in superamento accertato, storia insufficiente o nessun
    superamento dimostrabile. Memoria minima, sequenze lunghe, letture molto in ritardo. Mai un superamento falso
    (oracolo in ordine), mai una perdita silenziosa."""
    import random
    import core.drawdown_hard_stop as modulo
    monkeypatch.setattr(modulo, "RECENT_READINGS_LIMIT", limiti[0])
    monkeypatch.setattr(modulo, "SUMMARY_LIMIT", limiti[1])
    rnd = random.Random(1000 + 10 * limiti[0] + limiti[1])
    for _ in range(2500):
        superato, incerta, oracolo, arrivo = _esito_a_proprieta(rnd, limiti, 3, 24)
        assert not (superato and not oracolo), ("falso superamento", limiti, arrivo)
        assert not (oracolo and not superato and not incerta), ("perdita silenziosa", limiti, arrivo)


# ------------------------------------------------- D-631-13/14/15 (Codex/Sol/Claude D su 3689ae8f): ciclo 16
# Mandato owner PF-OWNER-PR631-CYCLE16-GO-20261112.
def _ingresso_in_coda(c):
    """Segnale approvato con il `CMD_QUICK_BET` trattenuto (in coda verso il motore). Restituisce i comandi."""
    trattenuti = []
    reale = c.rc.bus.publish

    def publish(topic, payload=None):
        if topic == "CMD_QUICK_BET":
            trattenuti.append(payload)
            c.rc.bus.events.append((topic, payload))
            return None
        return reale(topic, payload)

    c.rc.bus.publish = publish
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    c.rc.bus.publish = reale
    assert len(trattenuti) == 1 and _ordini(c) == 0
    return trattenuti


@pytest.mark.parametrize("blocco", ["superamento", "storia_insufficiente", "picco_non_persistito"])
def test_queued_entry_never_reaches_the_broker_after_the_drawdown_block(tmp_path, monkeypatch, blocco):
    """D-631-13 (P1): un ingresso approvato e messo in coda prima del blocco non arriva al broker dopo: il blocco del
    drawdown si ricontrolla al punto finale d'invio del motore (come l'emergenza giornaliera). Rifiuto certo prima
    dell'invio (nessun esito AMBIGUOUS)."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    trattenuti = _ingresso_in_coda(c)
    stop = c.rc._hard_stop()
    if blocco == "superamento":
        _lettura(c, 85.0)
        assert stop.breached
    elif blocco == "storia_insufficiente":
        stop._mark_history_insufficient("test", 1.0, Decimal("90"), stop.peak_epoch)
    else:
        def guasto(values):
            raise OSError("disco pieno")
        monkeypatch.setattr(c.rc.db, "save_settings", guasto)
        monkeypatch.setattr(modulo_hard_stop(), "atomic_write_text", lambda *a, **k: (_ for _ in ()).throw(OSError()))
        stop.observe(150.0)
        assert stop.persist_failed
    for comando in trattenuti:
        c.rc.bus.publish("CMD_QUICK_BET", comando)
    assert _ordini(c) == 0
    assert not c.rc._uncertain_reservations_map()                # rifiuto certo, nessuna incertezza


def modulo_hard_stop():
    import core.drawdown_hard_stop as modulo
    return modulo


def test_queued_entry_without_any_block_is_still_placed(tmp_path):
    """Controllo positivo: senza blocco il comando in coda arriva al broker."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    trattenuti = _ingresso_in_coda(c)
    for comando in trattenuti:
        c.rc.bus.publish("CMD_QUICK_BET", comando)
    assert _ordini(c) == 1


def test_protective_exits_stay_allowed_with_the_drawdown_block(tmp_path):
    """Le uscite protettive (hedge del cashout) non passano dal punto d'invio delle entrate: con l'hard-stop attivo il
    controllo d'autorita' del cashout resta libero."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    _lettura(c, 85.0)
    assert c.rc._hard_stop().breached
    assert c.rc.cashout_authority_check() == ""


def _start_reale(tmp_path, monkeypatch, cassa, *, telegram_fallisce=False, nome="start.db", **cfg):
    from core import blackbox_runtime
    from tests.acceptance.test_issue461_pr29c_settlement_processes_and_schema import _app_su
    monkeypatch.chdir(tmp_path)
    if blackbox_runtime.get() is None:
        blackbox_runtime.install(str(tmp_path / "bb" / "blackbox.sqlite"), entrypoint="test_d1_c16")
    app = _app_su(str(tmp_path / nome), monkeypatch)
    rc = app.runtime
    carica = rc.settings_service.load_roserpina_config

    def con_limite():
        config = carica()
        for chiave, valore in (cfg or {"max_drawdown_abs": 10.0}).items():
            setattr(config, chiave, valore)
        return config

    monkeypatch.setattr(rc.settings_service, "load_roserpina_config", con_limite)
    rc.betfair_service.get_account_funds = lambda: _fondi(cassa)
    if telegram_fallisce:
        def giu(*a, **k):
            raise RuntimeError("telegram non raggiungibile")
        app.telegram_service.start = giu
    return app, rc


def _lock_di(rc):
    import os
    return os.path.normcase(os.path.realpath(rc.db.db_path)) + ".runtime.lock"


@pytest.mark.real_blackbox
@pytest.mark.parametrize("cassa", [85.0, 95.0])
def test_failed_start_after_a_startup_lockdown_releases_what_it_owns(tmp_path, monkeypatch, cassa):
    """D-631-14 (P2): picco 100, START con cassa 85 (superamento => LOCKDOWN durante l'avvio), Telegram fallisce.
    L'avvio fallito torna a STOPPED e smonta: lock di istanza, autorita' e sessione del drawdown rilasciati; il
    superamento resta su disco; un nuovo START e' possibile e ritrova il blocco. Con 95 (nessun superamento) idem."""
    import os
    from core import blackbox_runtime
    from core.system_state import RuntimeMode
    app, rc = _start_reale(tmp_path, monkeypatch, cassa, telegram_fallisce=True)
    altro = DrawdownHardStop(rc.db)
    altro.set_scope(_AMBITO_NON_MARCATO)                       # S18-1: stesso ambito delle letture dell'harness
    altro.restore()
    altro.observe(100.0)
    try:
        with pytest.raises(RuntimeError, match="telegram non raggiungibile"):
            rc.start(execution_mode="SIMULATION")
        assert rc.mode is RuntimeMode.STOPPED
        assert getattr(rc, "_instance_lock_fh", None) is None          # lock di istanza rilasciato
        authority = getattr(rc, "runtime_authority", None)
        assert authority is None or authority.released
        stato = json.loads(rc.db.get_settings()[STATE_KEY])
        assert stato["session_open"] is False
        assert stato["breached"] is (cassa == 85.0)                 # il blocco finanziario resta
        # nuovo START coerente: Telegram torna, il blocco (se c'era) viene riletto
        app.telegram_service.start = lambda *a, **k: {"started": False, "stub": True}
        esito = rc.start(execution_mode="SIMULATION")
        assert esito.get("started") is True, esito
        assert rc._hard_stop().breached is (cassa == 85.0)
    finally:
        try:
            app.stop()
        except Exception:
            pass
        blackbox_runtime.shutdown()


def test_start_opens_the_durable_session_before_any_funds_reading():
    """D-631-15: la sessione si apre su disco PRIMA della lettura dei fondi dello START."""
    import inspect
    from core.runtime_controller import RuntimeController
    src = inspect.getsource(RuntimeController._start_with_instance_lock)
    assert src.index("self._open_drawdown_session()") < src.index("start_funds = time.monotonic()")
    assert src.index("self._reload_drawdown_state()") < src.index("self._open_drawdown_session()")


@pytest.mark.real_blackbox
def test_start_is_refused_when_the_drawdown_session_cannot_be_made_durable(tmp_path, monkeypatch):
    """D-631-15: DB e marker non scrivibili allo START => nessuna lettura dei fondi, nessun trading, START interrotto
    (fail-closed) e risorse rilasciate. Il picco salvato resta intatto."""
    import os
    from core import blackbox_runtime
    from core.system_state import RuntimeMode
    app, rc = _start_reale(tmp_path, monkeypatch, 130.0)
    altro = DrawdownHardStop(rc.db)
    altro.set_scope(_AMBITO_NON_MARCATO)                       # S18-1: stesso ambito delle letture dell'harness
    altro.restore()
    altro.observe(100.0)
    letture = []
    rc.betfair_service.get_account_funds = lambda: (letture.append(1), _fondi(130.0))[1]
    reale = rc.db.save_settings

    def guasto(values):
        if STATE_KEY in values:
            raise OSError("disco pieno")
        return reale(values)

    monkeypatch.setattr(rc.db, "save_settings", guasto)
    monkeypatch.setattr(modulo_hard_stop(), "atomic_write_text", lambda *a, **k: (_ for _ in ()).throw(OSError()))
    try:
        with pytest.raises(RuntimeError, match="DRAWDOWN_SESSION_NOT_DURABLE"):
            rc.start(execution_mode="SIMULATION")
        assert letture == [] and rc.mode is RuntimeMode.STOPPED
        assert getattr(rc, "_instance_lock_fh", None) is None          # lock di istanza rilasciato
        stato = json.loads(reale.__self__.get_settings()[STATE_KEY])
        assert stato["peak"] == "100.0" and stato["session_open"] is False
    finally:
        try:
            app.stop()
        except Exception:
            pass
        blackbox_runtime.shutdown()


def test_double_storage_fault_during_start_then_crash_never_loses_the_observed_peak(tmp_path, monkeypatch):
    """D-631-15 (P1), riproduzione di Sol: picco 100 su disco, sessione precedente chiusa. START: sessione aperta su
    disco (prima delle letture); poi DB e marker giu', lettura 130 non salvata; crash; riavvio a 115 con lo storage
    tornato. La sessione risulta aperta: baseline non verificata, nessun ingresso fino alla conferma owner (con il
    picco dichiarato). Prima la sessione si apriva DOPO la lettura e, con lo storage giu', su disco restava chiusa:
    baseline verificata e 115 ammesso contro il picco 100."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    c.rc._hard_stop().open_session()
    c.rc._hard_stop().close_session()
    c.rc._reload_drawdown_state()
    c.rc._open_drawdown_session()                                # START: prima delle letture
    reale = _failing_db_and_marker(c, monkeypatch)
    _lettura(c, 130.0)                                           # picco osservato, non salvato
    monkeypatch.undo()
    c.rc.db.save_settings = reale
    _riavvio(c)                                                  # crash, storage tornato
    stop = c.rc._hard_stop()
    assert not stop.baseline_verified and stop.snapshot()["peak"] == 100.0   # nessuna presunzione sul massimo
    _lettura(c, 115.0)
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 0 and _rifiuti(c)[-1] == "drawdown_baseline_non_verificata"
    esito = c.rc.owner_confirm_drawdown_baseline(**_CONFIRM, declared_peak=130.0)
    assert esito["confirmed"] is True and c.rc._hard_stop().snapshot()["peak"] == 130.0
    _lettura(c, 115.0)
    assert c.rc._hard_stop().breached                            # 130 -> 115 = 15 >= 10


# ------------------------------------------------- ciclo 17 (PF-OWNER-PR631-CYCLE17-GO-20261112): D-631-13/16/17/18
@pytest.mark.parametrize("blocco", ["lockdown_amministrativo", "lockdown_da_altra_origine", "emergency_stop"])
def test_queued_entry_never_reaches_the_broker_under_any_lockdown(tmp_path, blocco):
    """D-631-13 (Claude D su 9c9121f0): qualsiasi LOCKDOWN attivo nega l'ingresso gia' in coda al punto finale d'invio,
    qualunque ne sia l'origine (amministrativo, rischio, desk), come l'Emergency Stop. Rifiuto certo prima dell'invio."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    trattenuti = _ingresso_in_coda(c)
    if blocco == "lockdown_amministrativo":
        c.rc.force_lockdown("owner_lockdown")
    elif blocco == "lockdown_da_altra_origine":
        c.rc.mode = c.rc.mode.__class__.LOCKDOWN                 # LOCKDOWN impostato da un altro componente
    else:
        c.rc._emergency_stopped = True
    for comando in trattenuti:
        c.rc.bus.publish("CMD_QUICK_BET", comando)
    assert _ordini(c) == 0
    assert not c.rc._uncertain_reservations_map()


def test_final_entry_check_reports_lockdown_with_its_reason(tmp_path):
    """D-631-13: il controllo finale restituisce un motivo esplicito per il LOCKDOWN, '' senza blocchi."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    assert c.rc.drawdown_entry_block_reason() == ""
    c.rc.force_lockdown("owner_lockdown")
    assert c.rc.drawdown_entry_block_reason().startswith("runtime_lockdown")


def test_final_entry_check_is_atomic_with_a_concurrent_breach(tmp_path, monkeypatch):
    """D-631-16 (Sol su 9c9121f0): il controllo finale e la registrazione di un superamento sono serializzati. Un
    superamento concorrente o finisce prima del controllo (=> rifiuto) o dopo che il controllo e' terminato: mai un
    '' restituito quando il superamento era gia' registrato prima della risposta."""
    import threading
    import time as _time
    from core.runtime_controller import RuntimeController
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    stop = c.rc._hard_stop()
    momenti = {}
    originale = RuntimeController._drawdown_persist_block_reason

    def concorrente():
        _lettura(c, 85.0)                                      # superamento registrato da un altro thread
        momenti["superamento"] = _time.monotonic()

    def intercetta(self):
        t = threading.Thread(target=concorrente)
        t.start()
        t.join(timeout=0.5)                                    # con il controllo atomico il thread aspetta
        return originale(self)

    monkeypatch.setattr(RuntimeController, "_drawdown_persist_block_reason", intercetta)
    esito = c.rc.drawdown_entry_block_reason()
    risposta = _time.monotonic()
    deadline = _time.monotonic() + 5
    while "superamento" not in momenti and _time.monotonic() < deadline:
        _time.sleep(0.01)
    assert stop.breached
    assert esito != "" or momenti["superamento"] > risposta, (esito, momenti, risposta)


def _sessione_lasciata_aperta_e_baseline_confermata(c, dichiarato):
    _lettura(c, 100.0)
    c.rc._hard_stop().open_session()
    _riavvio(c)
    c.rc._reload_drawdown_state()
    assert c.rc._hard_stop().baseline_block_reason()
    _lettura(c, 120.0)
    assert c.rc.owner_confirm_drawdown_baseline(**_CONFIRM, declared_peak=dichiarato)["confirmed"]


def test_late_dip_after_owner_baseline_confirmation_is_measured_from_the_declared_peak(tmp_path):
    """D-631-17 (Codex su 9c9121f0, riprodotto da B e D): il picco dichiarato dall'owner alla conferma della baseline
    e' il massimo noto da quell'istante. 130 a t+3 e poi 115 arrivata da t+2 => calo 15 dal picco 130, oltre il limite
    10: superamento, mai perso perche' il picco dichiarato non entrava nella cronologia."""
    import time as _time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _sessione_lasciata_aperta_e_baseline_confermata(c, 130.0)
    stop = c.rc._hard_stop()
    assert stop.peak == Decimal("130")
    base = _time.monotonic() + 100
    c.rc._apply_funds_reading(_fondi(130.0), source="t", read_at=base + 3)
    c.rc._apply_funds_reading(_fondi(115.0), source="t", read_at=base + 2)
    assert stop.breached
    assert Decimal(stop.snapshot()["breach"]["peak"]) == Decimal("130")


def test_owner_baseline_confirmation_never_lowers_the_declared_or_saved_peak(tmp_path):
    """D-631-17: la conferma non sostituisce mai il picco con uno inferiore: dichiarato 90 sotto il salvato 120 =>
    resta 120; e il calo successivo si misura da li'."""
    import time as _time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _sessione_lasciata_aperta_e_baseline_confermata(c, 90.0)
    stop = c.rc._hard_stop()
    assert stop.peak == Decimal("120")
    base = _time.monotonic() + 100
    c.rc._apply_funds_reading(_fondi(125.0), source="t", read_at=base + 3)
    c.rc._apply_funds_reading(_fondi(108.0), source="t", read_at=base + 2)
    assert stop.breached and Decimal(stop.snapshot()["breach"]["peak"]) == Decimal("120")


def test_declared_peak_confirmed_by_another_process_is_adopted_for_late_readings(tmp_path):
    """D-631-17, subentro: la conferma salvata da un altro processo, adottata alla rilettura, apre la cronologia dal
    picco dichiarato: un calo tardivo successivo si misura da quel picco."""
    import time as _time
    a = _catena_su(tmp_path, "conferma.db", max_drawdown_abs=10.0)
    _lettura(a, 100.0)
    a.rc._hard_stop().open_session()
    b = _catena_su(tmp_path, "conferma.db", max_drawdown_abs=10.0)
    b.rc._reload_drawdown_state()
    b.rc.mode = b.rc.mode.__class__.ACTIVE
    _riavvio(a)
    a.rc._reload_drawdown_state()
    _lettura(a, 100.0)
    assert a.rc.owner_confirm_drawdown_baseline(**_CONFIRM, declared_peak=130.0)["confirmed"]
    b.rc._reload_drawdown_state()
    stop = b.rc._hard_stop()
    assert stop.peak == Decimal("130") and not stop.baseline_block_reason()
    base = _time.monotonic() + 100
    b.rc._apply_funds_reading(_fondi(130.0), source="t", read_at=base + 3)
    b.rc._apply_funds_reading(_fondi(115.0), source="t", read_at=base + 2)
    assert stop.breached


def _proprietario_con_superamento_poi_fermo(tmp_path, nome):
    a = _catena_su(tmp_path, nome, max_drawdown_abs=10.0)
    b = _catena_su(tmp_path, nome, max_drawdown_abs=10.0)       # costruito prima che A scriva
    _avvia_con_autorita(a)
    _lettura(a, 100.0)
    _lettura(a, 130.0)
    _lettura(a, 115.0)
    assert a.rc._hard_stop().breached
    salvato = json.loads(a.rc.db.get_settings()[STATE_KEY])
    _stop_pulito(a)
    return a, b, salvato


def _stato_salvato(c):
    return json.loads(c.rc.db.get_settings()[STATE_KEY])


@pytest.mark.parametrize("azione", ["pause", "lettura_di_stato", "ingresso"])
def test_never_started_controller_never_rewrites_the_owner_breach(tmp_path, azione):
    """D-631-18 (Codex su 9c9121f0, riprodotto da B e D): un controller mai avviato, costruito prima delle scritture
    del proprietario, non modifica lo stato finanziario persistito dopo lo STOP del proprietario (lease rilasciata):
    ne' con pause(), ne' leggendo il conto per lo stato, ne' su un ingresso. Il superamento resta."""
    import time as _time
    a, b, salvato = _proprietario_con_superamento_poi_fermo(tmp_path, f"stantio_{azione}.db")
    b.rc.betfair_service.get_account_funds = lambda: _fondi(120.0)
    b.rc.betfair_service.disconnect = lambda *x, **k: None
    if azione == "pause":
        b.rc.pause()
    elif azione == "lettura_di_stato":
        b.rc.mode = b.rc.mode.__class__.ACTIVE
        b.rc._apply_funds_reading(_fondi(120.0), source="status", read_at=_time.monotonic())
    else:
        b.rc.mode = b.rc.mode.__class__.ACTIVE
        b.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
        assert _ordini(b) == 0
    dopo = _stato_salvato(a)
    assert dopo["breached"] is True and Decimal(dopo["peak"]) == Decimal("130")
    assert dopo["seq"] >= salvato["seq"]


def test_stale_snapshot_never_overwrites_a_newer_persisted_record(tmp_path):
    """D-631-18, causa nella persistenza: uno stato in memoria piu' vecchio del record su disco (seq inferiore) non lo
    sovrascrive mai, qualunque sia il chiamante. Il salvataggio rifiutato nega gli ingressi di quell'istanza."""
    c = _catena(tmp_path)
    vecchio = DrawdownHardStop(c.rc.db)
    vecchio.restore(reconcile=False)
    nuovo = DrawdownHardStop(c.rc.db)
    nuovo.restore(reconcile=False)
    nuovo.observe(100.0)
    nuovo.observe(130.0)
    assert nuovo._memorize_if_exceeded(10.0, None, 115.0, nuovo.peak)
    vecchio.observe(120.0)                                       # snapshot stantio: seq 0 contro il disco piu' avanti
    record = json.loads(c.rc.db.get_settings()[STATE_KEY])
    assert record["breached"] is True and Decimal(record["peak"]) == Decimal("130")
    assert vecchio.persist_failed
    assert vecchio.retry_persist() is False                      # resta rifiutato finche' non rilegge
    vecchio.restore(reconcile=False)
    assert vecchio.breached and vecchio.peak == Decimal("130")


@pytest.mark.parametrize("bloccato", [True, False])
def test_manual_quick_bet_topic_hits_the_same_final_check(tmp_path, bloccato):
    """D-631-13: anche la richiesta manuale (`REQ_QUICK_BET`) arriva allo stesso controllo finale del motore: in
    LOCKDOWN nessun ordine, senza blocco l'ordine parte (controllo positivo)."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    trattenuti = _ingresso_in_coda(c)
    if bloccato:
        c.rc.force_lockdown("owner_lockdown")
    for comando in trattenuti:
        c.rc.bus.publish("REQ_QUICK_BET", comando)
    assert _ordini(c) == (0 if bloccato else 1)


def test_reading_started_before_the_baseline_confirmation_is_never_silently_passed(tmp_path):
    """D-631-17: una lettura iniziata prima della conferma e arrivata dopo e' precedente alla storia del picco
    confermato: con un calo possibile oltre il limite non e' un PASS silenzioso (storia insufficiente o superamento)."""
    import time as _time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    c.rc._hard_stop().open_session()
    _riavvio(c)
    c.rc._reload_drawdown_state()
    prima = _time.monotonic()
    _lettura(c, 120.0)
    assert c.rc.owner_confirm_drawdown_baseline(**_CONFIRM, declared_peak=130.0)["confirmed"]
    c.rc._apply_funds_reading(_fondi(118.0), source="t", read_at=_time.monotonic() + 50)
    c.rc._apply_funds_reading(_fondi(115.0), source="t", read_at=prima + 0.0001)
    stop = c.rc._hard_stop()
    assert stop.breached or stop.history_block_reason()
    assert c.rc.drawdown_entry_block_reason() != ""


def test_confirmed_peak_survives_restart_and_takeover(tmp_path):
    """D-631-17: il picco dichiarato e confermato e' durevole: dopo un riavvio (stesso DB) non torna piu' basso e la
    baseline resta verificata finche' la sessione si chiude in modo pulito."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _sessione_lasciata_aperta_e_baseline_confermata(c, 130.0)
    _riavvio(c)
    c.rc._reload_drawdown_state()
    assert c.rc._hard_stop().peak == Decimal("130")


def test_proper_takeover_after_owner_stop_inherits_the_breach_and_writes_again(tmp_path):
    """D-631-18, controllo positivo: il controller costruito prima, quando si avvia davvero (lock, lease, rilettura),
    eredita il superamento del proprietario precedente e torna a poter salvare: la protezione dagli snapshot vecchi non
    blocca il subentro legittimo."""
    a, b, salvato = _proprietario_con_superamento_poi_fermo(tmp_path, "subentro.db")
    _avvia_con_autorita(b)
    stop = b.rc._hard_stop()
    assert stop.breached and stop.peak == Decimal("130")
    assert b.rc._drawdown_writes_allowed()
    stop.observe(140.0)
    dopo = _stato_salvato(b)
    assert dopo["breached"] is True and Decimal(dopo["peak"]) == Decimal("140") and dopo["seq"] > salvato["seq"]
    assert not stop.persist_failed


def test_final_entry_check_names_the_breach_before_the_lockdown_it_caused(tmp_path):
    """D-631-13/16: con l'hard-stop superato (che ha anche portato il runtime in LOCKDOWN) il controllo finale riporta il
    motivo del superamento, piu' preciso del LOCKDOWN che ne deriva."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    _lettura(c, 85.0)
    assert c.rc._hard_stop().breached
    assert c.rc.drawdown_entry_block_reason() == REASON_ACTIVE


def test_non_owner_with_the_current_seq_never_retries_its_unsaved_peak(tmp_path):
    """Grok su 6d4ac43f, con la concorrenza ottimistica del ciclo 17: anche un controller non proprietario allineato al
    disco (stesso seq) non ritenta il salvataggio del proprio picco non persistito sul record del proprietario: nega
    l'ingresso e il disco resta com'e'."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    stop = c.rc._hard_stop()
    stop.peak = Decimal("150")                                   # picco solo in memoria, salvataggio fallito
    stop.persist_failed = True
    c.rc.mode = c.rc.mode.__class__.STOPPED                      # mai avviato e fermo: non proprietario
    assert c.rc._drawdown_write_block()
    assert c.rc._drawdown_persist_block_reason() == "drawdown_picco_non_persistito"
    assert Decimal(_stato_salvato(c)["peak"]) == Decimal("100")


# ------------------------------------------------------- ciclo 18 (PF-OWNER-PR631-CYCLE18-GO-20261112): G17-1
def _riavvio_con_baseline_da_confermare(c):
    _lettura(c, 100.0)
    c.rc._hard_stop().open_session()
    _riavvio(c)
    c.rc._reload_drawdown_state()
    _lettura(c, 120.0)


@pytest.mark.parametrize("ordine", ["in_ordine", "fuori_ordine"])
def test_reading_started_before_baseline_confirmation_is_insufficient_history_never_a_false_hard_stop(tmp_path, ordine):
    """G17-1 (Grok su bcd19a32, riprodotto da B e D): conferma owner con picco dichiarato 130; una lettura 115 iniziata
    PRIMA della conferma e arrivata dopo non dimostra un calo dal 130 (il picco dichiarato non ha un istante noto: prima
    della lettura si conoscono solo 100 e 120, calo provabile 5). Superamento possibile ma non dimostrabile => storia
    insufficiente (ingressi negati, motivo distinto), mai un hard-stop. Vale arrivata in ordine e fuori ordine."""
    import time as _time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _riavvio_con_baseline_da_confermare(c)
    prima = _time.monotonic()
    if ordine == "fuori_ordine":
        prima -= 0.0005                                          # precede anche l'ultima lettura valutata (120)
    assert c.rc.owner_confirm_drawdown_baseline(**_CONFIRM, declared_peak=130.0)["confirmed"]
    c.rc._apply_funds_reading(_fondi(115.0), source="t", read_at=prima)
    stop = c.rc._hard_stop()
    assert not stop.breached
    assert stop.history_block_reason()
    assert c.rc.drawdown_entry_block_reason() == stop.history_block_reason()
    assert c.rc.mode is not c.rc.mode.__class__.LOCKDOWN or not stop.breached


def test_reading_started_before_baseline_confirmation_without_any_possible_breach_does_not_block(tmp_path):
    """G17-1, terzo stato: lettura iniziata prima della conferma con un calo massimo possibile sotto il limite (130 ->
    125 = 5 < 10) => nessun blocco dal drawdown (ne' hard-stop ne' storia insufficiente)."""
    import time as _time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _riavvio_con_baseline_da_confermare(c)
    prima = _time.monotonic()
    assert c.rc.owner_confirm_drawdown_baseline(**_CONFIRM, declared_peak=130.0)["confirmed"]
    c.rc._apply_funds_reading(_fondi(125.0), source="t", read_at=prima)
    stop = c.rc._hard_stop()
    assert not stop.breached and not stop.history_block_reason()
    assert c.rc.drawdown_entry_block_reason() == ""


def test_dip_proven_after_the_baseline_confirmation_is_still_a_hard_stop(tmp_path):
    """G17-1, primo stato: dopo la conferma un calo dimostrato dalle letture successive resta un hard-stop (nessuna
    perdita reale ignorata), anche con una lettura pre-conferma arrivata nel mezzo."""
    import time as _time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _riavvio_con_baseline_da_confermare(c)
    prima = _time.monotonic()
    assert c.rc.owner_confirm_drawdown_baseline(**_CONFIRM, declared_peak=130.0)["confirmed"]
    base = _time.monotonic() + 100
    c.rc._apply_funds_reading(_fondi(129.0), source="t", read_at=prima)          # pre-conferma, nessun calo possibile
    c.rc._apply_funds_reading(_fondi(140.0), source="t", read_at=base + 1)
    c.rc._apply_funds_reading(_fondi(125.0), source="t", read_at=base + 2)       # 140 -> 125 = 15 dimostrato
    stop = c.rc._hard_stop()
    assert stop.breached and Decimal(stop.snapshot()["breach"]["peak"]) == Decimal("140")


def test_baseline_confirmation_records_its_instant_and_epoch(tmp_path):
    """G17-1: la conferma registra istante (processo) ed epoca (numero progressivo delle conferme, persistito nella
    storia della baseline); ogni conferma successiva avanza l'epoca."""
    import time as _time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _riavvio_con_baseline_da_confermare(c)
    prima = _time.monotonic()
    assert c.rc.owner_confirm_drawdown_baseline(**_CONFIRM, declared_peak=130.0)["confirmed"]
    stop = c.rc._hard_stop()
    assert stop.baseline_confirmed_at is not None and stop.baseline_confirmed_at >= prima
    assert stop.baseline_epoch() == 1
    stop.open_session()
    _riavvio(c)                                                  # nuovo crash a sessione aperta: nuova conferma richiesta
    c.rc._reload_drawdown_state()
    _lettura(c, 120.0)
    assert c.rc.owner_confirm_drawdown_baseline(**dict(_CONFIRM, operation_id="PF-BASELINE-0002"))["confirmed"]
    assert c.rc._hard_stop().baseline_epoch() == 2


def test_confirmation_adopted_from_another_process_also_marks_earlier_readings_as_pre_confirmation(tmp_path):
    """G17-1, subentro: la conferma salvata da un altro processo e adottata alla rilettura vale da quell'istante anche
    qui: una lettura iniziata prima dell'adozione con un calo possibile ma non dimostrabile => storia insufficiente."""
    import time as _time
    a = _catena_su(tmp_path, "epoca.db", max_drawdown_abs=10.0)
    _riavvio_con_baseline_da_confermare(a)
    b = _catena_su(tmp_path, "epoca.db", max_drawdown_abs=10.0)
    b.rc._reload_drawdown_state()
    b.rc.mode = b.rc.mode.__class__.ACTIVE
    _lettura(b, 120.0)
    prima = _time.monotonic()
    assert a.rc.owner_confirm_drawdown_baseline(**_CONFIRM, declared_peak=130.0)["confirmed"]
    b.rc._hard_stop().restore(reconcile=False)                   # B adotta la conferma di A
    stop = b.rc._hard_stop()
    assert stop.peak == Decimal("130") and stop.baseline_epoch() == 1
    b.rc._apply_funds_reading(_fondi(115.0), source="t", read_at=prima)
    assert not stop.breached and stop.history_block_reason()


@pytest.mark.parametrize("cassa,quando,esito", [
    (115.0, "prima", "storia_insufficiente"),
    (115.0, "fuori_ordine", "storia_insufficiente"),
    (105.0, "prima", "hard_stop"),                               # 120 -> 105 dimostrato: mai declassato
    (125.0, "prima", "nessun_blocco"),
    (119.0, "prima", "storia_insufficiente"),
    (115.0, "dopo", "hard_stop"),
])
def test_owner_matrix_for_readings_around_the_baseline_confirmation(tmp_path, cassa, quando, esito):
    """G17-1, matrice owner/Claude D (#628 M50): lettura 120 a t0 (con il suo istante), conferma owner a 130, limite
    10. Calo dimostrato dalle letture => hard-stop; possibile solo rispetto al picco dichiarato => storia insufficiente;
    impossibile => nessun blocco. Una correzione di G17-1 non deve eliminare un hard-stop legittimo (caso 105)."""
    import time as _time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    c.rc._hard_stop().open_session()
    _riavvio(c)
    c.rc._reload_drawdown_state()
    t0 = _time.monotonic()
    c.rc._apply_funds_reading(_fondi(120.0), source="t", read_at=t0)
    assert c.rc.owner_confirm_drawdown_baseline(**_CONFIRM, declared_peak=130.0)["confirmed"]
    confermata = c.rc._hard_stop().baseline_confirmed_at
    assert confermata > t0
    at = {"prima": (t0 + confermata) / 2,                        # dopo 120, prima della conferma (mai dipendente dai tempi)
          "fuori_ordine": t0 - 0.001, "dopo": confermata + 1}[quando]
    c.rc._apply_funds_reading(_fondi(cassa), source="t", read_at=at)
    stop = c.rc._hard_stop()
    ottenuto = ("hard_stop" if stop.breached
                else "storia_insufficiente" if stop.history_block_reason() else "nessun_blocco")
    assert ottenuto == esito
    assert (c.rc.drawdown_entry_block_reason() == "") is (esito == "nessun_blocco")


def test_epoch_one_reading_delivered_after_a_second_confirmation_is_never_proof_against_the_new_base(tmp_path):
    """G17-1, matrice M50: una lettura dell'epoca 1 (iniziata prima della seconda conferma) consegnata dopo la seconda
    conferma non e' mai una prova contro la nuova base: al piu' storia insufficiente."""
    import time as _time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    c.rc._hard_stop().open_session()
    _riavvio(c)
    c.rc._reload_drawdown_state()
    t0 = _time.monotonic()
    c.rc._apply_funds_reading(_fondi(120.0), source="t", read_at=t0)
    assert c.rc.owner_confirm_drawdown_baseline(**_CONFIRM, declared_peak=125.0)["confirmed"]
    epoca_uno = _time.monotonic()
    c.rc._hard_stop().open_session()
    _riavvio(c)
    c.rc._reload_drawdown_state()
    assert c.rc.owner_confirm_drawdown_baseline(**dict(_CONFIRM, operation_id="PF-BASELINE-0002"),
                                                declared_peak=130.0)["confirmed"]
    assert c.rc._hard_stop().baseline_epoch() == 2
    c.rc._apply_funds_reading(_fondi(119.0), source="t", read_at=epoca_uno)
    assert not c.rc._hard_stop().breached


# ------------------------------------------------- ciclo 19 (PF-OWNER-PR631-CYCLE19-GO / SCOPE-AMENDMENT-20261112)
def _letture_prima_della_conferma(c, letture, cap=15.0):
    """Baseline da confermare; letture (offset, cassa) con istanti tutti precedenti alla conferma owner (senza picco
    dichiarato). Restituisce l'istante base."""
    import time as _time
    _lettura(c, 100.0)
    c.rc._hard_stop().open_session()
    _riavvio(c)
    c.rc._reload_drawdown_state()
    t = _time.monotonic()
    _time.sleep(0.01)
    for offset, cassa in letture:
        c.rc._apply_funds_reading(_fondi(cassa), source="t", read_at=t + offset)
    assert c.rc.owner_confirm_drawdown_baseline(**_CONFIRM)["confirmed"]
    assert c.rc._hard_stop().baseline_confirmed_at > t + max(o for o, _ in letture)
    return t


def test_s19a_proven_dip_after_a_late_reading_is_a_hard_stop_not_insufficient_history(tmp_path):
    """S19-A (Sol su 5bb61f7f, riprodotto da B e D): 100, 90, 130 prima della conferma, poi 110 tardiva fra 100 e 90
    (limite 15). Il calo 110 -> 90 = 20 e' dimostrato dalle letture conservate: hard-stop, mai storia insufficiente
    (il picco confermato 130 e' solo un limite superiore e non deve anticipare l'incertezza)."""
    c = _catena(tmp_path, max_drawdown_abs=15.0)
    t = _letture_prima_della_conferma(c, [(0.001, 100.0), (0.003, 90.0), (0.004, 130.0)])
    c.rc._apply_funds_reading(_fondi(110.0), source="t", read_at=t + 0.002)
    stop = c.rc._hard_stop()
    assert stop.breached and not stop.history_block_reason()
    assert Decimal(stop.snapshot()["breach"]["loss"]) == Decimal("20")


@pytest.mark.parametrize("ordine", [(0, 1, 2), (0, 2, 1), (1, 0, 2), (1, 2, 0), (2, 0, 1), (2, 1, 0)])
def test_s19a_proven_dip_between_late_readings_in_any_order(tmp_path, ordine):
    """S19-A, famiglia dei casi casuali di Claude D: letture iniziate prima della conferma (picco dichiarato 130) e
    consegnate dopo, in ordine qualunque. 112 -> 95 = 17 >= 15 e' dimostrato dalle letture: hard-stop qualunque sia
    l'ordine di arrivo (uno stato di storia insufficiente segnato prima, quando il calo era davvero solo possibile,
    puo' restare: bloccano entrambi; il superamento dimostrato non si perde)."""
    import time as _time
    c = _catena(tmp_path, max_drawdown_abs=15.0)
    _lettura(c, 100.0)
    c.rc._hard_stop().open_session()
    _riavvio(c)
    c.rc._reload_drawdown_state()
    t = _time.monotonic()
    _time.sleep(0.01)
    c.rc._apply_funds_reading(_fondi(98.0), source="t", read_at=t + 0.001)
    assert c.rc.owner_confirm_drawdown_baseline(**_CONFIRM, declared_peak=130.0)["confirmed"]
    tardive = [(t + 0.002, 112.0), (t + 0.004, 95.0), (t + 0.006, 129.0)]
    for i in ordine:
        istante, cassa = tardive[i]
        c.rc._apply_funds_reading(_fondi(cassa), source="t", read_at=istante)
    stop = c.rc._hard_stop()
    assert stop.breached
    assert Decimal(stop.snapshot()["breach"]["loss"]) >= Decimal("15")


def test_s19b_failed_owner_rebase_restores_the_confirmed_peak(tmp_path, monkeypatch):
    """S19-B (Sol su 5bb61f7f, riprodotto da B e D): un ribasamento owner con salvataggio fallito ripristina tutto lo
    stato precedente, picco confermato incluso: una lettura iniziata prima della conferma resta valutata contro il
    limite superiore 130 (119 => storia insufficiente, mai un passaggio senza blocco)."""
    import time as _time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    c.rc._hard_stop().open_session()
    _riavvio(c)
    c.rc._reload_drawdown_state()
    t0 = _time.monotonic()
    c.rc._apply_funds_reading(_fondi(120.0), source="t", read_at=t0)
    assert c.rc.owner_confirm_drawdown_baseline(**_CONFIRM, declared_peak=130.0)["confirmed"]
    stop = c.rc._hard_stop()
    prima = (stop._confirmed_peak, stop.baseline_confirmed_at, stop.peak, stop.peak_epoch)
    reale = c.rc.db.save_settings

    def guasto(values):
        raise OSError("disco pieno")

    monkeypatch.setattr(c.rc.db, "save_settings", guasto)
    monkeypatch.setattr(modulo_hard_stop(), "atomic_write_text", lambda *a, **k: (_ for _ in ()).throw(OSError()))
    assert stop.owner_rebase_peak(200.0, authorization="#628", reason="test", operation_id="REB-S19B", confirm=True) is False
    assert (stop._confirmed_peak, stop.baseline_confirmed_at, stop.peak, stop.peak_epoch) == prima
    monkeypatch.setattr(c.rc.db, "save_settings", reale)
    c.rc._apply_funds_reading(_fondi(119.0), source="t", read_at=(t0 + stop.baseline_confirmed_at) / 2)
    assert stop.breached or stop.history_block_reason()
    assert c.rc.drawdown_entry_block_reason() != ""


def test_s19b_rebase_retry_after_failure_succeeds_and_survives_restart(tmp_path, monkeypatch):
    """S19-B: dopo il fallimento, un nuovo tentativo con un operation_id nuovo riesce e il picco ribasato e' durevole
    (riavvio); il fallimento non lascia stato parziale su disco."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    stop = c.rc._hard_stop()
    reale = c.rc.db.save_settings
    salvato = _stato_salvato(c)

    def guasto(values):
        raise OSError("disco pieno")

    monkeypatch.setattr(c.rc.db, "save_settings", guasto)
    assert stop.owner_rebase_peak(80.0, authorization="#628", reason="test", operation_id="REB-1", confirm=True) is False
    monkeypatch.setattr(c.rc.db, "save_settings", reale)
    assert _stato_salvato(c) == salvato
    assert stop.owner_rebase_peak(80.0, authorization="#628", reason="test", operation_id="REB-2", confirm=True) is True
    _riavvio(c)
    assert c.rc._hard_stop().peak == Decimal("80")


_AMBITO_NON_MARCATO = {"env": "UNTAGGED", "account": "untagged"}


def _lettura_ambito(c, cassa, *, simulated, conto="conto-a", allinea=True):
    """Lettura del conto marcata come quella di BetfairService (``simulated``); in LIVE l'identita' del conto e' lo
    username di login del client connesso (None => conto non identificabile). ``allinea``: il runtime e' nella modalita'
    della lettura, come in produzione (BetfairService marca la lettura con la propria modalita'); False = lettura in volo
    di una modalita' diversa da quella corrente."""
    import types
    if allinea:
        c.rc.simulation_mode = bool(simulated)
    if not getattr(c, "conto_iniziato", False):
        c.rc.risk_desk.equity_peak = 0.0
        c.rc.risk_desk.bankroll_start = 0.0
        c.conto_iniziato = True
    c.rc.betfair_service.client = types.SimpleNamespace(username=conto) if conto else None
    fondi = {"available": cassa, "exposure": 0.0, "ok": True, "simulated": simulated}
    if simulated:
        fondi["total"] = cassa                                   # come il broker simulato (PFC-008: totale coerente)
    c.rc.betfair_service.get_account_funds = lambda: dict(fondi)
    return c.rc._apply_funds_reading(dict(fondi), source="test")


def test_s18_sim_peak_never_judges_the_live_account(tmp_path):
    """S18-1 (Sol su bcd19a32, riprodotto da B e D): SIM a 1000 poi LIVE a 100 con limite 10%. Nessun hard-stop falso
    memorizzato, picco SIM intatto, nuovi ingressi negati con il motivo distinto dell'ambito (fail-closed fino alla
    separazione completa della PR-04)."""
    from core.drawdown_hard_stop import REASON_SCOPE_MISMATCH
    c = _catena(tmp_path, max_drawdown_hard_stop_pct=10.0)
    _lettura_ambito(c, 1000.0, simulated=True)
    salvato = _stato_salvato(c)
    assert salvato["scope"] == {"env": "SIM", "account": "sim"}
    _lettura_ambito(c, 100.0, simulated=False)
    stop = c.rc._hard_stop()
    assert not stop.breached and stop.peak == Decimal("1000")
    assert _stato_salvato(c) == salvato                          # nessuna scrittura
    assert c.rc.drawdown_entry_block_reason() == REASON_SCOPE_MISMATCH
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 0


def test_s18_live_peak_never_judges_the_sim_account(tmp_path):
    """S18-1: LIVE poi SIM: il picco LIVE non giudica il conto simulato e non viene toccato."""
    from core.drawdown_hard_stop import REASON_SCOPE_MISMATCH
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura_ambito(c, 130.0, simulated=False)
    salvato = _stato_salvato(c)
    _lettura_ambito(c, 50.0, simulated=True)
    assert not c.rc._hard_stop().breached and _stato_salvato(c) == salvato
    assert c.rc.drawdown_entry_block_reason() == REASON_SCOPE_MISMATCH


def test_s18_live_account_a_never_judges_live_account_b(tmp_path):
    """S18-1: LIVE conto A poi LIVE conto B: stati separati, B bloccato, stato di A intatto."""
    from core.drawdown_hard_stop import REASON_SCOPE_MISMATCH
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura_ambito(c, 500.0, simulated=False, conto="conto-a")
    salvato = _stato_salvato(c)
    assert salvato["scope"]["env"] == "LIVE" and "conto-a" not in json.dumps(salvato)   # mai lo username in chiaro
    _lettura_ambito(c, 100.0, simulated=False, conto="conto-b")
    assert not c.rc._hard_stop().breached and _stato_salvato(c) == salvato
    assert c.rc.drawdown_entry_block_reason() == REASON_SCOPE_MISMATCH


def test_s18_unidentifiable_live_account_blocks_new_entries(tmp_path):
    """S18-1: conto LIVE non identificabile con certezza => nessuna valutazione e nessun ingresso."""
    from core.drawdown_hard_stop import REASON_ACCOUNT_UNKNOWN
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura_ambito(c, 100.0, simulated=False, conto=None)
    assert c.rc.drawdown_entry_block_reason() == REASON_ACCOUNT_UNKNOWN
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 0


def test_s18_switching_back_to_the_owning_scope_resumes_normally_without_any_reset(tmp_path):
    """S18-1, cambio in sessione: SIM -> LIVE -> SIM. Tornando all'ambito del record il drawdown riprende con il SUO
    picco (nessun azzeramento): un calo SIM vero resta un hard-stop."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura_ambito(c, 1000.0, simulated=True)
    _lettura_ambito(c, 100.0, simulated=False)
    _lettura_ambito(c, 995.0, simulated=True)
    assert c.rc.drawdown_entry_block_reason() == ""
    _lettura_ambito(c, 985.0, simulated=True)
    stop = c.rc._hard_stop()
    assert stop.breached and Decimal(stop.snapshot()["breach"]["peak"]) == Decimal("1000")


def test_s18_provenance_survives_stop_restart_and_takeover(tmp_path):
    """S18-1: la provenienza e' persistita: dopo riavvio, e in un altro processo sullo stesso DB, una lettura LIVE resta
    bloccata contro lo stato SIM; una lettura dello stesso ambito procede."""
    from core.drawdown_hard_stop import REASON_SCOPE_MISMATCH
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura_ambito(c, 1000.0, simulated=True)
    _riavvio(c)
    c.rc._reload_drawdown_state()
    _lettura_ambito(c, 100.0, simulated=False)
    assert c.rc.drawdown_entry_block_reason() == REASON_SCOPE_MISMATCH
    del c.rc._drawdown_hard_stop                                 # subentro: nuovo stato riletto dallo stesso DB
    _lettura_ambito(c, 995.0, simulated=True)
    assert c.rc.drawdown_entry_block_reason() == ""
    assert c.rc._hard_stop().peak == Decimal("1000")


def test_s18_owner_procedures_in_another_scope_never_touch_the_live_state(tmp_path):
    """S18-1 (prova di Claude D: rebase in SIM ribassava il picco LIVE). Fuori ambito ribasamento, sblocchi e conferma
    sono rifiutati: il picco LIVE 130 resta e il calo LIVE successivo e' un hard-stop."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura_ambito(c, 130.0, simulated=False)
    _lettura_ambito(c, 1000.0, simulated=True)                   # passaggio a SIM
    stop = c.rc._hard_stop()
    assert stop.owner_rebase_peak(100.0, authorization="#628", reason="t", operation_id="REB-SIM", confirm=True) is False
    assert c.rc.owner_unlock_drawdown_hard_stop(authorization="#628", reason="t", operation_id="U1",
                                                confirm=True)["unlocked"] is False
    assert c.rc.owner_clear_drawdown_history_insufficient(authorization="#628", reason="t", operation_id="C1",
                                                          confirm=True)["cleared"] is False
    assert c.rc.owner_confirm_drawdown_baseline(**_CONFIRM)["confirmed"] is False
    assert stop.peak == Decimal("130") and _stato_salvato(c)["peak"] == "130.0"
    _lettura_ambito(c, 115.0, simulated=False)                   # ritorno a LIVE: calo 15 vero
    assert stop.breached


def test_s18_state_without_provenance_is_quarantined_until_owner_assignment(tmp_path):
    """S18-1, migrazione conservativa: un record scritto prima della provenienza (nessun campo `scope`) non si
    attribuisce d'ufficio: quarantena (ingressi negati, nessuna scrittura), blocchi preservati. L'attribuzione owner lo
    assegna all'ambito corrente senza reset (picco e superamento restano) e rimette la baseline da confermare."""
    from core.drawdown_hard_stop import REASON_SCOPE_UNATTRIBUTED
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    vecchio = {"peak": "150", "seq": 4, "peak_epoch": 0, "rebase_ops": [], "breached": True,
               "breach": {"kind": "max_drawdown_abs", "loss": "20"}, "session_open": False, "baseline_verified": True,
               "baseline": {}, "history_insufficient": {}}
    c.rc.db.save_settings({STATE_KEY: json.dumps(vecchio)})
    del c.rc._drawdown_hard_stop
    _lettura_ambito(c, 140.0, simulated=False)
    stop = c.rc._hard_stop()
    assert c.rc.drawdown_entry_block_reason() in (REASON_ACTIVE, REASON_SCOPE_UNATTRIBUTED)
    assert stop.scope_block_reason() == REASON_SCOPE_UNATTRIBUTED
    assert _stato_salvato(c)["seq"] == 4 and "scope" not in _stato_salvato(c)
    esito = c.rc.owner_assign_drawdown_scope(authorization="#628 M53", reason="attribuzione al conto LIVE",
                                             operation_id="ASSIGN-1", confirm=True)
    assert esito["assigned"], esito
    salvato = _stato_salvato(c)
    assert salvato["breached"] is True and salvato["peak"] == "150"          # nessun reset
    assert salvato["baseline_verified"] is False and salvato["scope"]["env"] == "LIVE"
    assert stop.scope_block_reason() == ""
    assert c.rc.owner_assign_drawdown_scope(authorization="#628", reason="r", operation_id="ASSIGN-2",
                                            confirm=True)["assigned"] is False   # nulla da attribuire


def test_s18_owner_assignment_refused_with_uncertain_orders(tmp_path):
    """S18-1: nessun cambio operativo con ordini incerti: l'attribuzione owner richiede le stesse verifiche degli
    sblocchi (nessun ordine AMBIGUOUS)."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura_ambito(c, 1000.0, simulated=True)
    _lettura_ambito(c, 100.0, simulated=False)
    c.rc._owner_drawdown_checks = lambda: {"cash_verified": True, "cash": 100.0, "exposure": 0.0,
                                          "uncertain_orders": ["ref-incerto"]}
    esito = c.rc.owner_assign_drawdown_scope(authorization="#628", reason="r", operation_id="A1", confirm=True)
    assert esito["assigned"] is False and esito["reason"]
    assert _stato_salvato(c)["scope"] == {"env": "SIM", "account": "sim"}


def test_s18_queued_entry_never_reaches_the_broker_after_a_scope_switch(tmp_path):
    """S18-1 (precauzione owner): un ingresso approvato e messo in coda in SIM non parte dopo il passaggio a LIVE: il
    controllo d'ambito vale anche al punto finale d'invio del motore."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura_ambito(c, 100.0, simulated=True)
    trattenuti = _ingresso_in_coda(c)
    _lettura_ambito(c, 100.0, simulated=False)
    for comando in trattenuti:
        c.rc.bus.publish("CMD_QUICK_BET", comando)
    assert _ordini(c) == 0


@pytest.mark.parametrize("ambito", [True, False])
def test_s18_global_lockdown_blocks_entries_in_every_scope(tmp_path, ambito):
    """S18-1: la separazione non aggira i blocchi globali: LOCKDOWN amministrativo nega l'ingresso in ogni ambito."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura_ambito(c, 100.0, simulated=ambito)
    assert c.rc.drawdown_entry_block_reason() == ""
    c.rc.force_lockdown("owner_lockdown")
    assert c.rc.drawdown_entry_block_reason().startswith("runtime_lockdown")


@pytest.mark.parametrize("procedura", ["sblocco_superamento", "sblocco_storia", "conferma"])
def test_s19b_failed_owner_unlock_or_confirmation_keeps_every_verified_value(tmp_path, monkeypatch, procedura):
    """S19-B esteso (PF-OWNER-PR631-CYCLE19-FINAL-SCOPE-20261112): un salvataggio fallito durante sblocco o conferma owner
    non perde nulla: picco, epoca, picco confermato con il suo istante, superamento, storia insufficiente e baseline
    restano quelli di prima (fail-closed: il blocco resta)."""
    import time as _time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    c.rc._hard_stop().open_session()
    _riavvio(c)
    c.rc._reload_drawdown_state()
    c.rc._apply_funds_reading(_fondi(120.0), source="t", read_at=_time.monotonic())
    stop = c.rc._hard_stop()
    if procedura != "conferma":
        assert c.rc.owner_confirm_drawdown_baseline(**_CONFIRM, declared_peak=130.0)["confirmed"]
        if procedura == "sblocco_superamento":
            c.rc._apply_funds_reading(_fondi(115.0), source="t", read_at=_time.monotonic() + 1)
            assert stop.breached
        else:
            stop._mark_history_insufficient("test", 1.0, Decimal("110"), stop.peak_epoch)
    prima = (stop.peak, stop.peak_epoch, stop._confirmed_peak, stop.baseline_confirmed_at, stop.breached,
             dict(stop.history_insufficient), stop.baseline_verified)
    salvato = _stato_salvato(c)

    def guasto(values):
        raise OSError("disco pieno")

    monkeypatch.setattr(c.rc.db, "save_settings", guasto)
    c.rc.betfair_service.get_account_funds = lambda: _fondi(130.0)   # conto reale per la rilettura dello sblocco
    if procedura == "sblocco_superamento":
        esito = c.rc.owner_unlock_drawdown_hard_stop(authorization="#628", reason="t", operation_id="U9", confirm=True)
        assert esito["unlocked"] is False
    elif procedura == "sblocco_storia":
        esito = c.rc.owner_clear_drawdown_history_insufficient(authorization="#628", reason="t", operation_id="H9",
                                                                confirm=True)
        assert esito["cleared"] is False
    else:
        assert c.rc.owner_confirm_drawdown_baseline(**_CONFIRM, declared_peak=130.0)["confirmed"] is False
    assert (stop.peak, stop.peak_epoch, stop._confirmed_peak, stop.baseline_confirmed_at, stop.breached,
            dict(stop.history_insufficient), stop.baseline_verified) == prima
    assert _stato_salvato(c) == salvato
    assert c.rc.drawdown_entry_block_reason() != ""


def test_s19a_proof_between_real_readings_older_than_the_window(tmp_path):
    """S19-A generale (oracolo R49 di Claude D, caso 16): letture vere con istante precedente all'apertura della finestra
    (92, poi 110) consegnate prima della conferma owner a 131; 96 iniziata dopo il 110 ma consegnata dopo la conferma.
    110 -> 96 = 14 >= 10 e' dimostrato da due letture vere: hard-stop, anche se fuori dalla finestra il massimo e' noto solo
    come limite superiore (mai storia insufficiente)."""
    import time as _time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    c.rc._hard_stop().open_session()
    _riavvio(c)
    c.rc._reload_drawdown_state()                                # finestra aperta adesso
    t = _time.monotonic() - 50                                   # istanti precedenti all'apertura
    c.rc._apply_funds_reading(_fondi(92.0), source="t", read_at=t + 1)
    c.rc._apply_funds_reading(_fondi(110.0), source="t", read_at=t + 3)
    assert c.rc.owner_confirm_drawdown_baseline(**_CONFIRM, declared_peak=131.0)["confirmed"]
    c.rc._apply_funds_reading(_fondi(96.0), source="t", read_at=t + 12)
    stop = c.rc._hard_stop()
    assert stop.breached and Decimal(stop.snapshot()["breach"]["loss"]) == Decimal("14")


def test_s19a_proof_from_the_exact_window_base_for_a_late_pre_confirmation_reading(tmp_path):
    """S19-A (oracolo R49 di Claude D, caso 240 con tempi realistici): picco 100 noto all'apertura della finestra (START),
    96 consegnata prima della conferma owner a 106, poi 87 iniziata prima della conferma e consegnata dopo. 100 -> 87 = 13
    e' dimostrato dal massimo esatto della finestra: hard-stop, mai storia insufficiente."""
    import time as _time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    c.rc._hard_stop().open_session()
    _riavvio(c)
    c.rc._reload_drawdown_state()                                # finestra aperta adesso, base 100
    _time.sleep(0.01)
    t = _time.monotonic()
    c.rc._apply_funds_reading(_fondi(96.0), source="t", read_at=t + 0.001)
    _time.sleep(0.01)
    assert c.rc.owner_confirm_drawdown_baseline(**_CONFIRM, declared_peak=106.0)["confirmed"]
    c.rc._apply_funds_reading(_fondi(87.0), source="t", read_at=t + 0.005)
    stop = c.rc._hard_stop()
    assert stop.breached and Decimal(stop.snapshot()["breach"]["loss"]) == Decimal("13")


def test_s18_evaluation_uses_the_scope_of_its_own_reading_not_the_last_declared(tmp_path):
    """S18-1: l'ambito di una lettura si ridichiara nella sezione critica della sua valutazione. Se fra la dichiarazione e
    la valutazione un'altra lettura ha dichiarato un altro ambito (LIVE), la lettura SIM e' valutata comunque come SIM sul
    record SIM: il picco SIM sale, mai bloccato ne' valutato con l'ambito di un'altra lettura."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura_ambito(c, 1000.0, simulated=True)
    stop = c.rc._hard_stop()
    stop.set_scope({"env": "LIVE", "account": "betfair:altro"})  # dichiarato da una lettura LIVE concorrente
    c.rc._evaluate_drawdown_hard_stop(1200.0, None, scope={"env": "SIM", "account": "sim"})
    assert stop.peak == Decimal("1200")
    assert Decimal(_stato_salvato(c)["peak"]) == Decimal("1200")


@pytest.mark.parametrize("verso", [False, True])
def test_s18_mode_switch_invalidates_the_declared_scope_until_a_new_reading(tmp_path, verso):
    """S18-1 (Claude D in anteprima sulla patch del ciclo 19): al cambio di modalita' SIM/LIVE l'ambito dichiarato non
    vale piu'. Fino alla prima lettura del conto nella nuova modalita' il controllo finale nega gli ingressi: un ingresso
    gia' in coda non parte verso il client della nuova modalita' valutato contro lo stato dell'altra."""
    from core.drawdown_hard_stop import REASON_ACCOUNT_UNKNOWN
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura_ambito(c, 100.0, simulated=not verso)
    trattenuti = _ingresso_in_coda(c)
    c.rc.set_simulation_mode(verso)
    assert c.rc.drawdown_entry_block_reason() == REASON_ACCOUNT_UNKNOWN
    for comando in trattenuti:
        c.rc.bus.publish("CMD_QUICK_BET", comando)
    assert _ordini(c) == 0
    _lettura_ambito(c, 100.0, simulated=not verso)              # di nuovo l'ambito del record: si riprende
    assert c.rc.drawdown_entry_block_reason() == ""


def test_s18_mode_set_before_any_reading_does_not_block_the_first_entry(tmp_path):
    """S18-1: impostare la modalita' prima di qualunque lettura del conto (avvio di headless_main) non invalida nulla:
    il primo ingresso legge il conto, dichiara il proprio ambito e parte."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    c.rc.set_simulation_mode(True)
    assert c.rc._hard_stop().scope is None
    _lettura(c, 100.0)
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 1


@pytest.mark.parametrize("impegnato", [100.0, 0.0])
def test_s18_zero_available_reading_of_another_scope_never_touches_the_record(tmp_path, impegnato):
    """S18-1 (Codex su 12618e75): stato SIM salvato a 1000, nuovo processo (ambito non ancora dichiarato), prima lettura
    LIVE con disponibile 0 (impegnato 100, oppure 0 = perdita totale). Il ramo del disponibile zero dichiara l'ambito
    della lettura come gli altri: nessun superamento fittizio scritto sul record SIM, ingressi negati per ambito diverso."""
    import types
    from core.drawdown_hard_stop import REASON_SCOPE_MISMATCH
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura_ambito(c, 1000.0, simulated=True)
    salvato = _stato_salvato(c)
    del c.rc._drawdown_hard_stop                                 # nuovo processo: stato riletto, ambito non dichiarato
    c.rc.simulation_mode = False                                 # il nuovo processo e' in LIVE
    c.rc.betfair_service.client = types.SimpleNamespace(username="conto-a")
    fondi = {"available": 0.0, "exposure": -impegnato, "ok": True, "simulated": False}
    c.rc._apply_funds_reading(dict(fondi), source="test")
    assert _stato_salvato(c) == salvato
    assert not c.rc._hard_stop().breached
    assert c.rc.drawdown_entry_block_reason() == REASON_SCOPE_MISMATCH


def _record_senza_provenienza(c, **extra):
    vecchio = {"peak": "150", "seq": 4, "peak_epoch": 0, "rebase_ops": [], "breached": True,
               "breach": {"kind": "max_drawdown_abs", "loss": "20"}, "session_open": False, "baseline_verified": True,
               "baseline": {}, "history_insufficient": {}}
    vecchio.update(extra)
    c.rc.db.save_settings({STATE_KEY: json.dumps(vecchio)})
    del c.rc._drawdown_hard_stop


def test_s18_session_writes_never_lift_the_quarantine_of_a_state_without_provenance(tmp_path):
    """S18-1 (Sol e Grok su 12618e75): apertura e chiusura della sessione (esenti dal controllo d'ambito) non cancellano
    la quarantena di un record senza provenienza: dopo la loro scrittura e una rilettura lo stato resta non attribuito
    finche' l'owner non lo assegna."""
    from core.drawdown_hard_stop import REASON_SCOPE_UNATTRIBUTED
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _record_senza_provenienza(c)
    stop = c.rc._hard_stop()
    assert stop.open_session() and stop.close_session()
    del c.rc._drawdown_hard_stop                                 # nuovo processo
    _lettura_ambito(c, 140.0, simulated=False)
    assert c.rc._hard_stop().scope_block_reason() == REASON_SCOPE_UNATTRIBUTED
    assert c.rc.drawdown_entry_block_reason() != ""


@pytest.mark.parametrize("violato", [True, False])
def test_s18_session_writes_after_a_declared_scope_never_attribute_a_state_without_provenance(tmp_path, violato):
    """S18-1 (Grok su 559b39ca): con l'ambito gia' dichiarato da una lettura, apertura e chiusura della sessione non
    scrivono quell'ambito come provenienza del record senza provenienza: dopo il riavvio lo stato resta in quarantena
    (picco e superamento mai adottati dal conto corrente senza l'assegnazione owner)."""
    from core.drawdown_hard_stop import REASON_SCOPE_UNATTRIBUTED
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _record_senza_provenienza(c, breached=violato, breach={"kind": "max_drawdown_abs", "loss": "20"} if violato else {})
    _lettura_ambito(c, 140.0, simulated=False)                   # ambito dichiarato: quarantena
    stop = c.rc._hard_stop()
    assert stop.scope_block_reason() == REASON_SCOPE_UNATTRIBUTED
    assert stop.open_session() and stop.close_session()
    assert _stato_salvato(c).get("scope") is None
    del c.rc._drawdown_hard_stop                                 # nuovo processo
    _lettura_ambito(c, 140.0, simulated=False)
    assert c.rc._hard_stop().scope_block_reason() == REASON_SCOPE_UNATTRIBUTED
    assert c.rc.drawdown_entry_block_reason() != ""


@pytest.mark.parametrize("campo", [{"scope": None}])
def test_s18_state_with_null_provenance_is_quarantined_too(tmp_path, campo):
    """S18-1: un record con stato finanziario e provenienza nulla (scritto senza ambito) e' non attribuito come uno
    senza il campo: quarantena, mai adottato dal primo conto che legge."""
    from core.drawdown_hard_stop import REASON_SCOPE_UNATTRIBUTED
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _record_senza_provenienza(c, **campo)
    _lettura_ambito(c, 140.0, simulated=False)
    assert c.rc._hard_stop().scope_block_reason() == REASON_SCOPE_UNATTRIBUTED


def test_s18_mode_switch_between_check_and_declaration_never_restores_the_old_scope(tmp_path, monkeypatch):
    """S18-1 (Sol su 559b39ca): verifica della modalita' e dichiarazione dell'ambito sono una sola sezione critica con
    l'invalidazione del cambio modalita'. Una lettura SIM che ha appena superato il controllo e un passaggio a LIVE
    concorrente non lasciano dichiarato l'ambito SIM: gli ingressi restano negati fino a una lettura LIVE."""
    import threading
    from core.runtime_controller import RuntimeController
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura_ambito(c, 100.0, simulated=True)
    assert c.rc.drawdown_entry_block_reason() == ""
    originale = RuntimeController._drawdown_scope_is_current
    passaggi = []

    def controllo_poi_cambio(self, scope):
        esito = originale(self, scope)
        if not passaggi:                                         # una sola volta: il cambio arriva dopo il controllo
            t = threading.Thread(target=c.rc.set_simulation_mode, args=(False,))
            t.start()
            t.join(timeout=0.5)                                  # col lock tenuto il cambio attende la dichiarazione
            passaggi.append(t)
        return esito

    monkeypatch.setattr(RuntimeController, "_drawdown_scope_is_current", controllo_poi_cambio)
    RuntimeController._declare_drawdown_scope(c.rc, {"available": 100.0, "ok": True, "simulated": True})
    passaggi[0].join(timeout=5)
    assert not passaggi[0].is_alive()
    monkeypatch.setattr(RuntimeController, "_drawdown_scope_is_current", originale)
    assert c.rc.simulation_mode is False
    assert c.rc._hard_stop().scope != {"env": "SIM", "account": "sim"}
    assert c.rc.drawdown_entry_block_reason() != ""


def test_s18_late_reading_of_the_previous_mode_never_reenables_entries(tmp_path):
    """S18-1 (Sol su 12618e75): una lettura SIM iniziata prima del passaggio a LIVE e consegnata dopo non ridichiara
    l'ambito SIM per i gate: con il client gia' LIVE gli ingressi restano negati finche' non arriva una lettura LIVE."""
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    c.rc.simulation_mode = True
    _lettura_ambito(c, 100.0, simulated=True)
    trattenuti = _ingresso_in_coda(c)
    c.rc.set_simulation_mode(False)
    _lettura_ambito(c, 100.0, simulated=True, allinea=False)     # risposta SIM in volo, consegnata dopo il cambio
    assert c.rc.drawdown_entry_block_reason() != ""
    for comando in trattenuti:
        c.rc.bus.publish("CMD_QUICK_BET", comando)
    assert _ordini(c) == 0


def test_s18_zero_available_reading_of_the_other_mode_in_a_new_process_is_never_evaluated(tmp_path):
    """S18-1: nuovo processo in SIM (ambito non ancora dichiarato) e una lettura LIVE con disponibile 0 consegnata
    comunque: non e' della modalita' corrente, quindi non si dichiara ne' si valuta. Il ramo del disponibile zero passa
    l'ambito della lettura alla valutazione: senza, la valutazione userebbe l'ambito non dichiarato (nessun controllo) e
    scriverebbe una perdita totale fittizia sul record SIM."""
    import types
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura_ambito(c, 1000.0, simulated=True)
    salvato = _stato_salvato(c)
    del c.rc._drawdown_hard_stop                                 # nuovo processo, ancora in SIM
    c.rc.betfair_service.client = types.SimpleNamespace(username="conto-a")
    c.rc._apply_funds_reading({"available": 0.0, "exposure": 0.0, "ok": True, "simulated": False}, source="test")
    assert _stato_salvato(c) == salvato
    assert not c.rc._hard_stop().breached


# ------------------------------------------------- M65 (PF-OWNER-PR631-CYCLE20-M65-GO-20261112): sessione dell'ingresso
def _m65_sessioni(tmp_path, monkeypatch):
    """App reale in SIM: START (sessione A) con 100 EUR, un ingresso da 5 EUR ammesso e trattenuto in coda verso il
    motore (CMD_QUICK_BET non ancora consegnato), STOP BOT, START (sessione B) con 0,20 EUR: nella sessione B lo stesso
    ingresso nuovo sarebbe rifiutato dal gate fondi. Configurazione MM completa dei test; Liquidity Guard spento perche'
    il book del mercato fittizio non esiste (estraneo al caso)."""
    from tests.risk_config_fixtures import legacy_test_config
    app, rc = _start_reale(tmp_path, monkeypatch, 100.0)
    monkeypatch.setattr(rc.settings_service, "load_roserpina_config",
                        lambda: legacy_test_config(max_drawdown_abs=1000.0, liquidity_guard_enabled=False))
    assert rc.start(execution_mode="SIMULATION").get("started") is True
    trattenuti = []
    reale = rc.bus.publish

    def publish(topic, payload=None):
        if topic == "CMD_QUICK_BET":
            trattenuti.append(payload)
            return None
        return reale(topic, payload)

    rc.bus.publish = publish
    rc._on_signal_received(_signal(stake=5.0, bet_type="BACK"))   # ordine normale (sopra il minimo exchange)
    rc.bus.publish = reale
    assert len(trattenuti) == 1
    return app, rc, trattenuti[0]


def _m65_ordini_broker(app):
    import time
    broker = app.betfair_service.simulation_broker
    fine = time.monotonic() + 1.0                                  # l'esecutore SIM e' asincrono
    while time.monotonic() < fine and not broker.state.orders:
        time.sleep(0.02)
    return len(broker.state.orders)


def _m65_chiudi(app):
    from core import blackbox_runtime
    try:
        app.stop()
    except Exception:
        pass
    blackbox_runtime.shutdown()


@pytest.mark.real_blackbox
def test_m65_entry_admitted_in_a_previous_session_never_reaches_the_broker_after_stop_start(tmp_path, monkeypatch):
    """M65: un ingresso ammesso nella sessione A e rimasto in coda non arriva al broker dopo STOP -> START: i controlli
    finanziari della sessione A (fondi, esposizione aggregata) non valgono per la sessione B. Rifiuto prima di
    qualunque persistenza dell'intento o invio, mai ripristinato automaticamente."""
    app, rc, vecchio = _m65_sessioni(tmp_path, monkeypatch)
    try:
        rc.stop()                                              # STOP BOT: l'autorizzazione della sessione A decade
        rc.betfair_service.get_account_funds = lambda: _fondi(0.2)
        assert rc.start(execution_mode="SIMULATION").get("started") is True    # sessione B
        esito = app.trading_engine.submit_quick_bet(vecchio)   # consegna tardiva del vecchio ingresso
        assert esito.get("ok") is False and esito.get("status") == "DENIED", esito.get("status")
        assert _m65_ordini_broker(app) == 0
    finally:
        _m65_chiudi(app)


@pytest.mark.real_blackbox
def test_m65_new_session_cannot_start_while_an_entry_of_the_old_session_is_being_sent(tmp_path, monkeypatch):
    """M65, concorrenza STOP -> START -> invio: un ingresso della sessione A che ha gia' superato il controllo finale ed
    e' in invio (dentro l'operazione finanziaria in volo) blocca la nascita della sessione B: lo STOP lo attende e un
    nuovo START non acquisisce la lease finche' non finisce. Non esiste quindi un istante in cui un ingresso della
    sessione A venga inviato sotto la sessione B (nessun TOCTOU fra controllo e invio)."""
    import threading
    from core.runtime_controller import RuntimeController
    monkeypatch.setattr(RuntimeController, "RUNTIME_AUTHORITY_DRAIN_SEC", 0.3)
    app, rc, vecchio = _m65_sessioni(tmp_path, monkeypatch)
    in_invio, prosegui = threading.Event(), threading.Event()
    verdetto = RuntimeController.admission_fence_verdict

    def fermo_dopo_il_controllo(self, request):
        esito = verdetto(self, request)
        if (request or {}).get("customer_ref") == vecchio["customer_ref"]:
            in_invio.set()
            assert prosegui.wait(10)
        return esito

    monkeypatch.setattr(RuntimeController, "admission_fence_verdict", fermo_dopo_il_controllo)
    esiti = []
    invio = threading.Thread(target=lambda: esiti.append(app.trading_engine.submit_quick_bet(vecchio)))
    try:
        invio.start()
        assert in_invio.wait(10)                               # sessione A, controllo superato, invio in volo
        rc.stop()
        rc.betfair_service.get_account_funds = lambda: _fondi(0.2)
        esito_start = rc.start(execution_mode="SIMULATION")
        assert esito_start.get("started") is not True                 # nessuna sessione B con l'invio in volo
        assert esito_start.get("reason") == "runtime_authority_draining", esito_start.get("reason")
        assert rc.drawdown_entry_block_reason() != "" or rc.mode.name != "ACTIVE"
        prosegui.set()
        invio.join(10)
        assert not invio.is_alive()
    finally:
        prosegui.set()
        _m65_chiudi(app)


@pytest.mark.real_blackbox
def test_m65_entry_admitted_in_the_current_session_still_reaches_the_broker(tmp_path, monkeypatch):
    """M65, non regressione: un ingresso ammesso e consegnato nella stessa sessione parte come prima."""
    app, rc, corrente = _m65_sessioni(tmp_path, monkeypatch)
    try:
        esito = app.trading_engine.submit_quick_bet(corrente)
        assert esito.get("ok") is True, esito.get("status")
        assert _m65_ordini_broker(app) == 1
    finally:
        _m65_chiudi(app)


# ------------------------------------- M68 (PF-OWNER-PR631-C20-FINAL-SAFETY-GATES-20261112): stato del runtime all'invio
@pytest.mark.parametrize("stato", ["PAUSED", "STOPPED", "LOCKDOWN"])
def test_m68_queued_entry_never_reaches_the_broker_when_the_bot_is_not_active(tmp_path, stato):
    """M68: un ingresso approvato e messo in coda quando il bot era ACTIVE non arriva al broker se, alla consegna, il
    bot e' in PAUSA, fermo o in LOCKDOWN: il controllo finale d'invio del motore rifiuta ogni stato diverso da ACTIVE,
    anche con l'autorizzazione tecnica ancora valida (harness senza autorita' da verificare)."""
    from core.system_state import RuntimeMode
    c = _catena(tmp_path)
    _lettura(c, 100.0)
    trattenuti = _ingresso_in_coda(c)
    if stato == "PAUSED":
        c.rc.pause()
    else:
        c.rc.mode = getattr(RuntimeMode, stato)
    motivo = c.rc.drawdown_entry_block_reason()
    atteso = "runtime_lockdown:" if stato == "LOCKDOWN" else f"runtime_non_attivo:{stato}"
    assert motivo.startswith(atteso), motivo                     # motivo distinto: LOCKDOWN non e' una PAUSA
    for comando in trattenuti:
        c.rc.bus.publish("CMD_QUICK_BET", comando)
    assert _ordini(c) == 0


def test_m68_protective_paths_stay_open_while_paused(tmp_path):
    """M68: il blocco dei nuovi ingressi in PAUSA non tocca le operazioni protettive: il cashout (OrderRouter, con il
    proprio controllo d'autorita') resta ammesso e la PAUSA non e' un LOCKDOWN."""
    c = _catena(tmp_path)
    _lettura(c, 100.0)
    c.rc.pause()
    assert c.rc.drawdown_entry_block_reason() != ""
    assert c.rc.cashout_authority_check() == ""


def test_m65_session_change_between_the_fence_check_and_the_send_point_blocks_the_order(tmp_path, monkeypatch):
    """M65: la sessione d'ammissione annotata dal controllo del fence arriva al punto d'invio e vi si ricontrolla. Una
    sessione nuova aperta fra i due (es. invio eseguito in ritardo dall'esecutore dopo STOP -> START) nega l'ordine
    prima del broker, con motivo certo."""
    from core.runtime_controller import RuntimeController
    from core.trading_engine import TradingEngine
    c = _catena(tmp_path)
    _lettura(c, 100.0)
    trattenuti = _ingresso_in_coda(c)
    reale = TradingEngine._submit_to_order_path
    annotate = []

    def nuova_sessione_prima_dell_invio(self, ctx, request):
        # fuori dal percorso d'invio: un'asserzione qui verrebbe assorbita dal motore come errore d'invio
        annotate.append((request.get("admission_session"), RuntimeController._entry_session(c.rc)))
        RuntimeController._new_entry_session(c.rc)
        return reale(self, ctx, request)

    monkeypatch.setattr(TradingEngine, "_submit_to_order_path", nuova_sessione_prima_dell_invio)
    for comando in trattenuti:
        c.rc.bus.publish("CMD_QUICK_BET", comando)
    assert len(annotate) == 1 and annotate[0][0] == annotate[0][1], annotate   # sessione annotata dal fence
    assert _ordini(c) == 0


def test_f2_sim_reading_racing_the_switch_to_live_never_leaves_the_sim_scope_declared(tmp_path):
    """F2 (M68, Claude D su e294bd6d): cambio del flag di modalita' e invalidazione dell'ambito sono una sola sezione
    critica con la dichiarazione dell'ambito. Una lettura SIM che arriva esattamente mentre il flag passa a LIVE non
    lascia dichiarato l'ambito SIM: il controllo finale nega gli ingressi LIVE finche' non arriva una lettura LIVE.
    Deterministico: il setter del flag avvia la lettura SIM concorrente e la attende (fino a 0,5 s); se il flag fosse
    assegnato fuori dal lock, dopo l'invalidazione, la lettura vedrebbe ancora SIM e dichiarerebbe l'ambito SIM."""
    import threading
    from core.runtime_controller import RuntimeController
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura_ambito(c, 100.0, simulated=True)
    assert c.rc.drawdown_entry_block_reason() == ""
    letture = []

    class _FlagSpia(type(c.rc)):
        @property
        def simulation_mode(self):
            return self.__dict__.get("_sm_valore", False)

        @simulation_mode.setter
        def simulation_mode(self, valore):
            if valore is False and self.__dict__.get("_sm_valore") is True and not letture:
                lettura = threading.Thread(target=RuntimeController._declare_drawdown_scope,
                                           args=(self, {"available": 100.0, "total": 100.0, "ok": True,
                                                        "simulated": True}))
                letture.append(lettura)
                lettura.start()
                lettura.join(timeout=0.5)                      # col lock tenuto la lettura attende il cambio
            self.__dict__["_sm_valore"] = valore

    c.rc.__dict__["_sm_valore"] = c.rc.__dict__.pop("simulation_mode", True)
    c.rc.__class__ = _FlagSpia
    c.rc.set_simulation_mode(False)
    letture[0].join(timeout=5)
    assert not letture[0].is_alive()
    assert c.rc._hard_stop().scope != {"env": "SIM", "account": "sim"}
    assert c.rc.drawdown_entry_block_reason() != ""


def test_f2_final_check_never_sees_the_live_flag_with_the_sim_scope_still_valid(tmp_path):
    """F2 (M68, sonda D test_d_f2_race): il controllo finale d'ingresso non vede mai, nella propria sezione critica, il
    flag gia' LIVE (client verso LIVE) insieme all'ambito SIM ancora valido ('' = ingresso ammesso sullo stato SIM).
    Con il controllo in corso (lock tenuto) un passaggio a LIVE concorrente attende: flag e invalidazione cambiano
    insieme, dentro il lock. Uccide la variante con l'assegnazione del flag PRIMA del lock."""
    import threading
    import time
    from core.drawdown_hard_stop import REASON_ACCOUNT_UNKNOWN
    from core.runtime_controller import RuntimeController
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    c.rc.set_simulation_mode(True)
    _lettura_ambito(c, 100.0, simulated=True)
    assert c.rc.drawdown_entry_block_reason() == ""
    with RuntimeController._drawdown_eval_lock:                    # controllo finale in corso
        cambio = threading.Thread(target=lambda: c.rc.set_simulation_mode(False), daemon=True)
        cambio.start()
        time.sleep(0.2)
        client_live = not c.rc.simulation_mode
        ingresso = c.rc.drawdown_entry_block_reason()              # RLock: stesso thread
        assert not (client_live and ingresso == ""), (client_live, ingresso)
    cambio.join(5)
    assert not cambio.is_alive()
    assert c.rc.drawdown_entry_block_reason() == REASON_ACCOUNT_UNKNOWN


def test_m68_queued_entry_never_reaches_the_broker_when_the_desk_is_in_lockdown(tmp_path):
    """M68 (Sol su 4c627189): il LOCKDOWN del desk (money management: drawdown dal picco oltre `lockdown_drawdown_pct`)
    nega anche gli ingressi gia' in coda al punto d'invio, senza hard-stop configurati e con il runtime ancora ACTIVE:
    picco 100, lettura fresca 75, soglia 20% => nessun ordine."""
    from core.system_state import DeskMode
    c = _catena(tmp_path, lockdown_drawdown_pct=20.0)
    _lettura(c, 100.0)
    trattenuti = _ingresso_in_coda(c)
    _lettura(c, 75.0)
    assert c.rc._desk_mode() == DeskMode.LOCKDOWN
    assert c.rc.drawdown_entry_block_reason() == "desk_lockdown"
    for comando in trattenuti:
        c.rc.bus.publish("CMD_QUICK_BET", comando)
    assert _ordini(c) == 0


def test_m68_unreadable_desk_state_blocks_the_queued_entry(tmp_path, monkeypatch):
    """M68: stato del desk non leggibile al punto d'invio => ingresso negato (fail-closed), mai ammesso."""
    from core.runtime_controller import RuntimeController
    c = _catena(tmp_path)
    _lettura(c, 100.0)
    trattenuti = _ingresso_in_coda(c)

    def illeggibile(self):
        raise RuntimeError("desk non leggibile")

    monkeypatch.setattr(RuntimeController, "_desk_mode", illeggibile)
    assert c.rc.drawdown_entry_block_reason() == "desk_non_verificabile"
    for comando in trattenuti:
        c.rc.bus.publish("CMD_QUICK_BET", comando)
    assert _ordini(c) == 0


# ---------------------------------- ciclo 21 (PF-OWNER-PR631-C21-EQUAL-TIMESTAMP-GO-20261112): letture con lo stesso istante
def _letture_stesso_istante(tmp_path, valori, **cfg):
    import time
    c = _catena(tmp_path, **(cfg or {"max_drawdown_abs": 10.0}))
    _lettura(c, 100.0)
    istante = time.monotonic() + 100.0
    for valore in valori:
        c.rc._apply_funds_reading(_fondi(valore), source="test", read_at=istante)
    return c


@pytest.mark.parametrize("ordine", [(130.0, 115.0), (115.0, 130.0)])
def test_c21_equal_start_readings_with_a_possible_drop_are_history_insufficient_in_both_orders(tmp_path, ordine):
    """Ciclo 21 (Sol su d771e1f8, Claude D): picco 100, limite 10, letture 130 e 115 iniziate nello STESSO istante.
    L'ordine reale e' ignoto: un calo di 15 e' possibile ma non dimostrabile => storia insufficiente in ENTRAMBI gli
    ordini di arrivo. Mai un hard-stop inventato (130 -> 115) ne' un PASS (115 -> 130); un ingresso in coda non parte."""
    c = _letture_stesso_istante(tmp_path, ordine)
    stop = c.rc._hard_stop()
    assert not stop.breached
    assert stop.history_insufficient.get("active") is True
    assert stop.history_insufficient.get("reason") == "letture_con_lo_stesso_istante"
    assert c.rc.drawdown_entry_block_reason() == REASON_HISTORY_INSUFFICIENT


@pytest.mark.parametrize("ordine", [(130.0, 85.0), (85.0, 130.0)])
def test_c21_a_drop_proven_by_the_history_still_triggers_the_hard_stop(tmp_path, ordine):
    """Ciclo 21: una perdita gia' dimostrata dalla cronologia resta un hard-stop anche con letture dello stesso istante:
    85 contro il picco 100 letto PRIMA (calo di 15 >= 10), in qualunque ordine di arrivo."""
    c = _letture_stesso_istante(tmp_path, ordine)
    assert c.rc._hard_stop().breached
    assert c.rc.drawdown_entry_block_reason() != ""


@pytest.mark.parametrize("ordine", [(130.0, 130.0), (105.0, 100.0), (100.0, 105.0)])
def test_c21_equal_start_readings_without_a_possible_breach_do_not_block(tmp_path, ordine):
    """Ciclo 21: letture dello stesso istante senza un calo possibile pari al limite (uguali, o 5 < 10) non bloccano."""
    c = _letture_stesso_istante(tmp_path, ordine)
    stop = c.rc._hard_stop()
    assert not stop.breached and not stop.history_insufficient.get("active")
    assert c.rc.drawdown_entry_block_reason() == ""


@pytest.mark.parametrize("ordine", [(130.0, 115.0), (115.0, 130.0)])
def test_c21_equal_start_readings_with_the_percentage_cap(tmp_path, ordine):
    """Ciclo 21: lo stesso vale per il limite percentuale (10% di 130 = 13 <= 15) => storia insufficiente."""
    c = _letture_stesso_istante(tmp_path, ordine, max_drawdown_hard_stop_pct=10.0)
    stop = c.rc._hard_stop()
    assert not stop.breached and stop.history_insufficient.get("active") is True


def test_c21_equal_start_peer_already_moved_to_the_summary_is_still_compared(tmp_path):
    """Ciclo 21 (Codex su 954b226b): la lettura gemella dello stesso istante gia' passata dalle recenti al riepilogo
    (oltre il limite delle letture recenti) e' ancora una gemella: picco 100, 130 a t2, molte letture successive a 130,
    poi 115 a t2 => calo possibile ma non dimostrabile => storia insufficiente, mai un PASS."""
    import time
    from core.drawdown_hard_stop import RECENT_READINGS_LIMIT
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    t2 = time.monotonic() + 100.0
    c.rc._apply_funds_reading(_fondi(130.0), source="test", read_at=t2)
    for i in range(1, RECENT_READINGS_LIMIT + 2):
        c.rc._apply_funds_reading(_fondi(130.0), source="test", read_at=t2 + i)
    c.rc._apply_funds_reading(_fondi(115.0), source="test", read_at=t2)
    stop = c.rc._hard_stop()
    assert not stop.breached
    assert stop.history_insufficient.get("active") is True
    assert c.rc.drawdown_entry_block_reason() == REASON_HISTORY_INSUFFICIENT


def test_c21_equal_start_lower_peer_in_the_summary_and_a_late_higher_reading(tmp_path):
    """Ciclo 21, caso speculare: 115 a t2 passata al riepilogo, poi 130 a t2 consegnata in ritardo (dopo molte letture
    a 125: calo dimostrabile 130 -> 125 = 5 sotto il limite): se 130 era prima di 115 il calo di 15 e' possibile => storia insufficiente, mai un PASS."""
    import time
    from core.drawdown_hard_stop import RECENT_READINGS_LIMIT
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    t2 = time.monotonic() + 100.0
    c.rc._apply_funds_reading(_fondi(115.0), source="test", read_at=t2)
    for i in range(1, RECENT_READINGS_LIMIT + 2):
        c.rc._apply_funds_reading(_fondi(125.0), source="test", read_at=t2 + i)
    c.rc._apply_funds_reading(_fondi(130.0), source="test", read_at=t2)
    stop = c.rc._hard_stop()
    assert not stop.breached
    assert stop.history_insufficient.get("active") is True


def test_c21_owner_clearance_after_equal_start_readings_never_judges_the_stale_gate_cash(tmp_path):
    """Ciclo 21 (Codex su ccecc8db): 115 poi 130 dello stesso istante => storia insufficiente; la 130, tardiva, non
    aggiornava l'ultima cassa del gate (restava 115) mentre picco e cassa del desk erano 130. Dopo lo sblocco owner
    (verifiche con la cassa verificata 130) il segnale successivo confrontava la cassa vecchia 115 col picco 130 e
    memorizzava una perdita fittizia di 15. Lo sblocco rilegge il conto (prova fresca, lettura ordinata): nessun
    hard-stop inventato, il segnale passa."""
    import time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    istante = time.monotonic()                                    # t2 nel passato (realistico)
    for valore in (115.0, 130.0):
        c.rc._apply_funds_reading(_fondi(valore), source="test", read_at=istante)
    assert c.rc._hard_stop().history_block_reason() == REASON_HISTORY_INSUFFICIENT
    c.rc.betfair_service.get_account_funds = lambda: _fondi(130.0)   # conto reale: lo sblocco lo rilegge
    esito = c.rc.owner_clear_drawdown_history_insufficient(authorization="#631 decisione owner",
                                                           reason="riconciliazione verificata",
                                                           operation_id="PF-C21-CLR", confirm=True)
    assert esito["cleared"] is True, esito
    assert esito["checks"]["fresh_cash"] == 130.0
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert not c.rc._hard_stop().breached, c.rc._hard_stop().snapshot()["breach"]
    assert _ordini(c) == 1


def test_c21_owner_clearance_keeps_a_drop_proven_by_the_fresh_entry_reading(tmp_path):
    """Ciclo 21 (caso di controllo di Claude D): 130 poi 115 dello stesso istante, conto reale a 115. Dopo lo sblocco
    owner la lettura fresca del segnale (115, successiva a t2) dimostra il calo 130 -> 115 = 15: hard-stop, nessun
    ordine. L'allineamento della cassa del gate allo sblocco non lo trasforma mai in un PASS."""
    import time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    istante = time.monotonic() + 100.0
    for valore in (130.0, 115.0):
        c.rc._apply_funds_reading(_fondi(valore), source="test", read_at=istante)
    assert c.rc._hard_stop().history_block_reason() == REASON_HISTORY_INSUFFICIENT
    esito = c.rc.owner_clear_drawdown_history_insufficient(authorization="#631 decisione owner",
                                                           reason="riconciliazione verificata",
                                                           operation_id="PF-C21-CTRL", confirm=True)
    assert esito["cleared"] is True, esito
    c.rc.betfair_service.get_account_funds = lambda: _fondi(115.0)
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert c.rc._hard_stop().breached
    assert _ordini(c) == 0


@pytest.mark.parametrize("ordine,conto", [((130.0, 115.0), 130.0), ((130.0, 115.0), 121.0), ((115.0, 130.0), 130.0)])
def test_c21_owner_clearance_in_both_arrival_orders_never_invents_a_loss(tmp_path, ordine, conto):
    """Ciclo 21 (Claude D su abbb6b3b, PF-OWNER-PR631-C21-CONTINUATION-20261112): letture 130 e 115 dello stesso
    istante t2 GIA' PASSATO, storia insufficiente, sblocco owner, conto reale `conto` (nessun calo dimostrato rispetto
    a 130 oltre il limite 10). In NESSUN ordine di arrivo lo sblocco riparte da una cassa ambigua: il segnale successivo
    rilegge il conto e passa, nessun hard-stop inventato."""
    import time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    istante = time.monotonic()                                    # t2 nel passato rispetto a sblocco e segnale
    for valore in ordine:
        c.rc._apply_funds_reading(_fondi(valore), source="test", read_at=istante)
    assert c.rc._hard_stop().history_block_reason() == REASON_HISTORY_INSUFFICIENT
    c.rc.betfair_service.get_account_funds = lambda: _fondi(conto)
    esito = c.rc.owner_clear_drawdown_history_insufficient(authorization="#631 decisione owner",
                                                           reason="riconciliazione verificata",
                                                           operation_id="PF-C21-ORD", confirm=True)
    assert esito["cleared"] is True, esito
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert not c.rc._hard_stop().breached, c.rc._hard_stop().snapshot()["breach"]
    assert _ordini(c) == 1


def _c21_insufficiente_ambigua(tmp_path, ordine=(130.0, 115.0)):
    import time
    c = _catena(tmp_path, max_drawdown_abs=10.0)
    _lettura(c, 100.0)
    istante = time.monotonic()
    for valore in ordine:
        c.rc._apply_funds_reading(_fondi(valore), source="test", read_at=istante)
    assert c.rc._hard_stop().history_block_reason() == REASON_HISTORY_INSUFFICIENT
    return c


def test_c21_owner_clearance_without_a_verifiable_fresh_reading_is_refused(tmp_path):
    """Ciclo 21: lo sblocco della storia insufficiente richiede una rilettura fresca verificata del conto. Conto non
    leggibile => nessuno sblocco (fail-closed), la storia insufficiente resta e gli ingressi restano negati."""
    c = _c21_insufficiente_ambigua(tmp_path)

    def giu():
        raise RuntimeError("conto non raggiungibile")

    c.rc.betfair_service.get_account_funds = giu
    esito = c.rc.owner_clear_drawdown_history_insufficient(authorization="#631", reason="r", operation_id="PF-C21-NF",
                                                           confirm=True)
    assert esito["cleared"] is False and esito["reason"] == "cassa_fresca_non_verificata"
    assert c.rc._hard_stop().history_block_reason() == REASON_HISTORY_INSUFFICIENT
    assert c.rc.drawdown_entry_block_reason() != ""


@pytest.mark.parametrize("ordine", [(130.0, 115.0), (115.0, 130.0)])
def test_c21_owner_clearance_then_crash_and_restart_never_invents_a_loss(tmp_path, ordine):
    """Ciclo 21 (Claude D su abbb6b3b): sblocco owner con conto reale 130, crash e riavvio subito dopo, segnale: nessun
    hard-stop inventato dalle letture ambigue di prima, in entrambi gli ordini."""
    c = _c21_insufficiente_ambigua(tmp_path, ordine)
    c.rc.betfair_service.get_account_funds = lambda: _fondi(130.0)
    assert c.rc.owner_clear_drawdown_history_insufficient(authorization="#631", reason="r", operation_id="PF-C21-RS",
                                                          confirm=True)["cleared"] is True
    _riavvio(c)
    c.rc._reload_drawdown_state()
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert not c.rc._hard_stop().breached, c.rc._hard_stop().snapshot()["breach"]
    assert _ordini(c) == 1
