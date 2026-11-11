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
    assert start_src.index("_applica_fondi_avvio") < start_src.index("_open_drawdown_session()")
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


def test_entry_reading_never_raises_or_writes_the_peak():
    """La lettura d'ingresso controlla soltanto il superamento: senza superamento non alza il picco e non scrive (un
    processo senza autorita' o un DB non scrivibile non vengono toccati)."""
    db = _Db()
    stop = DrawdownHardStop(db)
    stop.observe(100.0)
    prima = db.settings[STATE_KEY]
    db.fail_save = True
    assert stop.evaluate(10.0, 130.0, observe=False) is False
    assert stop.snapshot()["peak"] == 100.0 and not stop.persist_failed and db.settings[STATE_KEY] == prima
    db.fail_save = False
    assert stop.evaluate(10.0, 85.0, observe=False) is True       # superamento: memorizzato e salvato
    assert json.loads(db.settings[STATE_KEY])["breached"] is True
