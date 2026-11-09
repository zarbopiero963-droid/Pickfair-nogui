"""D-1 (P0) e D-3 — hard-stop del drawdown assoluto su cassa verificata con picco persistente; cap esatti in Decimal.

Decisione owner PF-ROSERPINA-D1-D2-B2-B3-FINANCIAL-SAFETY-20261109 (#586 c.6747859283). Difetti riprodotti dalla
campagna Grok a 5 tavoli (#586 c.6747305390, run DD5) e dall'audit su main 6519794b (#586 c.6747780896): ogni test
qui sotto FALLISCE sul main precedente (RED) e passa con la correzione.
Catena reale: RuntimeController + MoneyManagement + SimulationBroker (stub offline), nessun ordine reale.
"""
from __future__ import annotations

import json

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


@pytest.mark.parametrize("cash", [None, 0, -1, float("nan"), float("inf"), True, "x"])
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
