"""#461 PR28-b (PKG-P24-B): F4 durable daily loss and P37 session-loss stop.

Sources reconciled in Phase 0:
- #426 P37 (01/10, CONFERMATA): stop at the session-loss limit (owner value
  EUR 10).
- #426 06/10 decision: there is no fixed EUR 10/10 amount for the cashout;
  owner money limits stay separate risk limits.
So `max_session_loss` is an owner-configurable risk limit (no invented
default, fail-closed when unreadable). It blocks new entries and never
parametrises cashout or exits.

Phase 0 on main 54e9d17, reproduced with real classes:
- F4: the day's loss lived only in memory (risk_desk.realized_pnl restarts at
  0). EUR 8 lost, restart, another EUR 8 = 16 against max_daily_loss 10 with no
  stop.
- `reset_cycle()` and the drawdown auto-reset zero `realized_pnl` and with it
  the day's loss (8 -> 0).
- P37: no session-loss limit existed (field, loader, GUI, enforcement).
"""
from __future__ import annotations

import json

import pytest

from core.system_state import RuntimeMode
from core import validators
from database import Database
from services.setting_service import SettingsService
from tests.acceptance.test_issue437_pr27 import _catena, _rifiuti, _tavoli_occupati
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal
from tests.integration.test_runtime_controller_cycle_executor import _canonical_close_payload

pytestmark = [pytest.mark.integration, pytest.mark.safety]

_N = iter(range(10_000))


def _perdi(rc, importo):
    """Real settlement path (`_on_close_position`) with a net loss."""
    n = next(_N)
    rc._on_close_position(_canonical_close_payload(
        event_key=f"evt-loss-{n}", batch_id=f"b-loss-{n}", correlation_id=f"c-loss-{n}",
        gross_pnl=-importo, commission_amount=0.0, net_pnl=-importo, pnl=-importo,
        commission_pct=4.5,
    ))


def _perdita_giorno(rc):
    return rc._monitor_daily_loss_breach(source="TEST")


# ==========================================================================
# 1. F4: the day's loss survives restart and cycle reset
# ==========================================================================
def test_block_f4_perdita_del_giorno_sopravvive_al_riavvio(tmp_path):
    c = _catena(tmp_path, max_daily_loss=10.0)
    _perdi(c.rc, 8.0)
    assert _perdita_giorno(c.rc)["daily_loss_amount"] == pytest.approx(8.0)

    riavvio = _catena(tmp_path, max_daily_loss=10.0)        # same db file

    assert _perdita_giorno(riavvio.rc)["daily_loss_amount"] == pytest.approx(8.0)
    _perdi(riavvio.rc, 3.0)
    assert _perdita_giorno(riavvio.rc)["breached"] is True
    assert riavvio.rc.is_emergency_stopped, "11 > 10 after restart must stop"


def test_block_f4_breach_resta_dopo_reset_emergenza_e_riavvio(tmp_path):
    c = _catena(tmp_path, max_daily_loss=10.0)
    _perdi(c.rc, 12.0)
    assert c.rc.is_emergency_stopped
    c.rc.reset_emergency()

    riavvio = _catena(tmp_path, max_daily_loss=10.0)

    stato = _perdita_giorno(riavvio.rc)
    assert stato["breached"] is True
    assert stato["daily_loss_amount"] == pytest.approx(12.0)


def test_pass_f4_nuovo_giorno_riparte_da_zero(tmp_path):
    c = _catena(tmp_path, max_daily_loss=10.0)
    c.db.save_settings({"daily_loss_state": json.dumps({
        "day_utc": "2000-01-01", "intraday_realized_pnl": -50.0,
        "breached": True, "breached_at": "2000-01-01T10:00:00",
    })})

    riavvio = _catena(tmp_path, max_daily_loss=10.0)

    stato = _perdita_giorno(riavvio.rc)
    assert stato["daily_loss_amount"] == 0.0
    assert stato["breached"] is False


@pytest.mark.parametrize("grezzo", [
    "{rotto",
    json.dumps({"day_utc": "oggi"}),
    json.dumps({"day_utc": "x", "intraday_realized_pnl": "nan"}),
    json.dumps({"day_utc": "x", "intraday_realized_pnl": True}),
])
def test_block_f4_stato_persistito_illeggibile_fail_closed(tmp_path, grezzo):
    c = _catena(tmp_path, max_daily_loss=10.0)
    c.db.save_settings({"daily_loss_state": grezzo})

    riavvio = _catena(tmp_path, max_daily_loss=10.0)

    stato = _perdita_giorno(riavvio.rc)
    assert stato["breached"] is True
    assert riavvio.rc._daily_loss_monitor_state["breached"] is True


def test_block_f4_reset_ciclo_non_azzera_la_perdita_del_giorno(tmp_path):
    c = _catena(tmp_path, max_daily_loss=10.0)
    _perdi(c.rc, 8.0)

    c.rc.reset_cycle()
    assert c.rc.risk_desk.realized_pnl == 0.0      # the desk cycle does reset

    assert _perdita_giorno(c.rc)["daily_loss_amount"] == pytest.approx(8.0)
    _perdi(c.rc, 3.0)
    assert c.rc.is_emergency_stopped


def test_block_f4_auto_reset_da_drawdown_non_azzera_la_perdita(tmp_path):
    c = _catena(tmp_path, max_daily_loss=10.0)
    c.rc.config.auto_reset_drawdown_pct = 0.0       # every settlement auto-resets

    _perdi(c.rc, 8.0)

    assert c.rc.risk_desk.realized_pnl == 0.0
    assert _perdita_giorno(c.rc)["daily_loss_amount"] == pytest.approx(8.0)


# ==========================================================================
# 2. P37: session-loss stop (owner-configured limit)
# ==========================================================================
def _sessione(tmp_path, **cfg):
    c = _catena(tmp_path, **cfg)
    c.rc._session_pnl_baseline = float(c.rc.risk_desk.realized_pnl)   # as start()
    return c


def test_block_p37_perdita_sessione_raggiunta_blocca_nuove_entrate(tmp_path):
    c = _sessione(tmp_path, max_session_loss=10.0)
    _perdi(c.rc, 6.0)
    _perdi(c.rc, 4.0)
    tavoli_prima = _tavoli_occupati(c)          # the losing table is in recovery

    c.rc._on_signal_received(_signal())

    assert c.broker.state.orders == {}
    assert c.bus.payloads("CMD_QUICK_BET") == []
    assert _rifiuti(c)[-1].startswith("session_loss_breached")
    assert _tavoli_occupati(c) == tavoli_prima
    assert c.rc.duplication_guard._active == {}
    assert len(c.bus.payloads("SESSION_LOSS_BREACH_TRIGGERED")) == 1


def test_pass_p37_sotto_il_limite_resta_operativo(tmp_path):
    c = _sessione(tmp_path, max_session_loss=10.0)
    _perdi(c.rc, 9.99)

    c.rc._on_signal_received(_signal())

    assert len(c.broker.state.orders) == 1, _rifiuti(c)
    assert c.bus.payloads("SESSION_LOSS_BREACH_TRIGGERED") == []


def test_block_p37_soglia_esatta_con_somma_float(tmp_path):
    c = _sessione(tmp_path, max_session_loss=0.3)
    _perdi(c.rc, 0.1)
    _perdi(c.rc, 0.2)                     # 0.30000000000000004 or 0.29999...

    c.rc._on_signal_received(_signal())

    assert c.broker.state.orders == {}
    assert _rifiuti(c)[-1].startswith("session_loss_breached")


@pytest.mark.parametrize("valore", [float("nan"), float("inf"), True, "abc", 0.0, -5.0])
def test_block_p37_limite_illeggibile_fail_closed(tmp_path, valore):
    c = _sessione(tmp_path, max_session_loss=valore)

    c.rc._on_signal_received(_signal())

    assert c.broker.state.orders == {}
    assert _rifiuti(c) == ["max_session_loss_non_valido"]


def test_pass_p37_non_impostato_nessun_limite(tmp_path):
    c = _sessione(tmp_path)
    _perdi(c.rc, 500.0)

    c.rc._on_signal_received(_signal())

    assert len(c.broker.state.orders) == 1, _rifiuti(c)


def test_block_p37_auto_next_e_resume_bloccati(tmp_path):
    c = _sessione(tmp_path, max_session_loss=10.0)
    _perdi(c.rc, 10.0)

    consentito, motivo = c.rc._risk_allows_auto_trade()
    assert consentito is False and motivo.startswith("session_loss_breached")

    c.rc.mode = RuntimeMode.PAUSED
    esito = c.rc.resume()
    assert esito["resumed"] is False
    assert esito["reason"].startswith("session_loss_breached")
    assert c.rc.mode == RuntimeMode.PAUSED


def test_pass_p37_cashout_resta_consentito(tmp_path):
    """The stop blocks new entries; exits/cashout keep their own gates."""
    c = _sessione(tmp_path, max_session_loss=10.0)
    _perdi(c.rc, 10.0)

    aperto, motivo = c.rc._cashout_gates_still_open({"signal_type": "CASHOUT"})

    assert aperto is True, motivo


def test_block_p37_reset_ciclo_non_azzera_la_sessione(tmp_path):
    c = _sessione(tmp_path, max_session_loss=10.0)
    _perdi(c.rc, 10.0)

    c.rc.reset_cycle()
    c.rc._on_signal_received(_signal())

    assert c.broker.state.orders == {}
    assert _rifiuti(c)[-1].startswith("session_loss_breached")


def test_pass_p37_start_apre_una_nuova_sessione(tmp_path):
    c = _sessione(tmp_path, max_session_loss=10.0)
    _perdi(c.rc, 10.0)
    assert c.rc._session_loss_block_reason().startswith("session_loss_breached")

    c.rc.betfair_service.connect = lambda **_k: {"session": "sim"}
    c.rc.betfair_service.get_account_funds = lambda: {"available": 1000.0}
    c.rc.start(execution_mode="SIMULATION")

    assert c.rc._session_pnl_baseline == pytest.approx(c.rc.risk_desk.realized_pnl)
    assert c.rc._session_loss_block_reason() == ""


def test_pass_p37_limite_salvato_e_ricaricato(tmp_path):
    svc = SettingsService(Database(str(tmp_path / "settings.db")))
    cfg = svc.load_roserpina_config()
    assert cfg.max_session_loss is None                  # no invented default
    cfg.max_session_loss = 10.0

    svc.save_roserpina_config(cfg)

    assert svc.load_roserpina_config().max_session_loss == 10.0


def test_block_p37_limite_corrotto_nel_db_arriva_come_non_valido(tmp_path):
    db = Database(str(tmp_path / "settings.db"))
    db.save_settings({"roserpina.max_session_loss": "abc"})

    letto = SettingsService(db).load_roserpina_config().max_session_loss

    assert validators.finite_number(letto) is None       # NaN: runtime blocks


def test_pass_soglia_di_arresto_con_tolleranza():
    assert validators.reaches_limit(0.1 + 0.2, 0.3)
    assert validators.reaches_limit(10.0, 10.0)
    assert not validators.reaches_limit(9.99, 10.0)
