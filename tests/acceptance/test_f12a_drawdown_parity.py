"""F-12 (#586, DD-01/DD-02, MOD-03, NF-02, ROS-01-leak) — stop sul drawdown % con la stessa semantica in SIM e LIVE.

Decisione owner F-CFG-04 (#582, 29/10/2026, OWNER_APPROVED): «lo stop drawdown percentuale sul bankroll configurato deve
scattare con stessa semantica, base di calcolo e condizioni in SIM e LIVE. Nessun trigger se non configurato/attivo;
nessun nuovo default».

Su `main` `ada4076`:
- F-12a: il percorso dei segnali applicava `max_drawdown_hard_stop_pct` solo con `execution_mode == "LIVE"`; bet manuale,
  lotto Dutching (`absolute_cap_reason_for_orders`) e auto-next non lo applicavano mai. Un limite non numerico faceva
  sollevare l'handler; NaN non scattava mai (fail-open).
- F-12b: i rifiuti `drawdown_hard_stop_active` e `max_recovery_tables_reached` non rilasciavano la chiave
  anti-duplicato: un segnale valido sullo stesso evento restava `duplicato_bloccato` per il TTL.
"""
from __future__ import annotations

import pytest

from tests.acceptance.test_issue437_pr27 import _rifiuti
from tests.acceptance.test_issue461_pr28e_funds_gate import _auto_next, _chain, _con_fondi
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal

LIMITE = 20.0


def _in_drawdown(rc, *, picco: float = 100.0, bankroll: float = 50.0, limite=LIMITE) -> None:
    rc.config.max_drawdown_hard_stop_pct = limite
    rc.risk_desk.equity_peak = picco
    rc.risk_desk.bankroll_current = bankroll


# ------------------------------------------------------------------ helper unico, stessa semantica SIM/LIVE
@pytest.mark.parametrize("modalita", ["SIMULATION", "LIVE"])
@pytest.mark.parametrize("bankroll,atteso", [(50.0, "drawdown_hard_stop_active"),      # 50 %
                                             (80.0, "drawdown_hard_stop_active"),      # 20 % = limite: scatta
                                             (80.01, ""),                              # 19,99 %
                                             (100.0, ""), (120.0, "")])
def test_stessa_decisione_in_sim_e_live(tmp_path, modalita: str, bankroll: float, atteso: str) -> None:
    c = _chain(tmp_path)
    c.rc.execution_mode = modalita
    _in_drawdown(c.rc, bankroll=bankroll)

    assert c.rc._drawdown_pct_stop_reason() == atteso


def test_non_configurato_non_scatta_mai(tmp_path) -> None:
    c = _chain(tmp_path)
    _in_drawdown(c.rc, bankroll=1.0, limite=None)

    assert c.rc._drawdown_pct_stop_reason() == ""


@pytest.mark.parametrize("limite", [float("nan"), float("inf"), -5.0, 0.0, 150.0, "abc", True])
def test_limite_configurato_non_valido_nega(tmp_path, limite) -> None:
    c = _chain(tmp_path)
    _in_drawdown(c.rc, bankroll=100.0, limite=limite)

    assert c.rc._drawdown_pct_stop_reason() == "max_drawdown_hard_stop_pct_non_valido"


def test_scatto_mette_il_runtime_in_lockdown(tmp_path) -> None:
    c = _chain(tmp_path)
    _in_drawdown(c.rc)

    c.rc._drawdown_pct_stop_reason()

    assert str(getattr(c.rc.mode, "value", c.rc.mode)) == "LOCKDOWN"


# ------------------------------------------------------------------ percorso dei segnali (catena reale, SIM)
def test_block_segnale_in_sim_rifiutato_e_chiave_rilasciata(tmp_path) -> None:
    c = _chain(tmp_path, stake=4.0, anti_duplication=True)
    _con_fondi(c, 500.0)
    _in_drawdown(c.rc)

    c.rc._on_signal_received(_signal())

    assert "drawdown_hard_stop_active" in _rifiuti(c), _rifiuti(c)
    assert c.bus.payloads("CMD_QUICK_BET") == []
    assert c.rc.duplication_guard._active == {}, "chiave anti-duplicato trattenuta da un rifiuto (F-12b)"


def test_segnale_senza_drawdown_parte_in_sim(tmp_path) -> None:
    c = _chain(tmp_path, stake=4.0, anti_duplication=True)
    _con_fondi(c, 500.0)
    _in_drawdown(c.rc, bankroll=100.0)

    c.rc._on_signal_received(_signal())

    assert len(c.bus.payloads("CMD_QUICK_BET")) == 1, _rifiuti(c)


def test_block_max_recovery_tables_rilascia_la_chiave(tmp_path) -> None:
    c = _chain(tmp_path, stake=4.0, anti_duplication=True)
    _con_fondi(c, 500.0)
    c.rc.config.allow_recovery = True
    c.rc.config.max_recovery_tables = 0
    for t in c.rc.table_manager._tables.values():                       # nessun tavolo libero
        t.status = "ACTIVE"
        t.current_event_key = f"occupato-{t.table_id}"

    c.rc._on_signal_received(_signal())

    assert any(r.startswith("max_recovery_tables_reached") for r in _rifiuti(c)), _rifiuti(c)
    assert c.rc.duplication_guard._active == {}, "chiave anti-duplicato trattenuta da un rifiuto (F-12b)"


# ------------------------------------------------------------------ bet manuale e lotto Dutching
def test_block_ordini_del_controller_rifiutati_in_drawdown(tmp_path) -> None:
    c = _chain(tmp_path)
    _in_drawdown(c.rc)
    ordine = {"market_id": "1.234", "selection_id": 11, "price": 2.0, "stake": 2.0, "bet_type": "BACK",
              "event_name": "Alpha v Beta"}

    assert c.rc.absolute_cap_reason_for_orders([ordine]) == "drawdown_hard_stop_active"
    _in_drawdown(c.rc, bankroll=100.0)
    assert c.rc.absolute_cap_reason_for_orders([ordine]) == ""


# ------------------------------------------------------------------ auto-next
def test_block_auto_next_rifiutato_in_drawdown(monkeypatch) -> None:
    from core.runtime_controller import RuntimeController

    originale = RuntimeController._evaluate_and_maybe_submit_auto_next_trade_serial

    def _con_drawdown(self, *a, **k):
        bankroll = float(self.risk_desk.bankroll_current or 150.0)
        # drawdown 6 % (> limite 5 %) ma sotto le soglie di difesa/lockdown del MM: decide solo lo stop % (F-12a)
        _in_drawdown(self, picco=bankroll / 0.94, bankroll=bankroll, limite=5.0)
        return originale(self, *a, **k)

    monkeypatch.setattr(RuntimeController, "_evaluate_and_maybe_submit_auto_next_trade_serial", _con_drawdown)
    _rc, bus, esito = _auto_next({"market_id": "1.2", "selection_id": 2, "price": 6.0, "stake": 2.0,
                                  "bet_type": "LAY"}, funds={"available": 150.0, "exposure": 0.0})

    assert [e for e in bus.events if e[0] == "CMD_QUICK_BET"] == [], esito
    assert esito["reason"] == "drawdown_hard_stop_active", esito
