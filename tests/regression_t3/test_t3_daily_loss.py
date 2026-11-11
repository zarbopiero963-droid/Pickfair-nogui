"""T3 regressions: durable daily loss across a new session (offline).

Row: PF-C28-01 («nessun reset cambiando sessione»). Real RuntimeController,
Database (tmp_path) and settlement path, via the PR28-b/PR27 helpers.
"""
from __future__ import annotations

import pytest

from tests.acceptance.test_issue437_pr28b import _catena, _perdi, _perdita_giorno


def test_pf_c28_01_new_session_does_not_reset_daily_loss(tmp_path):
    """ID: PF-C28-01

    Oracle: EUR 8 lost today, then start() opens a NEW session in the same
    process: the session baseline moves, the day's loss stays 8.0; a further
    EUR 3 loss gives 11.0 > max_daily_loss 10.0 => breached and STOP.
    """
    c = _catena(tmp_path, max_daily_loss=10.0)
    # The limit is a persisted owner setting: start()'s reload_config must read
    # it back (the chain's settings stub returns the legacy test config).
    load = c.rc.settings_service.load_roserpina_config

    def load_with_limit():
        cfg = load()
        cfg.max_daily_loss = 10.0
        return cfg
    c.rc.settings_service.load_roserpina_config = load_with_limit
    _perdi(c.rc, 8.0)
    assert _perdita_giorno(c.rc)["daily_loss_amount"] == pytest.approx(8.0)

    c.rc.betfair_service.connect = lambda **_k: {"session": "sim"}
    c.rc.betfair_service.get_account_funds = lambda: {"available": 1000.0}
    c.rc.start(execution_mode="SIMULATION")
    assert c.rc._session_pnl_baseline == pytest.approx(c.rc.risk_desk.realized_pnl)
    assert c.rc.config.max_daily_loss == pytest.approx(10.0)

    assert _perdita_giorno(c.rc)["daily_loss_amount"] == pytest.approx(8.0)
    _perdi(c.rc, 3.0)
    stato = _perdita_giorno(c.rc)
    assert stato["daily_loss_amount"] == pytest.approx(11.0)
    assert stato["breached"] is True, stato
    assert c.rc.is_emergency_stopped
