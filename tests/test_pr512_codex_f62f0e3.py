"""PR #512, ciclo 7 (deroga owner PF-PR512-CYCLE7-AUTHORIZATION): i tre P1 Codex confermati sul head f62f0e3
(r4306544229 lato mancante, r4306544236 reset SIM con tavoli trattenuti, r4306544231 baseline stantia).
RED sul head f62f0e3, GREEN con la patch."""
import threading

import pytest

from tests.f026.test_f018_rebuild_units import _Exchange, _rc, _row
from tests.f026.test_f032_rsr12_units import _rc as _rc32


# ---------------------------------------------------------------- r4306544229 (P1): riga pendente senza lato persistito
def test_pending_row_without_side_is_unattributed_never_rebuilt_as_back():
    rc = _rc([_row("r1", "SUBMITTED", "1.1", 1, "K1", side="", bet_id="77")], _Exchange([{"betId": "77", "marketId": "1.1"}]))
    res = rc._rebuild_open_positions()
    assert res["unattributed"] == ["r1"] and res["ok"] is False
    assert rc.table_manager.total_exposure() == 0.0, "lato inventato (BACK) con esposizione sottostimata"


@pytest.mark.parametrize("side", ["back", "Lay"])
def test_pending_row_with_a_valid_side_in_any_case_is_still_rebuilt(side):
    rc = _rc([_row("r1", "SUBMITTED", "1.1", 1, "K1", side=side, bet_id="77")], _Exchange([{"betId": "77", "marketId": "1.1"}]))
    res = rc._rebuild_open_positions()
    assert res["rebuilt"] == ["r1"]


# ---------------------------------------------------------------- r4306544236 (P1): reset_cycle non azzera il broker SIM
# finche' un tavolo resta trattenuto (posizione viva/incerta non regolata)
class _SimService:
    def __init__(self):
        self.resets = 0

    def get_account_funds(self):
        return {"ok": True, "available": 100.0, "exposure": 0.0}

    def reset_simulation(self):
        self.resets += 1
        return {"ok": True}


def _rc_reset(tmp_path, with_position):
    rc = _rc32(tmp_path / "r.db")
    rc.simulation_mode = True
    rc.betfair_service = _SimService()
    rc._daily_loss_state_lock = threading.Lock()
    rc._daily_loss_monitor_state = {"realized_pnl_day_baseline": 0.0, "realized_pnl": 0.0}
    rc._session_pnl_baseline = None
    rc.get_status = lambda: {}
    if with_position:
        rc.table_manager.activate(table_id=1, event_key="K1", exposure=5.0, market_id="1.1", selection_id=7, meta={})
        rc.duplication_guard.acquire("K1")
    return rc


def test_reset_cycle_keeps_the_sim_broker_state_while_a_table_is_retained(tmp_path):
    rc = _rc_reset(tmp_path, with_position=True)
    out = rc.reset_cycle()
    assert out["reset"] is True
    assert rc.betfair_service.resets == 0, "broker SIM azzerato con una posizione ancora riservata sul tavolo"
    assert out.get("sim_reset_skipped") == [1]


def test_reset_cycle_resets_the_sim_broker_when_no_table_is_retained(tmp_path):
    rc = _rc_reset(tmp_path, with_position=False)
    out = rc.reset_cycle()
    assert rc.betfair_service.resets == 1 and out.get("sim_reset_skipped") == []


# ---------------------------------------------------------------- r4306544231 (P1): baseline di espansione stantia dopo
# un reset la cui scrittura e' fallita: al riavvio non deve tornare autorevole
def test_stale_expansion_baseline_is_rejected_after_a_durable_cycle_reset(tmp_path):
    rc = _rc32(tmp_path / "b.db")
    assert rc._start_expansion_cycle("first_start", funds={"available": 100.0, "exposure": 0.0}) is True
    real_save = rc.db.save_settings

    def failing_for_baseline(data):
        if rc._EXPANSION_KEY in data:
            raise RuntimeError("disco pieno")
        return real_save(data)

    rc.db.save_settings = failing_for_baseline
    assert rc._start_expansion_cycle("reset_cycle", funds={"available": 130.0, "exposure": 0.0}) is False
    assert rc._persist_recovery_tables(cycle_reset=True) is True       # il reset e' durevole, la baseline no
    again = _rc32(tmp_path / "b.db")
    again._load_expansion_baseline()
    assert again._expansion_baseline is None, "baseline del ciclo precedente tornata autorevole dopo il riavvio"


def test_baseline_fixed_by_a_real_reset_cycle_survives_restart(tmp_path):
    """Riesame indipendente del ciclo 7: la sequenza REALE di reset_cycle() (baseline poi marker) non deve
    scartare al riavvio la baseline fissata dal reset stesso (stesso istante per baseline e marker)."""
    rc = _rc_reset(tmp_path, with_position=False)
    assert rc.reset_cycle()["reset"] is True
    again = _rc32(tmp_path / "r.db")
    assert again._load_expansion_baseline() is True
    assert again._expansion_baseline == pytest.approx(100.0), "baseline fissata dal reset scartata al riavvio"


def test_baseline_fixed_by_a_real_auto_reset_survives_restart(tmp_path):
    rc = _rc_reset(tmp_path, with_position=False)
    rc.simulation_mode = False
    rc._record_account_snapshot({"available": 100.0, "exposure": 0.0}, source="t")
    rc.config.auto_reset_drawdown_pct = 10.0
    rc._record_account_snapshot({"available": 80.0, "exposure": 0.0}, source="t")
    assert rc._maybe_auto_reset(20.0) is True
    again = _rc32(tmp_path / "r.db")
    again._load_expansion_baseline()
    assert again._expansion_baseline == pytest.approx(80.0)


def test_baseline_newer_than_the_last_reset_survives_restart(tmp_path):
    rc = _rc32(tmp_path / "c.db")
    rc._persist_recovery_tables(cycle_reset=True, reset_ts=1000.0)
    assert rc._start_expansion_cycle("first_start", funds={"available": 100.0, "exposure": 0.0}, set_at_ts=1001.0) is True
    again = _rc32(tmp_path / "c.db")
    again._load_expansion_baseline()
    assert again._expansion_baseline == pytest.approx(100.0)


@pytest.mark.parametrize("marker", ['{"at_ts": "garbage"}', '{"at_ts": null}', 'not json', '{"at_ts": "NaN"}'])
def test_corrupt_cycle_reset_marker_disables_expansion(tmp_path, marker):
    """Marker del reset illeggibile => ultimo reset ignoto => la baseline persistita non e' autorevole."""
    import json
    rc = _rc32(tmp_path / "d.db")
    assert rc._start_expansion_cycle("first_start", funds={"available": 50.0, "exposure": 0.0}) is True
    rc.db.save_settings({rc._CYCLE_RESET_KEY: marker})
    again = _rc32(tmp_path / "d.db")
    assert again._load_expansion_baseline() is True
    assert again._expansion_baseline is None, "baseline accettata con marker di reset corrotto (fail-open)"
    assert json.loads(again.db.get_settings()[again._EXPANSION_KEY])["baseline"] == pytest.approx(50.0)


@pytest.mark.parametrize("set_at_ts", ["NaN", "Infinity", "inf", "garbage", None, True])
def test_corrupt_baseline_timestamp_never_outranks_a_durable_reset(tmp_path, set_at_ts):
    """Seconda lettura indipendente del ciclo 7: `set_at_ts` non finito (NaN/inf/testo/null/bool) nella baseline
    persistita non deve scavalcare il confronto con il marker di reset (NaN < x e inf < x sono falsi)."""
    import json
    rc = _rc32(tmp_path / "e.db")
    rc.db.save_settings({rc._EXPANSION_KEY: json.dumps({"baseline": 50.0, "set_at_ts": set_at_ts}),
                         rc._CYCLE_RESET_KEY: json.dumps({"at_ts": 5000.0})})
    again = _rc32(tmp_path / "e.db")
    assert again._load_expansion_baseline() is True
    assert again._expansion_baseline is None, "baseline con set_at_ts corrotto accettata dopo un reset durevole"


def test_legacy_baseline_without_timestamp_is_accepted_only_until_a_reset_exists(tmp_path):
    import json
    rc = _rc32(tmp_path / "f.db")
    rc.db.save_settings({rc._EXPANSION_KEY: json.dumps({"baseline": 50.0, "reason": "first_start"})})
    rc._load_expansion_baseline()
    assert rc._expansion_baseline == pytest.approx(50.0)
    rc.db.save_settings({rc._CYCLE_RESET_KEY: json.dumps({"at_ts": 5000.0})})
    again = _rc32(tmp_path / "f.db")
    again._load_expansion_baseline()
    assert again._expansion_baseline is None
