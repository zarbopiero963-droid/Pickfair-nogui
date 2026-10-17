"""PF-ALL-F027 — test unitari sul codice reale (RuntimeController senza costruttore; broker e bus sono stub).

- la riconciliazione dei settlement gira in ACTIVE, PAUSED e LOCKDOWN, non in STOPPED;
- dopo un emergency stop scattato in LIVE il registro SIM non vale per le posizioni reali: nessuna emissione,
  incertezza segnalata una volta;
- la riconciliazione non produce ordini: fuori da ACTIVE l'auto-next si ferma.
"""
import pytest

from core.runtime_controller import RuntimeController
from core.system_state import RuntimeMode
from core.table_manager import TableManager


class _Bus:
    def __init__(self):
        self.events = []

    def publish(self, name, payload=None):
        self.events.append((name, dict(payload or {})))


class _Broker:
    def __init__(self):
        self.reads = 0

    def list_settlements(self):
        self.reads += 1
        return []


class _Service:
    def __init__(self, broker):
        self.broker = broker

    def get_simulation_broker(self):
        return self.broker


def _rc(mode):
    rc = RuntimeController.__new__(RuntimeController)
    rc.mode = mode
    rc.bus = _Bus()
    rc.table_manager = TableManager(table_count=2)
    rc.broker = _Broker()
    rc.betfair_service = _Service(rc.broker)
    return rc


@pytest.mark.parametrize("mode,allowed", [(RuntimeMode.ACTIVE, True), (RuntimeMode.PAUSED, True),
                                          (RuntimeMode.LOCKDOWN, True), (RuntimeMode.STOPPED, False)])
def test_reconciliation_modes(mode, allowed):
    rc = _rc(mode)
    assert rc._settlement_reconciliation_allowed() is allowed
    rc._poll_simulated_settlements_locked()
    assert rc.broker.reads == (1 if allowed else 0)


def test_live_emergency_never_reads_sim_ledger_and_reports_uncertainty_once():
    rc = _rc(RuntimeMode.LOCKDOWN)
    rc._settlement_source_before_emergency = "LIVE"
    rc.table_manager.activate(table_id=1, event_key="E1", exposure=7.0, market_id="1.1", selection_id=11)
    rc._poll_simulated_settlements_locked()
    rc._poll_simulated_settlements_locked()
    assert rc.broker.reads == 0, "registro SIM letto per posizioni LIVE"
    unc = [p for n, p in rc.bus.events if n == "SETTLEMENT_RECONCILIATION_UNCERTAIN"]
    assert len(unc) == 1 and unc[0]["open_exposure"] == 7.0
    assert rc.table_manager.total_exposure() == 7.0, "esposizione prudenziale mantenuta"


def test_sim_emergency_keeps_reading_sim_ledger():
    rc = _rc(RuntimeMode.LOCKDOWN)
    rc._settlement_source_before_emergency = "SIM"
    rc._poll_simulated_settlements_locked()
    assert rc.broker.reads == 1


@pytest.mark.parametrize("mode", [RuntimeMode.PAUSED, RuntimeMode.LOCKDOWN, RuntimeMode.STOPPED])
def test_no_new_order_outside_active(mode):
    rc = _rc(mode)
    assert rc._runtime_not_active_reason() == f"runtime_not_active:{mode.value}"


def test_active_allows_auto_next():
    assert _rc(RuntimeMode.ACTIVE)._runtime_not_active_reason() == ""
