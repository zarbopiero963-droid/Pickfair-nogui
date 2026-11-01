"""SIM-RESTART-003 — crash e recovery (C-05): posizioni vive sopravvivono al crash esterno; debito dopo crash fra
mercato CLOSED (exchange) e contabilizzazione (RST-03, sonda richiesta dal tracker #586, PR primaria F-10)."""
import tempfile
from decimal import Decimal as D

import pytest

from tests.fullscope.harness import oracle as O
from tests.fullscope.harness import runner as H

pytestmark = [pytest.mark.fullscope, pytest.mark.e2e, pytest.mark.recovery]

START = D("100")
COMM = D("0.045")


def test_external_kill_with_open_positions_restart_rebuilds_tables_no_replay():
    home = tempfile.mkdtemp(prefix="pf_restart003_")
    first = H.run([{"op": "signal", "m": "1.831", "price": 2.0}, {"op": "signal", "m": "1.832", "price": 1.5},
                   {"op": "checkpoint", "name": "open", "hold_s": 12}, {"op": "wait", "s": 1}],
                  home=home, kill_at_checkpoint="open", scenario="sim_restart_003")
    assert first["_killed_at"] == "open"
    second = H.run([{"op": "wait", "s": 1}], home=home, scenario="sim_restart_003")
    s = second["snaps"][-1]
    o = O.Oracle(s, {}, start=START, commission=COMM)
    assert O.table_markets(s) == {"1.831", "1.832"}, "tavoli non ricostruiti (F018): " + O.describe(s, o)
    assert not H.events(second, "QUICK_BET_ROUTED"), "ordine ri-inviato al riavvio: " + O.describe(s, o)
    O.check_no_duplicate_intents(s, ["1.831", "1.832"])
    O.check_conservation(s, o)


@pytest.mark.pf_requirement("RST-03")
def test_rst03_loss_settled_on_exchange_before_crash_is_booked_once_after_restart():
    """RST-03: l'exchange regola il mercato (perdita), il bot muore PRIMA di contabilizzare RUNTIME_CLOSE_POSITION.
    Atteso dopo il riavvio: la perdita e' contabilizzata una sola volta, il tavolo non e' stato liberato senza
    contabilizzazione, nessun doppio conteggio al secondo settlement."""
    home = tempfile.mkdtemp(prefix="pf_rst03_")
    first = H.run([{"op": "signal", "m": "1.841", "price": 2.0, "stake": 3.0},
                   {"op": "signal", "m": "1.842", "price": 2.0, "stake": 3.0},
                   {"op": "crash_before_close_position", "m": "1.841"},
                   {"op": "settle", "m": "1.841", "win": False}],
                  home=home, scenario="sim_rst03", requirements=["RST-03"])
    assert str(first.get("crashed_at", "")).startswith("BEFORE_RUNTIME_CLOSE_POSITION"), first.get("crashed_at")
    s_crash = first["snaps"][-1]
    assert any(x["market_id"] == "1.841" for x in s_crash["exch_settlements"]), "exchange non regolato prima del crash"
    second = H.run([{"op": "wait", "s": 4}, {"op": "settle", "m": "1.842", "win": True}, {"op": "wait", "s": 1}],
                   home=home, scenario="sim_rst03", requirements=["RST-03"])
    s = second["snaps"][-1]
    o = O.Oracle(s, {"1.841": False, "1.842": True}, start=START, commission=COMM)
    O.check_single_settlement_per_market(s)
    O.check_realized(s, o)                      # il debito di 1.841 (-3) e' presente, una sola volta
    O.check_conservation(s, o)
    assert O.table_markets(s) == set(), O.describe(s, o)
    closes = H.events(second, "RUNTIME_CLOSE_POSITION", "1.841")
    assert len(closes) == 1, f"settlement di 1.841 consegnato {len(closes)} volte dopo il riavvio: " + O.describe(s, o)
